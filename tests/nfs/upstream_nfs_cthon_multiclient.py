"""Run cthon04 simultaneously on all Scale NFS clients.

Mirrors the direct cthon flow from upstream_nfs_cthon (clone/build + ./server)
but targets Spectrum Scale exports and drives every configured client in parallel.
"""

import concurrent.futures
from os import environ

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    deploy_gpfs_scale,
    run_suite_cleanup,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)

CTHON_TIMEOUT = 10400
NFS_MOUNT = "/mnt/nfs"
NFS_MOUNT_V41 = "/mnt/nfsv4_1"


def _setup_cthon_on_client(client):
    """Install deps and build cthon04 on one client."""
    cmds = [
        "dnf install -y git gcc nfs-utils time make",
        # CRB may be unavailable on some Scale images; fall back to base repos.
        "dnf --enablerepo=crb install -y libtirpc-devel || dnf install -y libtirpc-devel",
        (
            "rm -rf cthon04 && "
            "git clone --depth=1 "
            "git://git.linux-nfs.org/projects/steved/cthon04.git && "
            "cd cthon04 && make all"
        ),
    ]
    for cmd in cmds:
        exit_code = client.exec_command(
            cmd=cmd, sudo=True, long_running=True, timeout=600
        )
        if exit_code != 0:
            raise OperationFailedError(
                f"Cthon setup failed on {client.hostname} "
                f"(exit {exit_code}): {cmd}"
            )


def _prepare_mounts(client, server_ip, nfs_export, version, port):
    """Create mount dirs and mount primary NFS version + NFSv4.1."""
    cmds = [
        f"umount -fl {NFS_MOUNT} {NFS_MOUNT_V41} 2>/dev/null || true",
        f"mkdir -p {NFS_MOUNT} {NFS_MOUNT_V41}",
        (
            f"mount -t nfs -o vers={version},port={port} "
            f"{server_ip}:{nfs_export} {NFS_MOUNT}"
        ),
        (
            f"mount -t nfs -o vers=4.1,port={port} "
            f"{server_ip}:{nfs_export} {NFS_MOUNT_V41}"
        ),
    ]
    for cmd in cmds:
        client.exec_command(cmd=cmd, sudo=True)


def _run_cthon_on_client(client, server_ip, nfs_export, iterations):
    """Run cthon iterations against both mounts on one client."""
    host = client.hostname
    for iteration in range(1, iterations + 1):
        log.info(">>> [%s] Cthon iteration %s/%s", host, iteration, iterations)
        for mount in (NFS_MOUNT, NFS_MOUNT_V41):
            cmd = (
                f"cd cthon04;./server -a -p {nfs_export} -m {mount} {server_ip}"
            )
            log.info("[%s] Running: %s", host, cmd)
            exit_code = client.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout=CTHON_TIMEOUT
            )
            if exit_code != 0:
                raise OperationFailedError(
                    f"Cthon failed on {host} mount={mount} "
                    f"iteration={iteration}/{iterations} (exit {exit_code})"
                )
    log.info("[%s] Cthon completed successfully", host)
    return host


def run(ceph_cluster, **kw):
    """Run cthon in parallel from all configured NFS clients on Scale.

    Config:
        clients: number of clients to use (default: all available)
        nfs_export: Scale NFS export path
        nfs_version: primary NFS version for first mount (default: 4.0)
        port: NFS port (default: 2049)
        iterations: times to repeat cthon per client (default: 1)
        skip_deployment: skip Scale deploy when suite already deployed
    """
    config = kw.get("config") or {}
    nfs_export = config.get("nfs_export") or environ.get(
        "EXPORT_NAME", "/ibm/scale_volume/export1"
    )
    port = str(config.get("port", "2049"))
    version = str(config.get("nfs_version", "4.0"))
    iterations = int(config.get("iterations", 1))
    if iterations < 1:
        raise ConfigError("iterations must be >= 1")

    server = ceph_cluster.get_nodes("installer")[0]
    clients_all = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", len(clients_all)))
    if no_clients < 1:
        raise ConfigError("clients must be >= 1")
    if no_clients > len(clients_all):
        raise ConfigError("The test requires more clients than available")
    clients = clients_all[:no_clients]

    try:
        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        log.info(
            "Preparing cthon on %d client(s): %s",
            len(clients),
            [c.hostname for c in clients],
        )
        for client in clients:
            _setup_cthon_on_client(client)
            _prepare_mounts(client, server.ip_address, nfs_export, version, port)

        log.info(
            "Starting parallel cthon on %d client(s) against %s:%s",
            len(clients),
            server.ip_address,
            nfs_export,
        )
        errors = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(clients)
        ) as executor:
            futures = {
                executor.submit(
                    _run_cthon_on_client,
                    client,
                    server.ip_address,
                    nfs_export,
                    iterations,
                ): client
                for client in clients
            }
            for future in concurrent.futures.as_completed(futures):
                client = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    log.error("Cthon failed on %s: %s", client.hostname, exc)
                    errors.append((client.hostname, exc))

        if errors:
            detail = "; ".join(f"{host}: {err}" for host, err in errors)
            raise OperationFailedError(
                f"Cthon multiclient failures ({len(errors)}/{len(clients)}): {detail}"
            )

    except (ConfigError, OperationFailedError):
        raise
    except Exception as e:
        log.error("Cthon multiclient setup/run failed: %s", e)
        raise OperationFailedError(
            f"Cthon multiclient setup/run failed: {e}"
        ) from e
    finally:
        for client in clients:
            try:
                client.exec_command(
                    sudo=True,
                    cmd=f"umount -l {NFS_MOUNT_V41} {NFS_MOUNT} 2>/dev/null || true",
                    check_ec=False,
                )
            except Exception as exc:
                log.warning(
                    "cthon multiclient umount failed on %s: %s",
                    client.hostname,
                    exc,
                )
        run_suite_cleanup(ceph_cluster, config)

    return 0
