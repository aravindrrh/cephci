"""SPECstorage benchmark on Spectrum Scale / GPFS NFS (upstream)."""

import shlex

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.spec_storage import SpecStorage
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    deploy_gpfs_scale,
    run_suite_cleanup,
    setup_gpfs_nfs,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)

# CI-sized defaults; certification sizes (e.g. VDA 1g) are too heavy for test clusters.
BENCHMARK_DEFAULTS = {
    "SWBUILD": {
        "Warmup_time": 30,
        "Dir_count": 15,
        "Files_per_dir": 30,
        "File_size": "3k",
        "Instances": 4,
    },
    "VDA": {
        "Warmup_time": 30,
        "Dir_count": 1,
        "Files_per_dir": 1,
        "File_size": "3k",
        "Instances": 1,
    },
    "EDA_BLENDED": {
        "Warmup_time": 30,
        "Dir_count": 1,
        "Files_per_dir": 1,
        "File_size": "3k",
        "Instances": 1,
    },
}


# Max time to delete large SPECstorage trees (legacy 1g VDA debris over NFS).
SPEC_EXPORT_CLEANUP_TIMEOUT = 3600


def _export_has_content(node, path):
    """Return True when path contains anything other than . and .."""
    quoted = shlex.quote(path.rstrip("/"))
    cmd = f"bash -lc 'shopt -s dotglob nullglob; ls -A {quoted} 2>/dev/null | head -1'"
    out, _err = node.exec_command(cmd=cmd, sudo=True, check_ec=False)
    return bool(out and out.strip())


def _rm_export_tree(node, path, via, timeout=SPEC_EXPORT_CLEANUP_TIMEOUT):
    """Delete all entries under path on a single node."""
    host = getattr(node, "hostname", str(node))
    quoted = shlex.quote(path.rstrip("/"))
    log.info("[%s] cleaning SPECstorage debris at %s (%s)", host, path, via)
    cmd = f"bash -lc 'shopt -s dotglob nullglob; rm -rf {quoted}/*'"
    node.exec_command(
        cmd=cmd,
        sudo=True,
        check_ec=False,
        long_running=True,
        timeout=timeout,
    )


def _cleanup_spec_export(primary_client, nfs_mount, server=None, nfs_export=None):
    """
    Remove leftover SPECstorage files from the shared export.

    Prefer GPFS-local delete on the admin/installer node (same path as nfs_export).
    Fall back to a single NFS client mount cleanup only when needed.
    """
    cleaned_via_gpfs = False
    if server is not None and nfs_export:
        if _export_has_content(server, nfs_export):
            _rm_export_tree(server, nfs_export, "GPFS")
            cleaned_via_gpfs = True
        else:
            host = getattr(server, "hostname", str(server))
            log.info(
                "[%s] SPECstorage export already empty at %s (GPFS)",
                host,
                nfs_export,
            )

    if cleaned_via_gpfs and not _export_has_content(primary_client, nfs_mount):
        host = getattr(primary_client, "hostname", str(primary_client))
        log.info(
            "[%s] SPECstorage export already empty at %s (NFS mount)",
            host,
            nfs_mount,
        )
        return

    if _export_has_content(primary_client, nfs_mount):
        _rm_export_tree(primary_client, nfs_mount, "NFS mount")
        return

    host = getattr(primary_client, "hostname", str(primary_client))
    log.info(
        "[%s] SPECstorage export already empty at %s (NFS mount)",
        host,
        nfs_mount,
    )


def _setup_spec_storage_ssh(primary_client, clients):
    """Ensure sshpass and root SSH access to SPECstorage client nodes."""
    primary_client.exec_command(cmd="dnf install -y sshpass", sudo=True)
    for client in clients:
        for target in (client.hostname, client.ip_address):
            cmd = (
                "sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no "
                f"-f -i ~/.ssh/id_rsa.pub root@{target}"
            )
            primary_client.exec_command(cmd=cmd, sudo=True, check_ec=False)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    clients_all = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "1"))
    if no_clients > len(clients_all):
        raise ConfigError("The test requires more clients than available")
    clients = clients_all[:no_clients]
    primary_client = clients[0]

    benchmark = config.get("benchmark", "SWBUILD")
    benchmark_defination = config.get("benchmark_defination")
    if benchmark_defination is None:
        benchmark_defination = BENCHMARK_DEFAULTS.get(
            benchmark, BENCHMARK_DEFAULTS["SWBUILD"]
        )
    load = config.get("load", "1")
    incr_load = config.get("incr_load", "1")
    num_runs = config.get("num_runs", "1")

    try:
        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        mount_config = dict(config)
        mount_config["skip_deployment"] = True
        gpfs = setup_gpfs_nfs(ceph_cluster, mount_config)
        nfs_mount = gpfs["nfs_mount"]
        nfs_export = gpfs.get("nfs_export")
        server = gpfs.get("server")

        _setup_spec_storage_ssh(primary_client, clients)
        _cleanup_spec_export(
            primary_client,
            nfs_mount,
            server=server,
            nfs_export=nfs_export,
        )

        log.info(
            "Run SPECstorage with %s benchmark on %s (clients=%d)",
            benchmark,
            nfs_mount,
            len(clients),
        )
        SpecStorage(primary_client).run_spec_storage(
            benchmark,
            load,
            incr_load,
            num_runs,
            clients,
            nfs_mount,
            benchmark_defination,
        )
        log.info("SPECstorage run completed")
        return 0
    except OperationFailedError:
        raise
    except Exception as e:
        log.error("SPECstorage failed: %s", e)
        raise OperationFailedError(f"SPECstorage failed: {e}") from e
    finally:
        run_suite_cleanup(ceph_cluster, config)
