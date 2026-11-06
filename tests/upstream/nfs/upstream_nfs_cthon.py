from concurrent.futures import ThreadPoolExecutor, as_completed

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster, wipe_export_contents

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)

# Extra mount used for NFSv4.1 cthon pass
NFS_MOUNT_V41 = "/mnt/nfsv4_1"
CTHON_TIMEOUT = 10400


def _prepare_client(client, nfs_node, export_path):
    """Install deps, build cthon, and mount the v4.1 export on one client."""
    log.info("Preparing cthon on client %s (export %s)", client.hostname, export_path)

    client.exec_command(
        cmd="sudo dnf install -y git gcc nfs-utils time make", sudo=True
    )
    client.exec_command(
        cmd="dnf --enablerepo=crb install -y libtirpc-devel", sudo=True
    )
    client.exec_command(
        cmd=(
            "rm -rf cthon04 && "
            "git clone --depth=1 git://git.linux-nfs.org/projects/steved/cthon04.git;"
            "cd cthon04;make all"
        ),
        sudo=True,
    )

    for cmd in (
        f"mkdir -p {NFS_MOUNT_V41}",
        f"mount -t nfs -o vers=4.1 {nfs_node.ip_address}:{export_path} {NFS_MOUNT_V41}",
    ):
        client.exec_command(cmd=cmd, sudo=True)


def _run_cthon(client, nfs_node_ip, export_path, mount, iteration, label):
    """Run one cthon pass on a client and log the result."""
    cmd = f"cd cthon04;./server -a -p {export_path} -m {mount} {nfs_node_ip}"
    log.info(
        "Cthon %s on %s export=%s mount=%s iteration=%s",
        label,
        client.hostname,
        export_path,
        mount,
        iteration,
    )
    out, _ = client.exec_command(cmd=cmd, sudo=True, timeout=CTHON_TIMEOUT)
    log.info("[%s/%s] %s", client.hostname, label, out)
    return out


def _run_cthon_iteration(client, nfs_node_ip, export_path, nfs_mount, iteration):
    """Run default-mount then v4.1-mount cthon passes for one client/iteration."""
    _run_cthon(
        client, nfs_node_ip, export_path, nfs_mount, iteration, label="default"
    )
    _run_cthon(
        client, nfs_node_ip, export_path, NFS_MOUNT_V41, iteration, label="v4.1"
    )


def _cleanup_client_extra_mount(client):
    try:
        client.exec_command(
            sudo=True, cmd=f"umount -l {NFS_MOUNT_V41}", check_ec=False
        )
        client.exec_command(
            sudo=True, cmd=f"rm -rf {NFS_MOUNT_V41}", check_ec=False
        )
    except Exception as exc:
        log.warning(
            "cthon extra-mount cleanup failed on %s: %s", client.hostname, exc
        )


def run(ceph_cluster, **kw):
    config = kw.get("config")
    nfs_mount = config.get("mount_point", "/mnt/nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    iterations = int(config.get("iterations", 1))
    no_clients = int(config.get("clients", len(clients)))
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    if iterations < 1:
        raise ConfigError("iterations must be >= 1")
    if no_clients < 1:
        raise ConfigError("clients must be >= 1")
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]

    log.info(
        "Setup nfs cluster for %s client(s); cthon iterations=%s",
        no_clients,
        iterations,
    )
    rc = 0
    try:
        setup_nfs_cluster(
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            ceph_cluster=ceph_cluster,
        )

        # Install/build cthon + v4.1 mount on every client in parallel.
        # setup_nfs_cluster assigns clients[i] -> /export_i
        with ThreadPoolExecutor(max_workers=len(clients)) as executor:
            futures = {
                executor.submit(
                    _prepare_client,
                    client,
                    nfs_node,
                    f"{nfs_export}_{idx}",
                ): client
                for idx, client in enumerate(clients)
            }
            for future in as_completed(futures):
                client = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    raise ConfigError(
                        f"cthon prepare failed on {client.hostname}: {exc}"
                    ) from exc

        for iteration in range(1, iterations + 1):
            log.info(
                ">>> Cthon iteration %s/%s across %s client(s)",
                iteration,
                iterations,
                len(clients),
            )
            with ThreadPoolExecutor(max_workers=len(clients)) as executor:
                futures = {
                    executor.submit(
                        _run_cthon_iteration,
                        client,
                        nfs_node.ip_address,
                        f"{nfs_export}_{idx}",
                        nfs_mount,
                        iteration,
                    ): client
                    for idx, client in enumerate(clients)
                }
                for future in as_completed(futures):
                    client = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        raise Exception(
                            f"cthon failed on {client.hostname} "
                            f"iteration {iteration}: {exc}"
                        ) from exc
    except Exception as e:
        log.error(f"Error : {e}")
        rc = 1
    finally:
        for idx, client in enumerate(clients):
            _cleanup_client_extra_mount(client)
            # Each client writes into its own /export_N during cthon
            wipe_export_contents(
                client,
                nfs_node.ip_address,
                f"{nfs_export}_{idx}",
                version="4.1",
                port=port,
            )
        try:
            cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        except Exception as exc:
            log.warning("cthon cleanup_cluster failed: %s", exc)
    return rc
