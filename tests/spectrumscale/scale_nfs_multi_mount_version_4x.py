from threading import Thread

from nfs_operations import cleanup_cluster

from cli.exceptions import ConfigError
from cli.utilities.utils import create_files
from spectrumscale.spectrum_scale_nfs_helpers import (
    is_spectrum_scale_backend,
    resolve_nfs_service_nodes,
    setup_nfs_cluster_or_scale,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify mount the NFS volume via v4.1 and v4.2 on two linux client and run IO's in parallel
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    # nfs cluster details
    if is_spectrum_scale_backend(config):
        _, nfs_server_name = resolve_nfs_service_nodes(ceph_cluster, config)
        ha = False
        vip = None
    else:
        nfs_nodes = ceph_cluster.get_nodes("nfs")
        no_servers = int(config.get("servers", "1"))
        if no_servers > len(nfs_nodes):
            raise ConfigError("The test requires more servers than available")
        servers = nfs_nodes[:no_servers]
        nfs_server_name = [nfs_node.hostname for nfs_node in servers]
        ha = bool(config.get("ha", False))
        vip = config.get("vip", None)

    no_clients = int(config.get("clients", "2"))
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.1")
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"

    clients = ceph_cluster.get_nodes("client")[:no_clients]

    try:
        # Setup nfs cluster
        setup_nfs_cluster_or_scale(
            ceph_cluster,
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            config=config,
            ha=ha,
            vip=vip,
        )

        # Run parallel IO on v4.1 and v4.2 mounts
        threads = []
        for client in clients:
            io = Thread(
                target=create_files,
                args=(client, nfs_mount, 50),
            )
            io.start()
            threads.append(io)
        for th in threads:
            th.join()

    except Exception as e:
        log.error(f"Failed to setup nfs-ganesha cluster {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1
    finally:
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
    return 0
