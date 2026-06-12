from time import sleep

from upstream_nfs_edit_export_config_with_ro import update_export_conf
from upstream_nfs_operations import cleanup_cluster, create_export, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Test readonly export option on NFS mount
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version")
    no_clients = int(config.get("clients", "3"))
    nfs_name = "cephfs-nfs"
    nfs_mount = "/mnt/nfs"
    nfs_export = "/export"
    nfs_server_name = nfs_nodes[0].hostname
    fs_name = "cephfs"

    # RO export parameters
    nfs_export_readonly = "/exportRO"
    nfs_readonly_mount = "/mnt/nfs_readonly"

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    installer = ceph_cluster.get_nodes("installer")[0]

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
            fs_name,
            ceph_cluster=ceph_cluster,
        )

        # Same path as upstream_nfs_edit_export_config_with_ro (export.create
        # ignores readonly=True and always writes Access_Type = RW).
        create_export(installer, nfs_export_readonly)
        log.info("Setting Access_Type = RO on %s", nfs_export_readonly)
        update_export_conf(installer, nfs_export_readonly, "RO")

        clients[0].create_dirs(dir_path=nfs_readonly_mount, sudo=True)
        if Mount(clients[0]).nfs(
            mount=nfs_readonly_mount,
            version=version,
            port=port,
            server=installer.ip_address,
            export=nfs_export_readonly,
        ):
            log.error(f"Failed to mount nfs on {clients[0].hostname}")
            return 1
        log.info("Mount succeeded on client")

        sleep(3)
        out, err, exit_code, _ = clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_readonly_mount}/file_ro",
            check_ec=False,
            verbose=True,
        )
        touch_output = f"{out or ''}{err or ''}"
        if exit_code != 0 and (
            "touch: cannot touch" in touch_output
            or "Read-only file system" in touch_output
        ):
            log.info("creation of file on RO export failed with expected error")
        else:
            log.error(
                "Unexpected touch result on RO export for %s: exit=%s out=%r err=%r",
                clients[0].hostname,
                exit_code,
                out,
                err,
            )
            return 1

        _, _, rw_exit_code, _ = clients[0].exec_command(
            sudo=True,
            cmd=f"touch {nfs_mount}/file_rw",
            verbose=True,
        )
        if rw_exit_code != 0:
            log.error("failed to create file on RW export")
            return 1
        log.info("Successfully created file on RW export")
    except Exception as e:
        log.error(f"Failed to validate export readonly: {e}")
        return 1

    finally:
        log.info("Cleaning up")
        log.info("Unmounting nfs-ganesha readonly mount on client:")
        if Unmount(clients[0]).unmount(nfs_readonly_mount):
            raise OperationFailedError(
                f"Failed to unmount nfs on {clients[0].hostname}"
            )
        log.info("Removing nfs-ganesha readonly mount dir on client:")
        clients[0].exec_command(sudo=True, cmd=f"rm -rf  {nfs_readonly_mount}")
        cleanup_cluster(clients[0], nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
