from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster, wipe_export_contents

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")
    nfs_mount = config.get("mount_point", "/mnt/nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    iterations = int(config.get("iterations", 1))
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname
    # Extra mount used for NFSv4.1 cthon pass
    nfs_mount_v41 = "/mnt/nfsv4_1"

    if iterations < 1:
        raise ConfigError("iterations must be >= 1")

    log.info("Setup nfs cluster")
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
        # Install pre-req
        cmd = "sudo dnf install -y git gcc nfs-utils time make"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = "dnf --enablerepo=crb install -y libtirpc-devel"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # clone Cthon
        cmd = (
            "rm -rf cthon04 && "
            "git clone --depth=1 git://git.linux-nfs.org/projects/steved/cthon04.git;"
            "cd cthon04;make all"
        )
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmds = [
            f"mkdir -p {nfs_mount_v41}",
            f"mount -t nfs -o vers=4.1 {nfs_node.ip_address}:{nfs_export}_1 {nfs_mount_v41}",
        ]
        for cmd in cmds:
            clients[0].exec_command(cmd=cmd, sudo=True)

        for iteration in range(1, iterations + 1):
            log.info(">>> Cthon iteration %s/%s", iteration, iterations)

            cmd = (
                f"cd cthon04;./server -a -p {nfs_export}_1 -m {nfs_mount} "
                f"{nfs_node.ip_address}"
            )
            out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
            log.info(out)

            cmd = (
                f"cd cthon04;./server -a -p {nfs_export}_1 -m {nfs_mount_v41} "
                f"{nfs_node.ip_address}"
            )
            out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
            log.info(out)
    except Exception as e:
        log.error(f"Error : {e}")
        rc = 1
    finally:
        # Wipe files + umount extras; leave static /export_N alone
        try:
            clients[0].exec_command(
                sudo=True,
                cmd=f"umount -l {nfs_mount_v41}",
                check_ec=False,
            )
            clients[0].exec_command(
                sudo=True, cmd=f"rm -rf {nfs_mount_v41}", check_ec=False
            )
        except Exception as exc:
            log.warning("cthon extra-mount cleanup failed: %s", exc)
        # First cthon pass and v4.1 both target /export_1
        wipe_export_contents(
            clients[0], nfs_node.ip_address, f"{nfs_export}_1", version="4.1", port=port
        )
        try:
            cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        except Exception as exc:
            log.warning("cthon cleanup_cluster failed: %s", exc)
    return rc
