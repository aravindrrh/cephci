from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")
    nfs_mount = config.get("mount_point", "/mnt/nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    log.info("Setup nfs cluster")
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
        cmd = "git clone --depth=1 git://git.linux-nfs.org/projects/steved/cthon04.git;cd cthon04;make all"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Run Cthon test
        cmd = f"cd cthon04;./server -a -p {nfs_export}_1 -m {nfs_mount} {nfs_node.ip_address}"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
        log.info(out)

        # "mkdir -p /mnt/nfsv3",
        # f"mount -t nfs -o vers=3 {nfs_node.ip_address}:{nfs_export} /mnt/nfsv3",
        cmds = ["mkdir -p /mnt/nfsv4_1",
                f"mount -t nfs -o vers=4.1 {nfs_node.ip_address}:{nfs_export}_1 /mnt/nfsv4_1"
                ]
        for cmd in cmds:
            clients[0].exec_command(cmd=cmd, sudo=True)
        #
        # # Run Cthon test v3
        # cmd = f"cd cthon04;./server -a -p {nfs_export}_1 -m /mnt/nfsv3 {nfs_node.ip_address}"
        # out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
        # log.info(out)

        # Run Cthon test v4.1
        cmd = f"cd cthon04;./server -a -p {nfs_export}_1 -m /mnt/nfsv4_1 {nfs_node.ip_address}"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
        log.info(out)
    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        # sleep(30)
        pass
    return 0
