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
        cmd = "sudo dnf -y install git gcc nfs-utils redhat-rpm-config krb5-devel python3-devel " \
              "python3-gssapi python3-ply"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = "rm -rf /root/pynfs && git clone git://git.linux-nfs.org/projects/cdmackay/pynfs.git;" \
              "cd /root/pynfs && yes | python3 setup.py build > /tmp/output_tempfile.txt"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = f"cd /root/pynfs/nfs4.0;./testserver.py {nfs_node.ip_address}:{nfs_export}_1 --verbose --maketree " \
              f"--showomit --rundeps all ganesha"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)

        # Run Cthon test
        log.info(f"Results V4.0 : {out}")

        # For V4.1
        cmd = f"cd /root/pynfs/nfs4.1;./testserver.py {nfs_node.ip_address}:{nfs_export}_1 --verbose --maketree " \
              f"--showomit --rundeps all ganesha"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)

        # Run Cthon test
        log.info(f"Results V4.1 : {out}")

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        # sleep(30)
        pass
    return 0
