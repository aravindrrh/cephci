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

        cmd = "sudo dnf install -y git gcc gcc-c++ make automake autoconf " \
              "pkgconf pkgconf-pkg-config libtool bison flex " \
              "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # clone LTP
        cmd = "git clone https://github.com/linux-test-project/ltp.git"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Build LTP
        cmd = "cd ltp;make autotools;./configure;make -j$(nproc);sudo make install"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Run LTP test
        cmd = f"cd /opt/ltp; sudo ./runltp -d {nfs_mount} -f fs " \
              "-o /tmp/ltp_output.log -l /tmp/ltp_run.log -p"
        clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        # sleep(30)
        # view test results
        log.info("===================RUN_RESULTS====================")
        cmd = "cat /tmp/ltp_output.log"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        log.info("===================RUN_LOG====================")
        cmd = "cat /tmp/ltp_run.log"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        # Find failed tests
        cmd = "grep FAIL /tmp/ltp_output.log"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        log.info("Cleaning up skipped")
        # cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        # log.info("Cleaning up successfull")
    return 0
