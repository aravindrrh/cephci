from os import environ

from cli.exceptions import OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import deploy_gpfs_scale, run_suite_cleanup, should_skip_deployment
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    clients = ceph_cluster.get_nodes("client")
    export_name = config.get("nfs_export") or environ.get("EXPORT_NAME", "/ibm/scale_volume")

    log.info("Setup nfs cluster")
    try:
        server = ceph_cluster.get_nodes("installer")[0]

        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        # Perform mount on client
        cmds = ["dnf -y install git wget gcc nfs-utils time make",
                "mkdir -p /mnt/nfsv3",
                f"mount -t nfs -o vers=3 {server.ip_address}:{export_name} /mnt/nfsv3",
                "mkdir -p /mnt/nfsv4",
                f"mount -t nfs -o vers=4 {server.ip_address}:{export_name} /mnt/nfsv4"
                ]

        for cmd in cmds:
            clients[0].exec_command(cmd=cmd, sudo=True)
            clients[1].exec_command(cmd=cmd, sudo=True)

        # Install pre-req
        cmd = "sudo dnf install -y git gcc gcc-c++ make automake autoconf " \
              "pkgconf pkgconf-pkg-config libtool bison flex " \
              "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
        clients[0].exec_command(cmd=cmd, sudo=True)
        clients[1].exec_command(cmd=cmd, sudo=True)

        # clone LTP
        cmd = "git clone https://github.com/linux-test-project/ltp.git"
        cmd += " && git clone https://github.com/linux-test-project/kirk.git"
        clients[0].exec_command(cmd=cmd, sudo=True)
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Build LTP
        cmd = "cd ltp;make autotools;./configure;make -j$(nproc);sudo make install"
        clients[0].exec_command(cmd=cmd, sudo=True, timeout=600)
        clients[1].exec_command(cmd=cmd, sudo=True, timeout=600)

        # Run LTP test (kirk is the current test runner; runltp was removed)
        v3_mount = "/mnt/nfsv3"
        v4_mount = "/mnt/nfsv4"

        log.info("Running LTP on v3 mount")
        cmd = (
            f"sudo sh -c 'cd ~/kirk && TMPDIR={v3_mount} ./kirk -f fs 2>&1 -d {v3_mount}"
            "| tee /tmp/ltp_run_v3.log > /tmp/ltp_output_v3.log'"
        )
        clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)

        log.info("Running LTP on v4 mount")
        cmd = (
            f"sudo sh -c 'cd ~/kirk && TMPDIR={v4_mount} ./kirk -f fs 2>&1 -d {v4_mount}"
            "| tee /tmp/ltp_run_v4.log > /tmp/ltp_output_v4.log'"
        )
        clients[1].exec_command(cmd=cmd, sudo=True, timeout=10400)

    except OperationFailedError:
        raise
    except Exception as e:
        log.error("LTP setup/run failed: %s", e)
        raise OperationFailedError(f"LTP setup/run failed: {e}") from e
    finally:
        # sleep(30)
        # view test results
        log.info("===================RUN_RESULTS - V3====================")
        cmd = "cat /tmp/ltp_output_v3.log"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        log.info("===================RUN_LOG - V3====================")
        cmd = "cat /tmp/ltp_run_v3.log"
        out = clients[0].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        log.info("===================RUN_RESULTS - V4====================")
        cmd = "cat /tmp/ltp_output_v4.log"
        out = clients[1].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        log.info("===================RUN_LOG - V4====================")
        cmd = "cat /tmp/ltp_run_v4.log"
        out = clients[1].exec_command(cmd=cmd, sudo=True)
        log.info(out)

        run_suite_cleanup(ceph_cluster, config)

    return 0
