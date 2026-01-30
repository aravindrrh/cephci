from utility.log import Log
from os import environ

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")
    clients = ceph_cluster.get_nodes("client")
    skip_deployment = environ['SKIP_DEPLOYMENT']
    export_name = environ['EXPORT_NAME']

    log.info("Setup nfs cluster")
    try:
        server = ceph_cluster.get_nodes("installer")[0]
        client = ceph_cluster.get_nodes("client")[0]

        if skip_deployment == "true":
            log.info("Skipping installation and deployment")
        else:
            cmds = ["rm -rf ci-tests/",
                    "yum install -y git wget",
                    "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
                    "sh ci-tests/build_scripts/common/basic-storage-scale.sh"]

            for cmd in cmds:
                server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=3600)

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
        clients[0].exec_command(cmd=cmd, sudo=True)
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Build LTP
        cmd = "cd ltp;make autotools;./configure;make -j$(nproc);sudo make install"
        clients[0].exec_command(cmd=cmd, sudo=True)
        clients[1].exec_command(cmd=cmd, sudo=True)

        # Run LTP test
        v3_mount = "/mnt/nfsv3"
        v4_mount = "/mnt/nfsv4"

        log.info("Running LTP on v3 mount")
        cmd = f"cd /opt/ltp; sudo ./runltp -d {v3_mount} -f fs " \
              "-o /tmp/ltp_output_v3.log -l /tmp/ltp_run_v3.log -p"
        clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)

        log.info("Running LTP on v4 mount")
        cmd = f"cd /opt/ltp; sudo ./runltp -d {v4_mount} -f fs " \
              "-o /tmp/ltp_output_v4.log -l /tmp/ltp_run_v4.log -p"
        clients[1].exec_command(cmd=cmd, sudo=True, timeout=10400)

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
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

    return 0
