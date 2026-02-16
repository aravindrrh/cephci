from utility.log import Log
from os import environ

log = Log(__name__)


def run (ceph_cluster, **kw):
    clients = ceph_cluster.get_nodes("client")
    log.info("Setup nfs cluster")
    skip_deployment = environ['SKIP_DEPLOYMENT']
    export_name = environ['EXPORT_NAME']

    try:
        server = ceph_cluster.get_nodes("installer")[0]

        if skip_deployment == "true":
            log.info("Skipping installation and deployment")
        else:
            cmds = ["rm -rf ci-tests/",
                    "yum install -y git wget",
                    "git clone https://github.com/aravindrrh/ci-tests.git; cd ci-tests; git checkout scale_downstream",
                    #"sh ci-tests/build_scripts/common/basic-storage-scale.sh"]
                    "sh ci-tests/build_scripts/common/basic-storage-scale-multi-node.sh"]
            for cmd in cmds:
                server.exec_command(cmd=cmd, sudo=True, long_running=True,)

        # Copy Gerontion folder to all the clients
        for client in clients:
            for cmd in ["mkdir /mnt/gpfstests", "curl -O http://10.0.210.156/gpfstests/ /mnt/gpfstests/gerontion"]:
                client.exec_command(cmd=cmd, sudo=True)

        # Install pre-req
        for client in clients:
            cmd = "sudo dnf install -y wget git gcc gcc-c++ time make automake autoconf " \
                  "pkgconf pkgconf-pkg-config libtool bison flex " \
                  "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
            client.exec_command(cmd=cmd, sudo=True)

        # Perform mount on all client with different mount versions
        for nfs_mount, ver in {'/mnt/nfsv3':'3', '/mnt/nfsv4_1':'4.1', '/mnt/nfsv4_2':'4.2'}.items():
            cmds = [f"mkdir -p {nfs_mount}",
                    f"mount -t nfs -o vers={ver} {server.ip_address}:{export_name} {nfs_mount}",
                    f"export TESTDIR={nfs_mount}"
                    ]
            for client in clients:
                for cmd in cmds:
                    client.exec_command(cmd=cmd, sudo=True)

        # Run gerontion tests

        # Run Racer
        for mount in ["/mnt/nfsv3", "/mnt/nfsv4_1", "/mnt/nfsv4_2"]:
            cmd = f"/mnt/gpfstests/gerontion/gerontion -N {clients[0].ip_address} -F {mount} racer"
            out = clients[0].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: Racer , Mount : {mount}, result : {out}")

        # Run cdata
        for mount in ["/mnt/nfsv3", "/mnt/nfsv4_1", "/mnt/nfsv4_2"]:
            cmd = f"/mnt/gpfstests/gerontion/gerontion -N {clients[1].ip_address} -F {mount} cdata"
            clients[1].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: cdata, Mount : {mount}, result : {out}")

        # Run locktest
        for mount in ["/mnt/nfsv3", "/mnt/nfsv4_1", "/mnt/nfsv4_2"]:
            cmd = f"/mnt/gpfstests/gerontion/gerontion -N {clients[1].ip_address} -F {mount} locktest"
            clients[2].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: Locktest, Mount : {mount}, result : {out}")

        # Run blast
        for mount in ["/mnt/nfsv3", "/mnt/nfsv4_1", "/mnt/nfsv4_2"]:
            cmd = f"/mnt/gpfstests/gerontion/gerontion -N {clients[1].ip_address} -F {mount} blast"
            clients[3].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: Blast, Mount : {mount}, result : {out}")


    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        pass
    return 0

