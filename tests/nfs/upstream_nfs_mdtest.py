from os import environ

from cli.exceptions import OperationFailedError
from utility.log import Log

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
                    "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
                    "sh ci-tests/build_scripts/common/basic-storage-scale.sh"]
            for cmd in cmds:
                exit_code = server.exec_command(
                    cmd=cmd, sudo=True, long_running=True,
                )
                if exit_code != 0:
                    raise OperationFailedError(
                        f"Mdtest server command failed (exit {exit_code}): {cmd}"
                    )

        # Install pre-req
        cmd = "sudo dnf install -y wget git gcc gcc-c++ time make automake autoconf " \
              "pkgconf pkgconf-pkg-config libtool bison flex " \
              "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = "sudo yum groupinstall -y Development Tools mpich mpich-devel;"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = """{
    echo 'export CFLAGS="-I/usr/include/mpich-x86_64"'
    echo 'export LDFLAGS="-L/usr/lib64/mpich"'
    echo 'export LD_LIBRARY_PATH="/usr/lib64/mpich/lib:$LD_LIBRARY_PATH"'
    echo 'export PATH="/usr/lib64/mpich/bin:$PATH"'
} >> ~/.bashrc"""
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = "source ~/.bashrc"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = """git clone https://github.com/hpc/ior.git;cd ior;./bootstrap;./configure;sudo make install"""
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Perform mount on client
        for nfs_mount, ver in {'/mnt/nfsv3':'3', '/mnt/nfsv4':'4'}.items():
            cmds = [f"mkdir -p {nfs_mount}",
                    f"mount -t nfs -o vers={ver} {server.ip_address}:{export_name} {nfs_mount}",
                    f"export TESTDIR={nfs_mount}"
                    ]
            for cmd in cmds:
                clients[0].exec_command(cmd=cmd, sudo=True)

            cmd = f"cd {nfs_mount};mdtest -d {nfs_mount} -n 1000"
            exit_code = clients[0].exec_command(
                sudo=True,
                cmd=cmd,
                long_running=True,
                timeout=7200
            )
            if exit_code != 0:
                log.error(
                    "Mdtest client command failed with exit code %s: %s",
                    exit_code, cmd,
                )
                raise OperationFailedError(
                    f"Mdtest client command failed (exit {exit_code}): {cmd}"
                )

    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Mdtest setup/run failed: %s", e)
        raise OperationFailedError(f"Mdtest setup/run failed: {e}") from e
    finally:
        pass
    return 0
