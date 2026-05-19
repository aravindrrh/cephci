from os import environ

from cli.exceptions import OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import deploy_gpfs_scale, run_suite_cleanup, should_skip_deployment
from utility.log import Log

log = Log(__name__)


def run (ceph_cluster, **kw):
    clients = ceph_cluster.get_nodes("client")
    log.info("Setup nfs cluster")
    config = kw.get("config") or {}
    export_name = config.get("nfs_export") or environ.get("EXPORT_NAME", "/ibm/scale_volume")

    try:
        server = ceph_cluster.get_nodes("installer")[0]

        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        # Install pre-req
        cmd = "sudo dnf install -y wget git gcc gcc-c++ time make automake autoconf " \
              "openmpi openmpi-devel pkgconf pkgconf-pkg-config libtool bison flex " \
              "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmds = ["subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms",
                "sudo dnf groupinstall -y \"Development Tools\"",
                "sudo dnf install -y mpich mpich-devel;"
        ]
        for cmd in cmds:
            clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = """{
    echo 'export CFLAGS="-I/usr/include/mpich-x86_64"'
    echo 'export LDFLAGS="-L/usr/lib64/mpich"'
    echo 'export LD_LIBRARY_PATH="/usr/lib64/mpich/lib:$LD_LIBRARY_PATH"'
    echo 'export PATH="/usr/lib64/mpich/bin:$PATH"'
} >> ~/.bashrc"""
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = "source ~/.bashrc; source /etc/profile.d/modules.sh; module load mpi/openmpi-x86_64"
        clients[0].exec_command(cmd=cmd, sudo=True)

        cmd = """git clone https://github.com/hpc/ior.git;cd ior; sed -i 's/^AC_PREREQ(\[2\.71\])/AC_PREREQ([2.69])/' configure.ac; ./bootstrap && ./configure && make install"""
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
        run_suite_cleanup(ceph_cluster, config)
    return 0
