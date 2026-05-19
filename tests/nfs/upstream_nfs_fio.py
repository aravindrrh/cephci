from cli.exceptions import OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import deploy_gpfs_scale, run_suite_cleanup, should_skip_deployment
from utility.log import Log

log = Log(__name__)


def run (ceph_cluster, **kw):
    config = kw.get("config") or {}
    clients = ceph_cluster.get_nodes("client")

    log.info("Setup nfs cluster")
    try:
        server = ceph_cluster.get_nodes("installer")[0]

        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        # Perform mount on client
        cmds = ["mkdir -p /mnt/nfsv3",
                f"mount -t nfs -o vers=3 {server.ip_address}:/ibm/scale_volume /mnt/nfsv3",
                "mkdir -p /mnt/nfsv4",
                f"mount -t nfs -o vers=4 {server.ip_address}:/ibm/scale_volume /mnt/nfsv4"
                ]

        for cmd in cmds:
            clients[0].exec_command(cmd=cmd, sudo=True)
            clients[1].exec_command(cmd=cmd, sudo=True)

        cmd = "yum install -y fio"
        clients[0].exec_command(cmd=cmd, sudo=True)

        fio_job_content = """[global]
ioengine=libaio
direct=1
runtime=60
time_based
size=1G
directory=/mnt/nfsv3
group_reporting

[seqwrite]
rw=write
bs=1M

[seqread]
rw=read
bs=1M

[randrw]
rw=randrw
rwmixread=70
bs=4k
numjobs=4"""
        filepath = "/mnt/nfsv3/ganesha_test.fio"
        cmd = f"cat <<'EOF' > {filepath}\n{fio_job_content}\nEOF"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Run on V3
        log.info("Running FIO on V3")
        output = "/mnt/nfsv3/fio_results.json"
        cmd = f"fio {filepath} --output={output} --output-format=json"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
        log.info("Execution complted on v3 mount")
        cmd = f"cat {output}"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True)

        fio_job_content = """[global]
ioengine=libaio
direct=1
runtime=60
time_based
size=1G
directory=/mnt/nfsv4
group_reporting

[seqwrite]
rw=write
bs=1M

[seqread]
rw=read
bs=1M

[randrw]
rw=randrw
rwmixread=70
bs=4k
numjobs=4"""
        filepath = "/mnt/nfsv4/ganesha_test.fio"
        cmd = f"cat <<'EOF' > {filepath}\n{fio_job_content}\nEOF"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Run on V4
        log.info("Running FIO on V4")
        output = "/mnt/nfsv4/fio_results.json"
        cmd = f"fio {filepath} --output={output} --output-format=json"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=10400)
        log.info("Execution complted on v4 mount")
        cmd = f"cat {output}"
        out, _ = clients[0].exec_command(cmd=cmd, sudo=True)

    except OperationFailedError:
        raise
    except Exception as e:
        log.error("FIO run failed: %s", e)
        raise OperationFailedError(f"FIO run failed: {e}") from e
    finally:
        run_suite_cleanup(ceph_cluster, config)
    return 0
