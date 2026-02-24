from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]

    try:
        cmds = ["rm -rf ci-tests/",
                "yum install -y git wget",
                "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
                "sh ci-tests/build_scripts/common/basic-storage-scale.sh"]

        for cmd in cmds:
            exit_code = server.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout=5400
            )
            if exit_code != 0:
                raise OperationFailedError(
                    f"Cthon server command failed (exit {exit_code}): {cmd}"
                )

        cmds = [f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
                f'echo "export EXPORT=\"/ibm/scale_volume\"" >> ~/.bashrc && source ~/.bashrc',
                "rm -rf ci-tests/",
                "yum install -y git wget",
                "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
                "sh ci-tests/build_scripts/storage-scale/client.sh"]

        for cmd in cmds:
            exit_code = client.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout=5400
            )
            if exit_code != 0:
                log.error(
                    "Cthon client command failed with exit code %s: %s",
                    exit_code,
                    cmd,
                )
                raise OperationFailedError(
                    f"Cthon client command failed (exit {exit_code}): {cmd}"
                )

    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Cthon setup/run failed: %s", e)
        raise OperationFailedError(f"Cthon setup/run failed: {e}") from e

    return 0
