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
                cmd=cmd, sudo=True, long_running=True, timeout="notimeout"
            )
            if exit_code != 0:
                raise OperationFailedError(
                    f"Long running server command failed (exit {exit_code}): {cmd}"
                )

        cmds = [f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
                f'echo "export EXPORT=\"/ibm/scale_volume\"" >> ~/.bashrc && source ~/.bashrc',
                "rm -rf ci-tests/",
                "yum install -y git wget",
                "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
                "sh ci-tests/build_scripts/storage-scale/long_running_tests.sh"]

        for cmd in cmds:
            exit_code = client.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout="notimeout"
            )
            if exit_code != 0:
                log.error(
                    "Long running client command failed with exit code %s: %s",
                    exit_code, cmd,
                )
                raise OperationFailedError(
                    f"Long running client command failed (exit {exit_code}): {cmd}"
                )
    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Long running tests setup/run failed: %s", e)
        raise OperationFailedError(
            f"Long running tests setup/run failed: {e}"
        ) from e

    return 0