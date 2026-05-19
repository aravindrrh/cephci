from cli.exceptions import OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    DEFAULT_CI_TESTS_BRANCH,
    deploy_gpfs_scale,
    run_suite_cleanup,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config") or {}
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]

    try:
        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        cmds = [f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
                f'echo "export EXPORT=\"/ibm/scale_volume\"" >> ~/.bashrc && source ~/.bashrc',
                "rm -rf ci-tests/",
                "yum install -y git wget",
                f"git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout {DEFAULT_CI_TESTS_BRANCH}",
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
    finally:
        run_suite_cleanup(ceph_cluster, config)

    return 0
