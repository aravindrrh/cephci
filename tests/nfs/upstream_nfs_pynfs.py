from os import environ

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
    export_name = config.get("nfs_export") or environ.get("EXPORT_NAME", "/ibm/scale_volume")

    try:
        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        cmds = [f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
                f'echo "export EXPORT=\"{export_name}\"" >> ~/.bashrc && source ~/.bashrc',
                "rm -rf ci-tests/",
                "yum install -y git wget",
                f"git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout {DEFAULT_CI_TESTS_BRANCH}",
                "sh ci-tests/build_scripts/pynfs/client.sh"]

        pynfs_timeout = int(config.get("pynfs_timeout", config.get("timeout", 14400)))
        for cmd in cmds:
            exit_code = client.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout=pynfs_timeout
            )
            if exit_code != 0:
                log.error(
                    "Pynfs client command failed with exit code %s: %s",
                    exit_code, cmd,
                )
                raise OperationFailedError(
                    f"Pynfs client command failed (exit {exit_code}): {cmd}"
                )
    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Pynfs setup/run failed: %s", e)
        raise OperationFailedError(f"Pynfs setup/run failed: {e}") from e
    finally:
        run_suite_cleanup(ceph_cluster, config)

    return 0
