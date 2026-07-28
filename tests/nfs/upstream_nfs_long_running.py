"""
Long-running NFS-Ganesha stress test on an already-deployed Scale export.

Does **not** deploy Scale or Ganesha — run after upstream_nfs_deploy / tools suite.
Client clones ci-tests and runs long_running_tests.sh (NFSv3 mount + multi-hour IO).
"""

from os import environ

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import run_suite_cleanup
from utility.log import Log

log = Log(__name__)

DEFAULT_NFS_EXPORT = "/ibm/scale_volume/export1"
CI_TESTS_REPO = "https://github.com/aravindrrh/ci-tests"
CI_TESTS_BRANCH = "scale_downstream"
LONG_RUNNING_SCRIPT = (
    "ci-tests/build_scripts/storage-scale/long_running_tests.sh"
)


def run(ceph_cluster, **kw):
    """
    Run ci-tests long_running_tests.sh on the first client.

    Config keys:
        nfs_export: Scale NFS export path (default /ibm/scale_volume/export1)
        clients: unused beyond requiring at least one client (default 1)

    Expects Scale + Ganesha + export already deployed. Cleans client mounts
    (and related dirs) in ``finally``.
    """
    config = dict(kw.get("config") or {})
    nfs_export = (
        config.get("nfs_export")
        or environ.get("EXPORT_NAME")
        or DEFAULT_NFS_EXPORT
    )

    installers = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")
    if not installers:
        raise ConfigError("Long running test requires an installer node")
    if not clients:
        raise ConfigError("Long running test requires at least one client node")

    server = installers[0]
    client = clients[0]

    log.info(
        "\n"
        + "=" * 70
        + "\n"
        + "  UPSTREAM NFS — long-running stress (no deploy)\n"
        + "  server=%s  export=%s  client=%s\n"
        + "=" * 70,
        server.ip_address,
        nfs_export,
        client.hostname,
    )

    cmds = [
        # Prefer env for this shell; also append bashrc for the stress script.
        f"export SERVER={server.ip_address!r}; export EXPORT={nfs_export!r}; "
        f'grep -q "^export SERVER=" ~/.bashrc || '
        f'echo "export SERVER=\\"{server.ip_address}\\"" >> ~/.bashrc; '
        f'grep -q "^export EXPORT=" ~/.bashrc || '
        f'echo "export EXPORT=\\"{nfs_export}\\"" >> ~/.bashrc; '
        f"true",
        "rm -rf ci-tests/ /root/ci-tests",
        "yum install -y git wget || dnf install -y git wget",
        f"git clone {CI_TESTS_REPO}; cd ci-tests; git checkout {CI_TESTS_BRANCH}",
        f"export SERVER={server.ip_address!r}; export EXPORT={nfs_export!r}; "
        f"sh {LONG_RUNNING_SCRIPT}",
    ]

    try:
        for cmd in cmds:
            log.info("[%s] %s", client.hostname, cmd)
            exit_code = client.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout="notimeout"
            )
            if exit_code != 0:
                log.error(
                    "Long running client command failed with exit code %s: %s",
                    exit_code,
                    cmd,
                )
                raise OperationFailedError(
                    f"Long running client command failed (exit {exit_code}): {cmd}"
                )
        log.info("Long running stress script completed successfully")
        return 0
    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Long running tests setup/run failed: %s", e)
        raise OperationFailedError(
            f"Long running tests setup/run failed: {e}"
        ) from e
    finally:
        # Drop stress leftovers, then sweep /mnt + leftover NFS mounts.
        try:
            client.exec_command(
                cmd=(
                    "bash -lc 'rm -rf /tmp/ganesha_test ci-tests /root/ci-tests "
                    "2>/dev/null || true'"
                ),
                sudo=True,
                check_ec=False,
            )
        except Exception as exc:
            log.warning("Client artifact cleanup failed: %s", exc)
        try:
            run_suite_cleanup(ceph_cluster, config)
        except Exception as exc:
            log.warning("Suite mount cleanup failed: %s", exc)
