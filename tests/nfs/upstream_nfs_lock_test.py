import json
import time

from cli.exceptions import OperationFailedError
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    DEFAULT_CI_TESTS_BRANCH,
    deploy_gpfs_scale,
    run_suite_cleanup,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)

NFSTEST_REPO = "git://git.linux-nfs.org/projects/mora/nfstest.git"
NFSTEST_DIR = "/root/nfstest"
LOCK_MOUNT_POINTS = ("/mnt/nfsv3", "/mnt/nfsv4")
_DEBUG_LOG = "/Users/arunravi/work/cephci/.cursor/debug-085da4.log"


def _agent_debug_log(hypothesis_id, location, message, data):
    # #region agent log
    try:
        with open(_DEBUG_LOG, "a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "sessionId": "085da4",
                        "hypothesisId": hypothesis_id,
                        "location": location,
                        "message": message,
                        "data": data,
                        "timestamp": int(time.time() * 1000),
                    }
                )
                + "\n"
            )
    except OSError:
        pass
    # #endregion


def _nfstest_run_log_path(nfs_version):
    """Per-version redirect path so nfstest_lock stdout stays off the Jenkins console."""
    ver = nfs_version.replace(".", "_")
    return f"/tmp/nfstest_lock_run_v{ver}.log"


def _log_nfstest_failure(client, nfs_version, exit_code, run_log=""):
    """Pull nfstest_lock log tail into cephci logs for post-mortem analysis."""
    log_path = ""
    tail = ""
    summary = ""
    run_tail = ""
    try:
        log_path, _ = client.exec_command(
            cmd="ls -t /tmp/nfstest_lock_*.log 2>/dev/null | head -1",
            sudo=True,
            check_ec=False,
        )
        log_path = (log_path or "").strip()
        if log_path:
            tail, _ = client.exec_command(
                cmd=f"tail -80 {log_path}",
                sudo=True,
                check_ec=False,
            )
            summary, _ = client.exec_command(
                cmd=f"grep -E 'tests \\(|FAIL|failed' {log_path} | tail -5",
                sudo=True,
                check_ec=False,
            )
        if run_log:
            run_tail, _ = client.exec_command(
                cmd=f"tail -80 {run_log}",
                sudo=True,
                check_ec=False,
            )
    except Exception as exc:
        log.warning("Could not read nfstest_lock log on %s: %s", client.hostname, exc)

    log.error(
        "nfstest_lock V%s failed (exit=%s) log=%s summary=%s run_log=%s "
        "createlog tail:\n%s\nrun_log tail:\n%s",
        nfs_version,
        exit_code,
        log_path,
        (summary or "").strip(),
        run_log,
        tail,
        run_tail,
    )
    _agent_debug_log(
        "H-LOCK",
        "upstream_nfs_lock_test.py:_log_nfstest_failure",
        "nfstest_lock failed",
        {
            "nfs_version": nfs_version,
            "exit_code": exit_code,
            "log_path": log_path,
            "summary": (summary or "").strip(),
        },
    )


def _deploy_scale_nfs(server, config):
    """Deploy Spectrum Scale NFS (single-node script used by the passing lock suite)."""
    branch = config.get("ci_tests_branch", DEFAULT_CI_TESTS_BRANCH)
    timeout = int(config.get("deploy_timeout", 7200))
    deploy_cmds = [
        "rm -rf ci-tests/",
        "yum install -y git wget",
        f"git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout {branch}",
        "sh ci-tests/build_scripts/common/basic-storage-scale.sh",
    ]
    for cmd in deploy_cmds:
        rc = server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=timeout)
        if rc != 0:
            raise OperationFailedError(
                f"Lock test deploy command failed (exit {rc}): {cmd}"
            )


def _umount_lock_mount_points(client):
    for mount_point in LOCK_MOUNT_POINTS:
        client.exec_command(
            cmd=f"umount -f {mount_point} 2>/dev/null || true",
            sudo=True,
            check_ec=False,
        )
        client.exec_command(
            cmd=f"umount -l {mount_point} 2>/dev/null || true",
            sudo=True,
            check_ec=False,
        )


def _mount_clients_for_lock_test(server, clients, export):
    mount_cmds = [
        "dnf -y install git wget gcc nfs-utils time make rpcbind",
        "systemctl enable --now rpcbind",
    ]
    for client in clients[:2]:
        _umount_lock_mount_points(client)
        for cmd in mount_cmds:
            client.exec_command(cmd=cmd, sudo=True)
        client.exec_command(cmd="mkdir -p /mnt/nfsv3", sudo=True)
        client.exec_command(
            cmd=f"mount -t nfs -o vers=3 {server.ip_address}:{export} /mnt/nfsv3",
            sudo=True,
        )
        client.exec_command(cmd="mkdir -p /mnt/nfsv4", sudo=True)
        client.exec_command(
            cmd=f"mount -t nfs -o vers=4 {server.ip_address}:{export} /mnt/nfsv4",
            sudo=True,
        )


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    clients = ceph_cluster.get_nodes("client")
    server = ceph_cluster.get_nodes("installer")[0]
    client = clients[0]
    nfstest_lock = f"{NFSTEST_DIR}/test/nfstest_lock"
    export = config.get("nfs_export", "/ibm/scale_volume")

    try:
        if not should_skip_deployment(config):
            if config.get("multi_node_deploy"):
                deploy_gpfs_scale(ceph_cluster, config)
            else:
                _deploy_scale_nfs(server, config)
        else:
            log.info("skip_deployment set — using cluster from suite deploy step")

        _mount_clients_for_lock_test(server, clients, export)

        log.info(">>> Installing required packages...")
        client.exec_command(
            cmd=(
                "dnf install -y git python3 python3-devel tcpdump "
                "wireshark sshpass firewalld"
            ),
            sudo=True,
        )

        log.info(">>> Enabling and configuring firewalld...")
        client.exec_command(cmd="systemctl enable firewalld --now", sudo=True)
        client.exec_command(
            cmd="firewall-cmd --zone=public --add-port=9900-9920/tcp --permanent",
            sudo=True,
        )
        client.exec_command(cmd="firewall-cmd --reload", sudo=True)
        client.exec_command(cmd="firewall-cmd --zone=public --list-ports", sudo=True)

        log.info(">>> Cloning nfstest repo...")
        client.exec_command(cmd=f"rm -rf {NFSTEST_DIR}", sudo=True)
        client.exec_command(cmd=f"git clone {NFSTEST_REPO} {NFSTEST_DIR}", sudo=True)

        log.info(">>> Configuring PYTHONPATH...")
        export_line = f"export PYTHONPATH={NFSTEST_DIR}"
        grep_cmd = (
            f"grep -qxF '{export_line}' ~/.bashrc || echo '{export_line}' >> ~/.bashrc"
        )
        client.exec_command(cmd=grep_cmd, sudo=True)

        log.info(">>> Verifying nfstest_lock exists...")
        client.exec_command(cmd=f"ls {nfstest_lock}", sudo=True)

        for nfs_version in ("3", "4", "4.1"):
            log.info(">>> Running nfstest_lock sanity test for V%s", nfs_version)
            run_log = _nfstest_run_log_path(nfs_version)
            # Redirect stdout/stderr to a file on the client — nfstest_lock DBG3
            # polling can emit millions of lines and must not stream to Jenkins.
            test_cmd = (
                f"bash -lc 'PYTHONPATH={NFSTEST_DIR} {nfstest_lock} "
                f"--server {server.ip_address} --export {export} "
                f"--nfsversion {nfs_version} --createlog "
                f">{run_log} 2>&1'"
            )
            exit_code = client.exec_command(
                cmd=test_cmd,
                sudo=True,
                long_running=True,
                timeout=7200,
                check_ec=False,
            )
            if exit_code != 0:
                _log_nfstest_failure(client, nfs_version, exit_code, run_log)
                raise OperationFailedError(
                    f"nfstest_lock V{nfs_version} failed with exit {exit_code} "
                    f"on {client.ip_address} (see {run_log} on client)"
                )

        log.info("NFS locking test completed successfully.")
        return 0

    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Lock test setup/run failed: %s", e)
        raise OperationFailedError(f"Lock test setup/run failed: {e}") from e
    finally:
        run_suite_cleanup(ceph_cluster, config)
