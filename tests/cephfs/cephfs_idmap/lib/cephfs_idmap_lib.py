"""
Shared helpers for CephFS idmapped mount functional tests.

Standalone validation of kernel CephFS idmap support (no ODF/Kubernetes).
Target: Tentacle on RHEL 10.2 clients.
"""

import random
import string
import traceback

from ceph.ceph import CommandFailed
from tests.cephfs.cephfs_utilsV1 import FsUtils
from tests.cephfs.lib.cephfs_common_lib import CephFSCommonUtils
from utility.log import Log

log = Log(__name__)

FS_NAME = "cephfs"
IDMAP_BASE_UID = 100000
IDMAP_BASE_UID_B = 200000
IDMAP_RANGE = 65536
DEFAULT_IDMAP_SPEC = f"b:0:{IDMAP_BASE_UID}:{IDMAP_RANGE}"
ALT_IDMAP_SPEC = f"b:0:{IDMAP_BASE_UID_B}:{IDMAP_RANGE}"
DMESG_FAIL_PATTERNS = ("mount_setattr",)
# Matches test_client.py steps in tier-2_cephfs_test-idmap.yaml (node8, node9).
CONFIGURED_CLIENT_COUNT = 2


def get_configured_clients(ceph_cluster, test_data=None, config=None):
    """
    Return only client nodes prepared by the suite bootstrap.

    Cluster conf may define more client-role nodes than test_client.py configures.
    """
    all_clients = ceph_cluster.get_ceph_objects("client")
    count = CONFIGURED_CLIENT_COUNT

    if test_data and test_data.get("idmap_suite", {}).get("configured_client_count"):
        count = test_data["idmap_suite"]["configured_client_count"]
    if config and config.get("configured_client_count"):
        count = config["configured_client_count"]

    if len(all_clients) < count:
        raise CommandFailed(
            f"Expected at least {count} configured client node(s), "
            f"found {len(all_clients)} with client role"
        )

    clients = all_clients[:count]
    log.info(
        "Using %d configured client(s): %s",
        len(clients),
        ", ".join(client.node.hostname for client in clients),
    )
    if len(all_clients) > count:
        skipped = all_clients[count:]
        log.info(
            "Skipping %d unconfigured client node(s): %s",
            len(skipped),
            ", ".join(client.node.hostname for client in skipped),
        )
    return clients


class IdmapTestHelper:
    """Utilities for idmapped CephFS kernel mount tests."""

    def __init__(self, ceph_cluster, test_data=None):
        self.ceph_cluster = ceph_cluster
        self.test_data = test_data or {}
        self.fs_util = FsUtils(ceph_cluster, test_data=self.test_data)
        self.common_utils = CephFSCommonUtils(ceph_cluster)
        self.mon_node_ips = self.fs_util.get_mon_node_ips()

    @staticmethod
    def random_suffix(length=8):
        return "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(length)
        )

    def prepare_clients(self, clients, build):
        for client in clients:
            self.ensure_ceph_kernel_module(client)
        self.fs_util.prepare_clients(clients, build)
        self.fs_util.auth_list(clients)
        self._ensure_filesystem(clients[0])

    def _ensure_filesystem(self, client):
        if not self.fs_util.get_fs_info(client, FS_NAME):
            self.fs_util.create_fs(client, FS_NAME)

    def mount_paths(self, suffix=None):
        suffix = suffix or self.random_suffix()
        plain = f"/mnt/cephfs_idmap_plain_{suffix}"
        idmap = f"/mnt/cephfs_idmap_view_{suffix}"
        return plain, idmap, suffix

    def record_environment_versions(self, client):
        cmds = {
            "kernel": "uname -r",
            "ceph": "ceph version",
            "mount": "mount --version | head -1",
            "ceph_common": "rpm -q ceph-common 2>/dev/null || dpkg -l ceph-common 2>/dev/null | tail -1",
            "util_linux": "rpm -q util-linux 2>/dev/null || dpkg -l util-linux 2>/dev/null | tail -1",
        }
        versions = {}
        for key, cmd in cmds.items():
            out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            versions[key] = (out or "").strip()
            log.info("Environment %s: %s", key, versions[key])
        return versions

    def ensure_ceph_kernel_module(self, client):
        """
        Load the CephFS kernel client module on the client node.

        The module may be installed but not listed in /proc/filesystems until loaded.
        """
        client.exec_command(
            sudo=True,
            cmd=(
                "dnf install -y kernel-modules-extra 2>/dev/null || "
                "yum install -y kernel-modules-extra"
            ),
            check_ec=False,
        )
        _out, err, exit_code, _duration = client.exec_command(
            sudo=True,
            cmd="modprobe ceph",
            check_ec=False,
            verbose=True,
        )
        if exit_code != 0:
            modinfo_out, _ = client.exec_command(
                sudo=True, cmd="modinfo ceph 2>/dev/null", check_ec=False
            )
            if not modinfo_out:
                raise CommandFailed(
                    "CephFS kernel module is not installed on client; "
                    f"modprobe ceph failed: {err}"
                )
            log.warning(
                "modprobe ceph returned %s but modinfo is available; continuing",
                exit_code,
            )

    def check_cephfs_kernel_module(self, client):
        self.ensure_ceph_kernel_module(client)
        out, _ = client.exec_command(
            sudo=True, cmd="grep -w ceph /proc/filesystems", check_ec=False
        )
        if "ceph" not in (out or ""):
            modinfo_out, _ = client.exec_command(
                sudo=True, cmd="modinfo ceph 2>/dev/null", check_ec=False
            )
            if not modinfo_out:
                raise CommandFailed(
                    "CephFS kernel client not available in /proc/filesystems "
                    "and modinfo ceph returned no data"
                )
            log.info(
                "ceph kernel module available (modinfo OK); "
                "/proc/filesystems will populate on first mount"
            )

    def check_cluster_health(self, client, wait_time=300):
        if self.common_utils.wait_for_healthy_ceph(client, wait_time):
            raise CommandFailed("Cluster health is not OK")
        out, _ = client.exec_command(sudo=True, cmd="ceph fs status")
        if "active" not in (out or "").lower():
            raise CommandFailed(f"No active MDS reported by ceph fs status:\n{out}")

    def kernel_mount_plain(self, client, mount_point, fs_name=FS_NAME):
        self.fs_util.kernel_mount(
            [client],
            mount_point,
            ",".join(self.mon_node_ips),
            extra_params=f",fs={fs_name}",
        )

    def _parse_idmap_spec(self, map_spec):
        """Parse X-mount.idmap spec (e.g. b:0:100000:65536) into inner/outer/count."""
        parts = map_spec.split(":")
        if parts[0] == "b" and len(parts) == 4:
            inner, outer, count = parts[1], parts[2], parts[3]
        elif len(parts) == 3:
            inner, outer, count = parts
        else:
            raise CommandFailed(f"Unsupported idmap spec: {map_spec}")
        return inner, outer, count

    def idmap_bind_mount(self, client, plain_mount, idmap_mount, map_spec=DEFAULT_IDMAP_SPEC):
        client.exec_command(sudo=True, cmd=f"mkdir -p {plain_mount} {idmap_mount}")
        inner, outer, count = self._parse_idmap_spec(map_spec)
        bind_cmd = (
            f"mount --bind --map-users {inner}:{outer}:{count} "
            f"--map-groups {inner}:{outer}:{count} "
            f"{plain_mount} {idmap_mount}"
        )
        self.exec_cmd(client, bind_cmd)

    def setup_plain_and_idmap_mounts(
        self, client, map_spec=DEFAULT_IDMAP_SPEC, suffix=None
    ):
        plain, idmap, suffix = self.mount_paths(suffix)
        self.kernel_mount_plain(client, plain)
        self.idmap_bind_mount(client, plain, idmap, map_spec=map_spec)
        return plain, idmap, suffix

    def umount_plain(self, client, plain_mount):
        client.exec_command(sudo=True, cmd=f"umount {plain_mount}", check_ec=False)
        client.exec_command(sudo=True, cmd=f"rm -rf {plain_mount}", check_ec=False)

    def umount_idmap_stack(self, client, idmap_mount, plain_mount):
        for mount in (idmap_mount, plain_mount):
            client.exec_command(
                sudo=True, cmd=f"umount {mount}", check_ec=False
            )
        client.exec_command(
            sudo=True,
            cmd=f"rm -rf {idmap_mount} {plain_mount}",
            check_ec=False,
        )

    def get_stat_uid_gid(self, client, path):
        out, _ = client.exec_command(
            sudo=True, cmd=f"stat -c '%u %g' {path}", check_ec=False
        )
        parts = (out or "").strip().split()
        if len(parts) != 2:
            raise CommandFailed(f"Unable to parse stat output for {path}: {out!r}")
        return int(parts[0]), int(parts[1])

    def assert_stat_uid_gid(self, client, path, expected_uid, expected_gid):
        uid, gid = self.get_stat_uid_gid(client, path)
        if uid != expected_uid or gid != expected_gid:
            raise CommandFailed(
                f"Ownership mismatch for {path}: expected {expected_uid}:{expected_gid}, "
                f"got {uid}:{gid}"
            )

    def assert_dmesg_clean(self, client, patterns=None):
        patterns = patterns or DMESG_FAIL_PATTERNS
        out, _ = client.exec_command(sudo=True, cmd="dmesg | tail -200", check_ec=False)
        for pattern in patterns:
            if pattern in (out or ""):
                raise CommandFailed(
                    f"dmesg contains unexpected pattern '{pattern}' after idmap operation"
                )

    def exec_cmd(self, client, cmd, expect_fail=False):
        out, err, exit_code, _duration = client.exec_command(
            sudo=True, cmd=cmd, check_ec=False, verbose=True
        )
        if expect_fail and exit_code == 0:
            raise CommandFailed(f"Command expected to fail but succeeded: {cmd}")
        if not expect_fail and exit_code != 0:
            raise CommandFailed(
                f"Command failed (exit {exit_code}): {cmd}\nstdout: {out}\nstderr: {err}"
            )
        return out, err, exit_code

    def unshare_run(
        self,
        client,
        shell_cmd,
        inner_uid=None,
        outer_uid=None,
        inner_gid=None,
        outer_gid=None,
        map_root=False,
        run_as_inner=False,
        expect_fail=False,
    ):
        """
        Run a command inside a user namespace.

        util-linux 2.39+ expects --map-users inner:outer:count (not --map-user).
        Use setpriv for numeric UID/GID drops; runuser requires local passwd/group entries.
        """
        parts = ["unshare", "--user"]
        if map_root:
            parts.append("--map-root-user")
        elif inner_uid is not None and outer_uid is not None:
            gid_inner = inner_gid if inner_gid is not None else inner_uid
            gid_outer = outer_gid if outer_gid is not None else outer_uid
            parts.append(f"--map-users {inner_uid}:{outer_uid}:1")
            parts.append(f"--map-groups {gid_inner}:{gid_outer}:1")

        if run_as_inner and inner_uid is not None:
            gid = inner_gid if inner_gid is not None else inner_uid
            shell_cmd = (
                f"setpriv --reuid {inner_uid} --regid {gid} --clear-groups "
                f"sh -c {repr(shell_cmd)}"
            )

        parts.append(f"sh -c {repr(shell_cmd)}")
        return self.exec_cmd(client, " ".join(parts), expect_fail=expect_fail)

    def install_recursive_tools(self, client):
        client.exec_command(
            sudo=True,
            cmd="dnf install -y rsync tar 2>/dev/null || yum install -y rsync tar",
            check_ec=False,
        )

    def capture_failure_artifacts(self, client):
        for cmd in ("dmesg | tail -100", "ceph health detail", "ceph fs status"):
            out, _ = client.exec_command(sudo=True, cmd=cmd, check_ec=False)
            log.error("%s:\n%s", cmd, out)


def init_idmap_test(ceph_cluster, kw):
    """Initialize helpers used by idmap test modules."""
    config = kw.get("config") or {}
    test_data = kw.get("test_data")
    build = config.get("build", config.get("rhbuild"))
    clients = get_configured_clients(ceph_cluster, test_data=test_data, config=config)
    helper = IdmapTestHelper(ceph_cluster, test_data=test_data)
    return config, test_data, build, clients, helper


def run_idmap_test(ceph_cluster, kw, test_fn):
    """Run a test function with standard error handling."""
    config = kw.get("config") or {}
    test_data = kw.get("test_data")
    clients = get_configured_clients(ceph_cluster, test_data=test_data, config=config)
    client = clients[0] if clients else None
    try:
        return test_fn(ceph_cluster, kw)
    except Exception as exc:
        log.error(exc)
        log.error(traceback.format_exc())
        if client:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(client)
        return 1
