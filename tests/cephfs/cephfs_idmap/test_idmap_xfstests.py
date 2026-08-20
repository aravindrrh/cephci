"""
TC-S13: xfstests idmapped group on kernel-mounted CephFS.
"""

import traceback

from tests.cephfs.cephfs_idmap.lib.cephfs_idmap_lib import (
    FS_NAME,
    IdmapTestHelper,
    init_idmap_test,
)
from tests.cephfs.lib.xfs_lib.xfs_utils import XfsTestSetup
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """TC-S13 — run upstream xfstests idmapped group on CephFS."""
    plain_mount = idmap_mount = None
    xfs_test = None
    mount_info = None
    try:
        _config, _test_data, build, clients, helper = init_idmap_test(ceph_cluster, kw)
        helper.prepare_clients(clients, build)
        client = clients[0]

        xfs_test = XfsTestSetup(ceph_cluster, client)
        if xfs_test.setup_environment():
            log.error("Failed to set up xfstests environment")
            return 1
        if xfs_test.clone_and_build_xfstests():
            log.error("Failed to clone and build xfstests")
            return 1

        suffix = helper.random_suffix()
        plain_mount = f"/mnt/cephfs_idmap_plain_{suffix}"
        idmap_mount = f"/mnt/cephfs_idmap_view_{suffix}"
        test_dev = f"idmap_test_{suffix}"
        scratch_dev = f"idmap_scratch_{suffix}"

        helper.kernel_mount_plain(client, plain_mount)
        helper.idmap_bind_mount(client, plain_mount, idmap_mount)

        mount_info = {
            "test_mount": idmap_mount,
            "scratch_mount": plain_mount,
            "mount_type": "kernel",
            "FSTYP": "ceph",
            "fs_name": FS_NAME,
            "test_dev": test_dev,
            "scratch_dev": scratch_dev,
        }

        if xfs_test.configure_local_config(mount_info):
            log.error("Failed to configure xfstests local.config")
            return 1

        out, _err, exit_code, _duration = client.exec_command(
            sudo=True,
            cmd="cd /root/xfstests-dev && ./check -g idmapped",
            check_ec=False,
            verbose=True,
            timeout=7200,
        )
        log.info("xfstests idmapped group output:\n%s", out)

        failed_tests, _ = client.exec_command(
            sudo=True,
            cmd=r"find /root/xfstests-dev/results -name '*.out.bad' 2>/dev/null | wc -l",
            check_ec=False,
        )
        failed_count = int((failed_tests or "0").strip() or "0")
        if exit_code != 0 or failed_count > 0:
            log.error(
                "xfstests idmapped group failed (exit=%s, bad=%s)",
                exit_code,
                failed_count,
            )
            return 1

        log.info("TC-S13 xfstests idmapped group passed")
        return 0

    except Exception as exc:
        log.error("TC-S13 failed: %s", exc)
        log.error(traceback.format_exc())
        clients = ceph_cluster.get_ceph_objects("client")
        if clients:
            IdmapTestHelper(ceph_cluster).capture_failure_artifacts(clients[0])
        return 1

    finally:
        if xfs_test and mount_info:
            xfs_test.cleanup(mount_info)
        elif plain_mount and idmap_mount and clients:
            IdmapTestHelper(ceph_cluster).umount_idmap_stack(
                clients[0], idmap_mount, plain_mount
            )
