"""
Spectrum Scale / NFS teardown stage for upstream GPFS test suites.

Run this module as the **last** test in a suite so static nodes return to a
clean state (no Ganesha RPMs, no Scale cluster, no deploy clones) for the
next run. Mid-suite modules should keep using mount-only suite cleanup.
"""

from tests.nfs.lib.upstream_gpfs_nfs_setup import uninstall_gpfs_scale
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Uninstall NFS-Ganesha RPMs, remove clones, and uninstall IBM Scale."""
    config = dict(kw.get("config") or {})

    try:
        log.info(
            "\n"
            + "=" * 70
            + "\n"
            + "  UPSTREAM NFS SUITE — Spectrum Scale / NFS cleanup\n"
            + "=" * 70
        )
        uninstall_gpfs_scale(ceph_cluster, config)
        log.info("Upstream NFS suite cleanup completed successfully")
        return 0
    except Exception as e:
        log.error("Upstream NFS suite cleanup failed: %s", e)
        return 1
