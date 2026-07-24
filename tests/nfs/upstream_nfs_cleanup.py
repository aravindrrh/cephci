"""
NFS-Ganesha teardown stage for upstream GPFS test suites.

Run this module as the **last** test in a suite. By default it removes
NFS-Ganesha only and leaves the Spectrum Scale cluster in place so the next
run can reuse Scale when VERSION_TO_USE is unchanged.

Set ``uninstall_scale: true`` (or ``force_scale_uninstall: true``) in suite
config to also tear down Scale. Mid-suite modules should keep using
mount-only suite cleanup.
"""

from tests.nfs.lib.upstream_gpfs_nfs_setup import uninstall_gpfs_scale
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Uninstall NFS-Ganesha (Scale left intact unless uninstall_scale is set)."""
    config = dict(kw.get("config") or {})

    try:
        log.info(
            "\n"
            + "=" * 70
            + "\n"
            + "  UPSTREAM NFS SUITE — NFS-Ganesha cleanup\n"
            + "=" * 70
        )
        uninstall_gpfs_scale(ceph_cluster, config)
        log.info("Upstream NFS suite cleanup completed successfully")
        return 0
    except Exception as e:
        log.error("Upstream NFS suite cleanup failed: %s", e)
        return 1
