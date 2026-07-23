"""
Suite stage: build/install NFS-Ganesha from source and create Scale export.

Runs on the node with role ``nfs``. Repo/branch via config:

  gerrit_host / gerrit_project / gerrit_refspec
  or ganesha_repo / ganesha_branch

Defaults: github.com / nfs-ganesha/nfs-ganesha / refs/heads/next
"""

from tests.nfs.lib.nfs_ganesha_deploy import deploy_ganesha_stack, resolve_ganesha_node
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    ensure_rpcbind_running,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Build/install Ganesha and create NFS export."""
    config = dict(kw.get("config") or {})
    if should_skip_deployment(config):
        log.info("Ganesha deploy stage skipped (skip_deployment)")
        return 0
    if config.get("skip_ganesha") and config.get("skip_export"):
        log.info("Ganesha deploy stage skipped (skip_ganesha and skip_export)")
        return 0

    try:
        ganesha = resolve_ganesha_node(ceph_cluster)
        ensure_rpcbind_running([ganesha])
        log.info(
            "Ganesha deploy on %s (project=%s ref=%s)",
            ganesha.hostname,
            config.get("gerrit_project") or config.get("ganesha_repo") or "default",
            config.get("ganesha_branch")
            or config.get("gerrit_refspec")
            or "refs/heads/next",
        )
        deploy_ganesha_stack(ceph_cluster, config)
        return 0
    except Exception as e:
        log.error("Ganesha deploy failed: %s", e)
        return 1
