"""
Spectrum Scale / NFS deployment stage for upstream GPFS test suites.

Run this module as the **first** test in a suite so multi-node deploy runs once.
Later modules should set ``skip_deployment: true`` in config (or SKIP_DEPLOYMENT).
"""

from cli.exceptions import ConfigError
from tests.nfs.lib.upstream_gpfs_nfs_setup import deploy_gpfs_scale, should_skip_deployment
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Deploy multi-node Spectrum Scale / NFS (suite bootstrap, no client mounts)."""
    config = dict(kw.get("config") or {})
    clients_all = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "2"))
    if no_clients > len(clients_all):
        raise ConfigError("The test requires more clients than available")
    if len(clients_all) < 2:
        raise ConfigError("Multi-node deploy requires at least two client nodes")

    if should_skip_deployment(config):
        log.info("skip_deployment set — deployment stage skipped")
        return 0

    try:
        log.info(
            "\n"
            + "=" * 70
            + "\n"
            + "  UPSTREAM NFS SUITE — Spectrum Scale / NFS deployment\n"
            + "=" * 70
        )
        deploy_gpfs_scale(ceph_cluster, config)
        log.info("Upstream NFS suite deployment completed successfully")
        return 0
    except Exception as e:
        log.error("Upstream NFS suite deployment failed: %s", e)
        return 1
