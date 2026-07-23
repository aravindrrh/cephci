"""
Suite stage: download + deploy IBM Spectrum Scale cluster only.

Uses mgr nodes as Scale members; installer runs spectrumscale.
"""

from tests.nfs.lib.scale_deploy import deploy_spectrum_scale, resolve_scale_roles
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    add_etc_host_entries,
    ensure_rpcbind_running,
    install_deploy_prereq_packages,
    setup_passwordless_ssh,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Deploy Spectrum Scale (no Ganesha)."""
    config = dict(kw.get("config") or {})
    if should_skip_deployment(config) or config.get("skip_scale"):
        log.info("Scale deploy stage skipped")
        return 0

    try:
        roles = resolve_scale_roles(ceph_cluster)
        nodes = ceph_cluster.get_nodes()
        cloud_type = str(config.get("cloud-type", "")).lower()
        is_baremetal = "baremetal" in cloud_type or any(
            getattr(getattr(n, "vm_node", None), "node_type", "") == "baremetal"
            for n in nodes
        )
        if not is_baremetal:
            add_etc_host_entries(nodes)
            setup_passwordless_ssh(nodes)
        install_deploy_prereq_packages(roles["scale_nodes"])
        ensure_rpcbind_running(roles["scale_nodes"])

        log.info(
            "Scale deploy: installer=%s mgr=%s",
            roles["installer"].hostname,
            [n.hostname for n in roles["scale_nodes"]],
        )
        deploy_spectrum_scale(ceph_cluster, config)
        return 0
    except Exception as e:
        log.error("Scale deploy failed: %s", e)
        return 1
