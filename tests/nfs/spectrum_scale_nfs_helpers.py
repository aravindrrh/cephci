"""
Helpers for running NFS-Ganesha functional tests against IBM Spectrum Scale (GPFS)
instead of CephFS.

Suites pass ``nfs_backend: spectrum_scale`` in the test ``config``. Optional keys:

- ``skip_spectrum_scale_stack_setup``: if true, do not clone/run basic-storage-scale.sh
  (cluster already provisioned).
- ``spectrum_scale_export``: NFS export path / pseudo (default ``/ibm/scale_volume``).
- ``spectrum_scale_pkg_cmd``: package install command for the stack node
  (default ``dnf -y install git wget gcc nfs-utils time make``).
- ``nfs_server_role``: optional cluster role to use as Ganesha server. If unset,
  **installer** is used first (Scale script + Ganesha run there), then ``nfs`` as fallback.
"""

from time import sleep

from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount
from nfs_operations import _get_client_specific_mount_versions
from utility.log import Log

log = Log(__name__)

DEFAULT_SPECTRUM_SCALE_EXPORT = "/ibm/scale_volume"


def is_spectrum_scale_backend(config):
    if not config:
        return False
    return config.get("nfs_backend") == "spectrum_scale"


def get_nfs_server_node(ceph_cluster, config=None):
    """
    Node whose address is used for ``mount -t nfs`` against Spectrum Scale / GPFS.

    ``basic-storage-scale.sh`` runs on the **installer** node and brings up Ganesha
    there. Job YAMLs reused from Ceph often label **nfs** on a different host (e.g.
    node2); that must not be the default mount target for GPFS-backed tests.
    """
    cfg = config or {}
    explicit = cfg.get("nfs_server_role")
    if explicit:
        nodes = ceph_cluster.get_nodes(explicit)
        if nodes:
            log.info(
                "Using explicit role %s node %s as NFS server endpoint",
                explicit,
                nodes[0].hostname,
            )
            return nodes[0]
    for role in ("installer", "nfs"):
        nodes = ceph_cluster.get_nodes(role)
        if nodes:
            log.info(
                "Using %s node %s as NFS server endpoint (Spectrum Scale / GPFS)",
                role,
                nodes[0].hostname,
            )
            return nodes[0]
    raise OperationFailedError(
        "spectrum_scale_nfs: no installer or nfs node available for NFS server"
    )


def run_spectrum_scale_stack_setup(server, config=None):
    """
    Clone ci-tests and run basic-storage-scale.sh on the stack node, matching
    ``nfs_run_spectrum_scale_upstream`` / ``upstream_nfs_fio`` behaviour.
    """
    cfg = config or {}
    if cfg.get("skip_spectrum_scale_stack_setup"):
        log.info("Skipping spectrum scale stack setup (skip_spectrum_scale_stack_setup)")
        return

    pkg = cfg.get(
        "spectrum_scale_pkg_cmd",
        "dnf -y install git wget gcc nfs-utils time make",
    )
    timeout = cfg.get("stack_setup_timeout", "notimeout")
    cmds = [
        "rm -rf ci-tests/",
        pkg,
        "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
        "sh ci-tests/build_scripts/common/basic-storage-scale.sh",
    ]
    for cmd in cmds:
        exit_code = server.exec_command(
            cmd=cmd, sudo=True, long_running=True, timeout=timeout
        )
        if exit_code != 0:
            raise OperationFailedError(
                f"spectrum_scale stack setup failed (exit {exit_code}): {cmd}"
            )


def resolve_nfs_service_nodes(ceph_cluster, config=None):
    """
    Return (nfs_nodes, nfs_server_name) for tests that historically used role ``nfs``.

    For ``nfs_backend: spectrum_scale``, the mount target is the **installer** node
    (same host as ``nfs_run_spectrum_scale_upstream``), not the Ceph-style ``nfs`` role.
    """
    cfg = config or {}
    if is_spectrum_scale_backend(cfg):
        node = get_nfs_server_node(ceph_cluster, cfg)
        return [node], node.hostname
    nodes = ceph_cluster.get_nodes("nfs")
    if not nodes:
        raise OperationFailedError(
            "No cluster nodes with role nfs (required when nfs_backend is not spectrum_scale)"
        )
    return nodes, nodes[0].hostname


def mount_spectrum_scale_export_on_clients(
    ceph_cluster, clients, nfs_mount, port, version, config=None
):
    """
    Mount the same Scale export on each client (optionally different NFS versions
    per client when ``version`` is a list, same as ``setup_nfs_cluster``).
    """
    cfg = config or {}
    server = get_nfs_server_node(ceph_cluster, cfg)
    run_spectrum_scale_stack_setup(server, cfg)
    export = cfg.get("spectrum_scale_export", DEFAULT_SPECTRUM_SCALE_EXPORT)
    port = str(port)
    mount_versions = _get_client_specific_mount_versions(version, clients)
    for ver, group in mount_versions.items():
        for client in group:
            client.create_dirs(dir_path=nfs_mount, sudo=True)
            if Mount(client).nfs(
                mount=nfs_mount,
                version=ver,
                port=port,
                server=server.ip_address,
                export=export,
            ):
                raise OperationFailedError(
                    f"Failed to mount spectrum_scale export on {client.hostname}"
                )
            sleep(1)
    log.info("Spectrum Scale NFS mount succeeded on %s client(s)", len(clients))


def setup_nfs_cluster_or_scale(
    ceph_cluster,
    clients,
    nfs_server_name,
    port,
    version,
    nfs_name,
    nfs_mount,
    fs_name,
    nfs_export,
    fs,
    config=None,
    ha=False,
    vip=None,
):
    """
    Dispatch to CephFS-backed ``setup_nfs_cluster`` or Scale mounts based on config.
    """
    from nfs_operations import setup_nfs_cluster

    cfg = config or {}
    if is_spectrum_scale_backend(cfg):
        if ha:
            log.warning(
                "spectrum_scale: ha=True ignored for GPFS-backed single-export mounts"
            )
        mount_spectrum_scale_export_on_clients(
            ceph_cluster, clients, nfs_mount, port, version, cfg
        )
        return
    setup_nfs_cluster(
        clients,
        nfs_server_name,
        port,
        version,
        nfs_name,
        nfs_mount,
        fs_name,
        nfs_export,
        fs,
        ha=ha,
        vip=vip,
        ceph_cluster=ceph_cluster,
    )
