"""
Helpers for gRPC-enabled cephadm NFS deployment in CephCI suites.

Set ``nfs_container_image`` in suite test config to override the NFS daemon
container before ``setup_nfs_cluster()`` runs.
"""

from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    install_grpcurl,
    list_grpc_services,
    verify_grpc_port_listening,
)
from utility.log import Log

log = Log(__name__)

NFS_CONTAINER_IMAGE_KEY = "mgr/cephadm/container_image_nfs"


def resolve_nfs_container_image(config):
    """Return NFS container image URL from test config, if provided."""
    return config.get("nfs_container_image") or config.get("container_image_nfs")


def apply_nfs_container_image(installer, image):
    """
    Set mgr NFS container image before deploying nfs.* services.

    Args:
        installer: Ceph installer node
        image: Full container image reference (registry/path:tag)
    """
    if not image:
        log.info("No nfs_container_image override in config; using cluster default")
        return

    cmd = (
        f"cephadm shell -- ceph config set mgr "
        f"{NFS_CONTAINER_IMAGE_KEY} {image}"
    )
    out, err = installer.exec_command(sudo=True, cmd=cmd, check_ec=False)
    if err and "error" in err.lower():
        raise OperationFailedError(
            f"Failed to set NFS container image '{image}': {err or out}"
        )
    log.info("Set %s to %s", NFS_CONTAINER_IMAGE_KEY, image)


def get_nfs_container_id(installer, nfs_name):
    """Return container_id for the first nfs.<nfs_name> daemon."""
    out = CephAdm(installer).ceph.orch.ps(
        service_name=f"nfs.{nfs_name}", format="json"
    )
    processes = __import__("json").loads(out) if out else []
    if not processes:
        return None
    return processes[0].get("container_id")


def verify_grpc_in_nfs_pod(nfs_node, client_node, nfs_ip, grpc_port=DEFAULT_GRPC_PORT):
    """
    Verify gRPC port and service discovery on a deployed NFS service.

    Returns:
        bool: True when port is listening and grpcurl lists services
    """
    if not verify_grpc_port_listening(nfs_node, grpc_port):
        return False
    install_grpcurl(client_node)
    target = f"{nfs_ip}:{grpc_port}"
    ok, services = list_grpc_services(client_node, target, plaintext=True)
    log.info("gRPC services on %s: %s", target, services)
    return ok and bool(services)
