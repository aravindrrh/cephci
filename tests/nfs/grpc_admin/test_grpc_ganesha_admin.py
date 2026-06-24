"""
NFS-Ganesha gRPC admin tests (D-Bus replacement management plane).

Uses the ``operation`` config key to select a scenario. Implemented operations
map to nfsService.proto RPCs available today; ExportMgr/Admin/Log operations
are stubbed and skipped until upstream proto merges.
"""

from time import sleep

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    check_blocked_operation,
    ensure_subvolume_group,
    get_client_ids,
    get_grace_period,
    get_session_ids,
    install_grpcurl,
    list_grpc_services,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    start_grace_with_event,
    tls_paths_from_config,
    verify_expected_services,
    verify_grpc_port_listening,
)
from tests.nfs.nfs_operations import cleanup_cluster, nfs_log_parser, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)

# Maps suite operation names to grace event IDs (StartNfsGrace).
GRACE_EVENT_OPS = {
    "start_grace_event_0": 0,
    "start_grace_event_2": 2,
    "start_grace_event_4": 4,
    "start_grace_event_5": 5,
    "grace_event_0": 0,
    "grace_event_2": 2,
    "grace_event_4": 4,
    "grace_event_5": 5,
}


def _use_tls(config):
    return bool(config.get("use_tls", False))


def _setup_additional_mounts(clients, nfs_server, nfs_export, nfs_mount, version, port):
    """Mount export on clients[1:] and return (client, mount_point) pairs."""
    mount_points = []
    for i, client in enumerate(clients[1:], start=1):
        mount_point = f"{nfs_mount}_{i}"
        client.create_dirs(dir_path=mount_point, sudo=True)
        if Mount(client).nfs(
            mount=mount_point,
            version=version,
            port=port,
            server=nfs_server,
            export=nfs_export,
        ):
            log.error("Failed to mount NFS on %s", client.hostname)
            continue
        client.exec_command(sudo=True, cmd=f"touch {mount_point}/grpc_admin_test_{i}")
        mount_points.append((client, mount_point))
        sleep(2)
    return mount_points


def _cleanup_additional_mounts(mount_points):
    for client, mount_point in mount_points:
        try:
            client.exec_command(
                sudo=True, cmd=f"rm -rf {mount_point}/*", check_ec=False
            )
            Unmount(client).unmount(mount_point)
            client.exec_command(
                sudo=True, cmd=f"rm -rf {mount_point}", check_ec=False
            )
        except Exception as exc:
            log.warning("Cleanup error on %s: %s", client.hostname, exc)


def run(ceph_cluster, **kw):
    """
    Run a gRPC admin test scenario.

    Supported operations (implemented):
        verify_port, list_services, get_grace_period,
        start_grace_event_0|2|4|5, get_client_ids, get_session_ids,
        show_clients_empty, show_clients_active, show_clients_and_sessions

    Blocked (skipped unless skip_if_blocked: false):
        add_export, remove_export, update_export, show_exports,
        display_export, shutdown, reload, grace_client_ip,
        set_log_level, get_log_level, remove_client
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    skip_rc = check_blocked_operation(operation, config)
    if skip_rc is not None:
        return skip_rc

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    subvolume_group = config.get("subvolume_group", "ganeshagroup")
    setup_cluster = config.get("setup_nfs_cluster", True)

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    target = resolve_grpc_target(config, nfs_ip)
    plaintext = not _use_tls(config)
    tls_paths = None if plaintext else tls_paths_from_config(config)
    additional_mounts = []

    log.info("Running gRPC admin operation: %s", operation)
    log.info("Target: %s (plaintext=%s)", target, plaintext)

    try:
        for c in clients:
            install_grpcurl(c)
        open_grpc_firewall(nfs_node, int(config.get("grpc_port", DEFAULT_GRPC_PORT)))
        ensure_subvolume_group(client, fs_name, subvolume_group)

        if setup_cluster:
            setup_nfs_cluster(
                clients=[client],
                nfs_server=nfs_server,
                port=nfs_port,
                version=nfs_version,
                nfs_name=nfs_name,
                nfs_mount=nfs_mount,
                fs_name=fs_name,
                export=nfs_export,
                fs=fs,
                ceph_cluster=ceph_cluster,
                enable_rdma=config.get("enable_rdma", False),
                rdma_port=config.get("rdma_port"),
            )

        export_path = f"{nfs_export}_0"

        if operation == "verify_port":
            if not verify_grpc_port_listening(nfs_node):
                raise OperationFailedError("gRPC port is not listening")

        elif operation == "list_services":
            ok, services = list_grpc_services(
                client, target, plaintext=plaintext, tls_paths=tls_paths
            )
            if not ok or not verify_expected_services(services):
                raise OperationFailedError("Expected gRPC services not discovered")

        elif operation == "get_grace_period":
            ok, ingrace, _ = get_grace_period(
                client, target, plaintext=plaintext, tls_paths=tls_paths
            )
            if not ok:
                raise OperationFailedError("GetGracePeriod RPC failed")
            log.info("Grace period state ingrace=%s", ingrace)

        elif operation in GRACE_EVENT_OPS:
            event_id = GRACE_EVENT_OPS[operation]
            ok, response = start_grace_with_event(
                client,
                target,
                event_id,
                node_id=int(config.get("node_id", 1)),
                ip_addr=config.get("grace_ip", nfs_ip),
                plaintext=plaintext,
                tls_paths=tls_paths,
            )
            if not ok:
                raise OperationFailedError(
                    f"StartGraceWithEvent event {event_id} failed: {response}"
                )
            sleep(int(config.get("post_grace_sleep", 5)))

        elif operation == "show_clients_empty":
            # Cluster up but no extra client mounts — expect zero or minimal clients.
            ok, client_ids, _ = get_client_ids(
                client, target, plaintext=plaintext, tls_paths=tls_paths
            )
            if not ok:
                raise OperationFailedError("GetClientIds RPC failed")
            log.info("Client IDs with primary mount only: %s", client_ids)

        elif operation in ("show_clients_active", "show_clients_and_sessions"):
            if len(clients) > 1:
                additional_mounts = _setup_additional_mounts(
                    clients,
                    nfs_server,
                    export_path,
                    nfs_mount,
                    nfs_version,
                    nfs_port,
                )
            sleep(int(config.get("client_settle_sleep", 10)))

            ok, client_ids, _ = get_client_ids(
                client, target, plaintext=plaintext, tls_paths=tls_paths
            )
            if not ok:
                raise OperationFailedError("GetClientIds RPC failed")
            log.info("Active client IDs: %s", client_ids)

            if operation == "show_clients_and_sessions":
                ok, session_ids, _ = get_session_ids(
                    client, target, plaintext=plaintext, tls_paths=tls_paths
                )
                if not ok:
                    raise OperationFailedError("GetSessionIds RPC failed")
                log.info("Active session IDs: %s", session_ids)

        elif operation == "get_client_ids":
            ok, client_ids, _ = get_client_ids(
                client, target, plaintext=plaintext, tls_paths=tls_paths
            )
            if not ok:
                raise OperationFailedError("GetClientIds RPC failed")
            log.info("Client IDs: %s", client_ids)

        elif operation == "get_session_ids":
            ok, session_ids, _ = get_session_ids(
                client, target, plaintext=plaintext, tls_paths=tls_paths
            )
            if not ok:
                raise OperationFailedError("GetSessionIds RPC failed")
            log.info("Session IDs: %s", session_ids)

        else:
            raise ConfigError(f"Unknown operation: {operation}")

        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("gRPC admin test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("gRPC admin test failed with unexpected error: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            _cleanup_additional_mounts(additional_mounts)
            nfs_log_parser(client=client, nfs_node=nfs_nodes, nfs_name=nfs_name)
            if setup_cluster:
                cleanup_cluster(
                    client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node
                )
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
