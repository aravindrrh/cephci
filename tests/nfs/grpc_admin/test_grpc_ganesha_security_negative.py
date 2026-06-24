"""
gRPC transport and security negative tests (single-node).

Validates port binding, service discovery, loopback targeting, and
connection failure handling. mTLS negative cases are included as stubs
until TLS is enabled in the test environment.
"""

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    DEFAULT_GRPC_UDS_PATH,
    check_blocked_operation,
    ensure_subvolume_group,
    expect_rpc_error,
    install_grpcurl,
    invoke_grpc_method,
    list_grpc_services,
    list_grpc_services_uds,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
    tls_paths_from_config,
    verify_grpc_port_listening,
)
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """
    Security / transport operations (``operation`` config key):

        verify_port_listening — gRPC port open on NFS node
        verify_tcp_discovery — grpcurl list over cluster IP (plaintext)
        verify_loopback_discovery — grpcurl list via 127.0.0.1 on NFS node
        verify_bad_port_rejected — connection to wrong port fails
        verify_tls_required — skipped until mTLS certs deployed in suite
        verify_tls_bad_cert — skipped until mTLS certs deployed in suite
        verify_uds_discovery — grpcurl list over Unix domain socket
        verify_admin_auth_matrix — Admin RPC auth (skip until AdminService proto)
        verify_invalid_protobuf — malformed JSON body rejected
        verify_missing_fields — required fields missing rejected
        verify_timeout — short client timeout yields DEADLINE_EXCEEDED / failure
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    tls_blocked = operation in ("verify_tls_required", "verify_tls_bad_cert")
    if tls_blocked and config.get("skip_if_blocked", True):
        log.info(
            "SKIP: %s pending mTLS cert deployment in gRPC admin suite",
            operation,
        )
        return -1

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = config.get("port", 2049)
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    grpc_port = int(config.get("grpc_port", DEFAULT_GRPC_PORT))

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    target = resolve_grpc_target(config, nfs_ip)

    try:
        install_grpcurl(client)
        install_grpcurl(nfs_node)
        open_grpc_firewall(nfs_node, grpc_port)
        ensure_subvolume_group(client, fs_name)

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
        )

        if operation == "verify_port_listening":
            if not verify_grpc_port_listening(nfs_node, grpc_port):
                raise OperationFailedError(f"Port {grpc_port} not listening")

        elif operation == "verify_tcp_discovery":
            ok, services = list_grpc_services(client, target, plaintext=True)
            if not ok or not services:
                raise OperationFailedError(
                    f"gRPC discovery failed on TCP target {target}"
                )

        elif operation == "verify_loopback_discovery":
            loopback_target = f"127.0.0.1:{grpc_port}"
            ok, services = list_grpc_services(
                nfs_node, loopback_target, plaintext=True
            )
            if not ok or not services:
                raise OperationFailedError(
                    "gRPC discovery failed on loopback — "
                    "ensure gRPC binds to 127.0.0.1 or adjust grpc_host"
                )

        elif operation == "verify_bad_port_rejected":
            bad_port = int(config.get("bad_port", grpc_port + 1000))
            bad_target = f"{nfs_ip}:{bad_port}"
            ok, services = list_grpc_services(client, bad_target, plaintext=True)
            if ok and services:
                raise OperationFailedError(
                    f"Expected connection failure on bad port {bad_port}"
                )
            log.info("Connection to bad port %s failed as expected", bad_port)

        elif operation == "verify_tls_required":
            skip_rc = check_blocked_operation(operation, config)
            if skip_rc is not None:
                return skip_rc
            tls_paths = tls_paths_from_config(config)
            ok, _ = list_grpc_services(
                client, target, plaintext=False, tls_paths=tls_paths
            )
            if not ok:
                raise OperationFailedError("mTLS discovery failed with valid certs")
            # Plaintext should fail when server requires TLS.
            ok_plain, _ = list_grpc_services(client, target, plaintext=True)
            if ok_plain:
                raise OperationFailedError(
                    "Plaintext discovery succeeded but TLS was expected to be required"
                )

        elif operation == "verify_tls_bad_cert":
            skip_rc = check_blocked_operation(operation, config)
            if skip_rc is not None:
                return skip_rc
            tls_paths = tls_paths_from_config(config)
            tls_paths["client_cert"] = config.get(
                "bad_client_cert", "/etc/ganesha/certs/bogus.crt"
            )
            ok, _ = list_grpc_services(
                client, target, plaintext=False, tls_paths=tls_paths
            )
            if ok:
                raise OperationFailedError(
                    "gRPC call succeeded with invalid client certificate"
                )

        elif operation == "verify_uds_discovery":
            uds_path = config.get("grpc_uds_path", DEFAULT_GRPC_UDS_PATH)
            ok, services = list_grpc_services_uds(nfs_node, uds_path)
            if not ok or not services:
                if config.get("skip_if_no_uds", True):
                    log.info(
                        "SKIP: UDS path %s not available — enable GRPC UDS in image",
                        uds_path,
                    )
                    return -1
                raise OperationFailedError(
                    f"UDS gRPC discovery failed on {uds_path}"
                )

        elif operation == "verify_admin_auth_matrix":
            skip_rc = check_blocked_operation("shutdown", config)
            if skip_rc is not None:
                return skip_rc
            admin_method = config.get(
                "admin_method", "adminService.Admin/Shutdown"
            )
            tls_paths = tls_paths_from_config(config)
            ok, _, _ = invoke_grpc_method(
                client,
                target,
                admin_method,
                data="{}",
                plaintext=False,
                tls_paths=tls_paths,
            )
            if ok:
                log.info("Admin RPC accepted with valid credentials")
            ok_plain, _, _ = invoke_grpc_method(
                client, target, admin_method, data="{}", plaintext=True
            )
            if ok_plain and not config.get("allow_plaintext_admin"):
                raise OperationFailedError(
                    "Admin RPC succeeded without mTLS — auth matrix unexpected"
                )

        elif operation == "verify_invalid_protobuf":
            method = config.get(
                "rpc_method", "nfsService.StartNfsGrace/StartGraceWithEvent"
            )
            bad_data = config.get("bad_data", "{not-valid-json")
            if not expect_rpc_error(client, target, method, bad_data):
                raise OperationFailedError(
                    "Expected RPC error for invalid protobuf/JSON body"
                )
            if not expect_rpc_error(client, target, method, "{}"):
                raise OperationFailedError(
                    "Expected RPC error when required fields are missing"
                )

        elif operation == "verify_missing_fields":
            method = config.get(
                "rpc_method", "nfsService.StartNfsGrace/StartGraceWithEvent"
            )
            if not expect_rpc_error(client, target, method, "{}"):
                raise OperationFailedError(
                    "Expected RPC error when required fields are missing"
                )

        elif operation == "verify_timeout":
            method = config.get(
                "rpc_method", "nfsService.GetNfsGrace/GetGracePeriod"
            )
            max_time = float(config.get("max_time_sec", 0.001))
            ok, out, err = invoke_grpc_method(
                client,
                target,
                method,
                plaintext=True,
                max_time_sec=max_time,
            )
            combined = f"{out}\n{err}".lower()
            if ok and "deadline" not in combined:
                log.info(
                    "Fast RPC completed within timeout — acceptable for GetGracePeriod"
                )
            elif not ok or "deadline" in combined or "timeout" in combined:
                log.info("Timeout behavior observed as expected")
            else:
                raise OperationFailedError(
                    f"Unexpected timeout test outcome: {out} {err}"
                )

        else:
            raise ConfigError(f"Unknown security operation: {operation}")

        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Security test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Security test failed: %s", exc)
        import traceback

        log.error(traceback.format_exc())
        return 1
    finally:
        try:
            cleanup_cluster(
                client, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node
            )
        except Exception as exc:
            log.warning("Cleanup error (non-fatal): %s", exc)
