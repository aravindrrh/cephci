"""
gRPC admin performance tests for NFS-Ganesha on cephadm NFS.

Measures latency of implemented nfsService RPCs under light client load.
ExportMgr perf (83632487) skips until upstream proto is available.
"""

import statistics
import time

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.grpc_admin.grpc_client import (
    DEFAULT_GRPC_PORT,
    check_blocked_operation,
    ensure_subvolume_group,
    get_client_ids,
    get_grace_period,
    install_grpcurl,
    list_grpc_services,
    open_grpc_firewall,
    prepare_cluster_nodes,
    resolve_grpc_target,
)
from tests.nfs.nfs_operations import cleanup_cluster, setup_nfs_cluster
from utility.log import Log

log = Log(__name__)


def _timed_call(fn, *args, **kwargs):
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, result


def _percentile(samples, pct):
    if not samples:
        return 0.0
    ordered = sorted(samples)
    idx = int(len(ordered) * pct / 100)
    idx = min(idx, len(ordered) - 1)
    return ordered[idx]


def _mount_extra_clients(clients, nfs_server, nfs_mount, nfs_export, nfs_version, nfs_port):
    """Mount NFS on additional clients to increase ShowClients cardinality."""
    for client in clients[1:]:
        client.exec_command(sudo=True, cmd=f"mkdir -p {nfs_mount}", check_ec=False)
        mount_cmd = (
            f"mount -t nfs -o vers={nfs_version},proto=tcp,port={nfs_port} "
            f"{nfs_server}:{nfs_export} {nfs_mount}"
        )
        client.exec_command(sudo=True, cmd=mount_cmd, check_ec=False)
        client.exec_command(
            sudo=True,
            cmd=f"touch {nfs_mount}/perf_client_{client.hostname}",
            check_ec=False,
        )


def run(ceph_cluster, **kw):
    """
    Performance operations (``operation`` config key):

        admin_perf_under_load — list_services / GetGracePeriod latency
        stats_collection_vs_io — poll GetClientIds while light I/O runs
        show_clients_at_scale — GetClientIds latency with multiple mounts
        export_ops_perf — blocked until ExportMgr proto
    """
    config = kw.get("config", {})
    operation = config.get("operation")
    if not operation:
        raise ConfigError("'operation' is required in config")

    if operation == "export_ops_perf":
        skip_rc = check_blocked_operation("add_export", config)
        if skip_rc is not None:
            return skip_rc

    nfs_name = config.get("nfs_name", "cephfs-nfs")
    nfs_export = config.get("nfs_export", "/export")
    nfs_mount = config.get("nfs_mount", "/mnt/nfs")
    nfs_version = config.get("nfs_version", 4.1)
    nfs_port = int(config.get("port", 2049))
    fs_name = config.get("fs_name", "cephfs")
    fs = config.get("fs", "cephfs")
    grpc_port = int(config.get("grpc_port", DEFAULT_GRPC_PORT))
    samples = int(config.get("samples", 20))
    p99_max_ms = float(config.get("p99_max_ms", 5000))

    clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip = prepare_cluster_nodes(
        ceph_cluster, config
    )
    target = resolve_grpc_target(config, nfs_ip)

    try:
        install_grpcurl(client)
        open_grpc_firewall(nfs_node, grpc_port)
        ensure_subvolume_group(client, fs_name)
        setup_nfs_cluster(
            clients=clients[:1],
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

        if operation == "admin_perf_under_load":
            latencies = []
            for _ in range(samples):
                ms, (ok, _) = _timed_call(
                    list_grpc_services, client, target, True, None
                )
                if ok:
                    latencies.append(ms)
                ms, (ok, _, _) = _timed_call(
                    get_grace_period, client, target, True, None
                )
                if ok:
                    latencies.append(ms)
            if len(latencies) < samples:
                raise OperationFailedError("Insufficient successful RPC samples")
            p50 = statistics.median(latencies)
            p99 = _percentile(latencies, 99)
            log.info("Admin perf: p50=%.1fms p99=%.1fms (n=%s)", p50, p99, len(latencies))
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"p99 latency {p99:.1f}ms exceeds threshold {p99_max_ms}ms"
                )

        elif operation == "stats_collection_vs_io":
            _mount_extra_clients(
                clients, nfs_server, nfs_mount, nfs_export, nfs_version, nfs_port
            )
            latencies = []
            for i in range(samples):
                client.exec_command(
                    sudo=True,
                    cmd=f"dd if=/dev/zero of={nfs_mount}/perf_io_{i} bs=4k count=16 "
                    "oflag=dsync 2>/dev/null",
                    check_ec=False,
                )
                ms, (ok, _, _) = _timed_call(
                    get_client_ids, client, target, True, None
                )
                if ok:
                    latencies.append(ms)
            if not latencies:
                raise OperationFailedError("No successful GetClientIds during I/O")
            p99 = _percentile(latencies, 99)
            log.info("Stats vs I/O: p99 GetClientIds=%.1fms", p99)
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"GetClientIds p99 {p99:.1f}ms too high during I/O"
                )

        elif operation == "show_clients_at_scale":
            num_clients = min(len(clients), int(config.get("clients", 3)))
            _mount_extra_clients(
                clients[:num_clients],
                nfs_server,
                nfs_mount,
                nfs_export,
                nfs_version,
                nfs_port,
            )
            latencies = []
            for _ in range(samples):
                ms, (ok, ids, _) = _timed_call(
                    get_client_ids, client, target, True, None
                )
                if ok:
                    latencies.append(ms)
                    log.info("GetClientIds returned %s ids in %.1fms", len(ids), ms)
            if not latencies:
                raise OperationFailedError("GetClientIds failed at scale")
            p99 = _percentile(latencies, 99)
            if p99 > p99_max_ms:
                raise OperationFailedError(
                    f"ShowClients-scale p99 {p99:.1f}ms exceeds {p99_max_ms}ms"
                )

        elif operation == "export_ops_perf":
            raise ConfigError("export_ops_perf should have been skipped above")

        else:
            raise ConfigError(f"Unknown perf operation: {operation}")

        return 0

    except (ConfigError, OperationFailedError) as exc:
        log.error("Perf test failed: %s", exc)
        return 1
    except Exception as exc:
        log.error("Perf test failed: %s", exc)
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
