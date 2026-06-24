# Polarion Traceability — NFS-Ganesha gRPC API (CephFS)

**FSAL scope:** CephFS / cephadm NFS in this repo only.

**Suite focus:** gRPC interface validation only. NFS protocol regression
(cthon/pynfs/locks/POSIX/scale) and D-Bus parity are **out of scope** for the
gRPC suites until cephadm consumes gRPC (later release).

GPFS / IBM Storage Scale cases belong on the `nfs_upstream` fork
(CEPH-83632481, CEPH-83632495, CEPH-83632498).

**Suites**

| Suite | Purpose |
|-------|---------|
| `tier1-nfs-ganesha-grpc.yaml` | Core nfsService smoke (CEPH-83630609–616) |
| `tier1-nfs-ganesha-grpc-admin.yaml` | Full gRPC API + transport/security |
| `tier1-nfs-ganesha-grpc-perf.yaml` | RPC latency + build/proto compile |

**Status legend:** `Automated` · `Partial` · `Deferred` (later release / no proto) · `Parked` (HA)

| Polarion ID | Title | Status | Suite / operation |
|-------------|-------|--------|-------------------|
| CEPH-83632459 | gRPC via Unix Domain Socket | Partial | `verify_uds_discovery` |
| CEPH-83632460 | gRPC via TCP / loopback | Automated | `verify_tcp_discovery`, `verify_loopback_discovery` |
| CEPH-83632461 | ExportMgr / ExportService ops | Automated | `show_exports`, `display_export`, `add_export`, `remove_export` |
| CEPH-83632467 | Inter-node cluster communication | Parked | HA suite (future) |
| CEPH-83632468 | Grace period via gRPC | Automated | `get_grace_period`, `start_grace_event_0` |
| CEPH-83632469 | Export coordination multi-node | Parked | HA suite (future) |
| CEPH-83632470 | Protocol buffer schema compatibility | Partial | `verify_proto_compile` |
| CEPH-83632471 | gRPC API contract / service list | Automated | `list_services` |
| CEPH-83632472 | mTLS authentication | Automated | `verify_tls_required` |
| CEPH-83632473 | Admin RPC authorization | Automated | `verify_admin_auth_matrix` |
| CEPH-83632474 | TLS inter-node encryption | Parked | HA + packet capture |
| CEPH-83632475 | ClientMgr APIs | Automated | `show_clients_mgr`, `add_client`, `remove_client`, `disconnect_nfsv41_client` |
| CEPH-83632476 | ExportService ShowExports | Automated | `show_exports` |
| CEPH-83632477 | Admin APIs (`nfsAdmin`) | Automated | `reread_config`, `trim_status`, `show_idmapper`, … |
| CEPH-83632478 | Log management API | Deferred | No Log gRPC proto upstream |
| CEPH-83632479 | Stats APIs | Automated | `export_stats_smoke`, `client_stats_smoke` |
| CEPH-83632480 | CephFS HA failover with gRPC | Parked | Future HA suite |
| CEPH-83632482 | Export edit during active I/O | Parked | Future HA / UpdateExport stress |
| CEPH-83632483 | Network partition handling | Parked | HA suite |
| CEPH-83632484 | Admin performance under load | Automated | `admin_perf_under_load` |
| CEPH-83632485 | Stats collection performance | Automated | `stats_collection_vs_io` |
| CEPH-83632486 | ShowClients at scale | Automated | `show_clients_at_scale` |
| CEPH-83632487 | Export operations performance | Automated | `export_ops_perf` |
| CEPH-83632488 | Cthon04 with gRPC image | Deferred | Covered by existing NFS suites |
| CEPH-83632489 | Pynfs with gRPC image | Deferred | Covered by existing NFS suites |
| CEPH-83632490 | Lock tests with gRPC image | Deferred | Covered by existing NFS suites |
| CEPH-83632491 | POSIX with gRPC image | Deferred | Covered by existing NFS suites |
| CEPH-83632492 | gRPC build configuration | Automated | `verify_grpc_build` |
| CEPH-83632493 | Dual-stack D-Bus + gRPC | Deferred | Not a gRPC-only suite goal |
| CEPH-83632494 | CephFS bootstrap with gRPC | Deferred | Image override in suite setup |
| CEPH-83632496 | Tier0 suite with gRPC | Deferred | Existing tier0 NFS suites |
| CEPH-83632497 | Tier1 suite with gRPC | Deferred | Existing tier1 NFS suites |
| CEPH-83632499 | Scale tests with gRPC | Deferred | Existing NFS scale suites |
| CEPH-83632500 | Connection failure handling | Automated | `verify_bad_port_rejected` |
| CEPH-83632501 | Invalid RPC request handling | Automated | `verify_invalid_protobuf` |
| CEPH-83632502 | Resource exhaustion handling | Automated | `verify_resource_exhaustion` |
| CEPH-83632503 | Timeout handling | Automated | `verify_timeout` |

## GPFS / IBM Storage Scale (out of scope)

| Polarion ID | Title | Track on |
|-------------|-------|----------|
| CEPH-83632481 | GPFS multi-node HA with gRPC | `nfs_upstream` |
| CEPH-83632495 | GPFS bootstrap with gRPC | `nfs_upstream` |
| CEPH-83632498 | GPFS upstream suite with gRPC | `nfs_upstream` |

## gRPC-enabled NFS container image

```yaml
config:
  nfs_container_image: quay.io/rhceph/nfs-ganesha-grpc:<tag>
```

Or apply via `grpc_admin.test_grpc_ganesha_deploy.py` operation `set_nfs_image`.
