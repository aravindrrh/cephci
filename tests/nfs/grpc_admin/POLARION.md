# Polarion Traceability — gRPC D-Bus Migration (CephFS only)

**FSAL scope:** CephFS / cephadm NFS in `red-hat-storage/cephci` only.

GPFS / IBM Storage Scale cases are **not** tracked here. They belong on the
`nfs_upstream` fork (Polarion IDs CEPH-83632481, CEPH-83632495, CEPH-83632498).

Results post to Polarion when suite tests include `polarion-id: CEPH-XXXXX`.

**Suites**

| Suite | Purpose |
|-------|---------|
| `tier1-nfs-ganesha-grpc-admin.yaml` | Admin transport, parity, security, negative (single-node) |
| `tier0-nfs-ganesha-grpc-regression.yaml` | Cthon, pynfs, locks, POSIX, tier gates, scale |
| `tier1-nfs-ganesha-grpc-perf.yaml` | Perf, build, dual-stack, proto compile |
| `tier1-nfs-ganesha-grpc.yaml` | IBM Cloud basic nfsService smoke (CEPH-83630609–616) |

**Status legend:** `Automated` · `Partial` (skip until image/proto) · `Skipped` (blocked proto) · `Parked` (HA)

| Polarion ID | Title | Status | Suite / module |
|-------------|-------|--------|----------------|
| CEPH-83632459 | Verify gRPC Local Communication via Unix Domain Socket | Partial | `verify_uds_discovery` — skips if UDS path absent |
| CEPH-83632460 | Verify gRPC Local Communication via TCP Loopback | Partial | `verify_tcp_discovery`, `verify_loopback_discovery` |
| CEPH-83632461 | Verify ExportMgr Service Operations | Skipped | `add_export` (blocked) |
| CEPH-83632467 | Verify gRPC Communication Between Cluster Nodes | Parked | HA suite (future) |
| CEPH-83632468 | Verify Grace Period Coordination via gRPC | Partial | `get_grace_period`, `start_grace_event_0` |
| CEPH-83632469 | Verify Export Coordination in Multi-Node Cluster | Parked | HA suite (future) |
| CEPH-83632470 | Verify Protocol Buffer Schema Compatibility | Partial | `verify_proto_compile` — skips if protoc/proto absent |
| CEPH-83632471 | Verify gRPC API Contract Tests | Partial | `list_services` |
| CEPH-83632472 | Verify mTLS Authentication Between Nodes | Skipped | `verify_tls_required` (blocked) |
| CEPH-83632473 | Verify Admin RPC Authorization | Skipped | `verify_admin_auth_matrix` (blocked until AdminService) |
| CEPH-83632474 | Verify TLS Encryption for Inter-Node Communication | Parked | HA + packet capture |
| CEPH-83632475 | Verify ClientMgr API Parity | Partial | `show_clients_and_sessions`, parity capture |
| CEPH-83632476 | Verify ExportMgr API Parity | Skipped | `show_exports` (blocked), parity capture |
| CEPH-83632477 | Verify Admin API Parity | Skipped | `compare_admin_parity` (blocked) |
| CEPH-83632478 | Verify Log Management API Parity | Skipped | `compare_log_parity` (blocked) |
| CEPH-83632479 | Verify Stats API Parity | Skipped | `compare_stats_parity` (blocked) |
| CEPH-83632480 | Verify CephFS HA Failover with gRPC | Parked | `baremetal/tier1-nfs-ganesha-grpc-ha` |
| CEPH-83632482 | Verify Export Edit During Active I/O | Parked | tier1 HA export tests + gRPC |
| CEPH-83632483 | Verify Network Partition Handling | Parked | HA suite |
| CEPH-83632484 | Verify gRPC Admin Performance Under Load | Automated | `test_grpc_ganesha_perf.py` — `admin_perf_under_load` |
| CEPH-83632485 | Verify Stats Collection Performance | Automated | `stats_collection_vs_io` |
| CEPH-83632486 | Verify ShowClients Performance with Many Clients | Automated | `show_clients_at_scale` |
| CEPH-83632487 | Verify Export Operations Performance | Skipped | `export_ops_perf` (blocked) |
| CEPH-83632488 | Verify Cthon04 Test Suite with gRPC | Automated | `test_cthon.py` — regression suite |
| CEPH-83632489 | Verify Pynfs Test Suite with gRPC | Automated | `nfs_verify_pynfs.py` — regression suite |
| CEPH-83632490 | Verify Lock Tests with gRPC | Automated | `nfs_non_overlapping_locks.py` — regression suite |
| CEPH-83632491 | Verify POSIX Compliance with gRPC | Automated | `test_grpc_pjdfstest.py` — regression suite |
| CEPH-83632492 | Verify gRPC Build Configuration | Automated | `verify_grpc_build` — perf suite |
| CEPH-83632493 | Verify Dual-Stack Build (D-Bus + gRPC) | Automated | `verify_dual_stack` — perf suite |
| CEPH-83632494 | Verify CephFS Bootstrap Script with gRPC | Automated | `verify_grpc_bootstrap` — regression suite |
| CEPH-83632496 | Verify CephCI Tier0 Suite with gRPC | Automated | `tier0_smoke` regression gate |
| CEPH-83632497 | Verify CephCI Tier1 Suite with gRPC | Automated | `tier1_smoke` regression gate |
| CEPH-83632499 | Verify CephCI Scale Tests with gRPC | Automated | `nfs_verify_scale.py` — regression suite |
| CEPH-83632500 | Verify gRPC Connection Failure Handling | Partial | `verify_bad_port_rejected` |
| CEPH-83632501 | Verify Invalid RPC Request Handling | Automated | `verify_invalid_protobuf` |
| CEPH-83632502 | Verify Resource Exhaustion Handling | Automated | `verify_resource_exhaustion` |
| CEPH-83632503 | Verify Timeout Handling | Automated | `verify_timeout` |

## GPFS / IBM Storage Scale (out of scope for this repo)

| Polarion ID | Title | Track on |
|-------------|-------|----------|
| CEPH-83632481 | Verify GPFS Multi-Node HA with gRPC | `nfs_upstream` fork |
| CEPH-83632495 | Verify GPFS Bootstrap Script with gRPC | `nfs_upstream` fork |
| CEPH-83632498 | Verify GPFS Upstream Suite with gRPC | `nfs_upstream` fork |

## gRPC-enabled NFS container image

Set before NFS cluster create in any gRPC suite:

```yaml
config:
  nfs_container_image: quay.io/rhceph/nfs-ganesha-grpc:<tag>
```

Or apply via `grpc_admin.test_grpc_ganesha_deploy.py` operation `set_nfs_image`.
This sets `mgr/cephadm/container_image_nfs` at bootstrap time (see `grpc_deploy.py`).

Jenkins: pass image via suite config override or `--docker-image` / `--docker-tag`
when your pipeline supports injecting `nfs_container_image` into test config.
