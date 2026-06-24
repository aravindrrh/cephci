# NFS-Ganesha gRPC Admin Tests

Management-plane automation for the **D-Bus → gRPC migration** in NFS-Ganesha
on **CephFS (cephadm NFS) only**.

This folder is separate from:
- IBM Cloud basic gRPC smoke — `test_nfs_grpc.py` / `tier1-nfs-ganesha-grpc.yaml`
- GPFS / IBM Storage Scale — tracked on the `nfs_upstream` fork, not here

## Scope

**FSAL:** CephFS only (`ceph nfs cluster`, cephadm NFS service).

| In scope (this folder) | Out of scope |
|------------------------|--------------|
| ExportMgr, Admin, ClientMgr, Log (single-node) | GPFS / IBM Storage Scale (`nfs_upstream`) |
| D-Bus vs gRPC parity (golden JSON) | Inter-node HA / grace inject (parked) |
| Local transport (TCP, UDS) | Network partition / VIP failover (parked) |
| mTLS / auth negative (single node) | `baremetal/tier1-nfs-ganesha-grpc-ha.yaml` (future) |
| Ceph NFS scale / tier0 / tier1 regression gates | `test_grpc_ganesha_ha_inject.py` (parked) |

## Related artifacts

- **Admin suite:** `suites/tentacle/nfs/tier1-nfs-ganesha-grpc-admin.yaml`
- **Regression suite:** `suites/tentacle/nfs/tier0-nfs-ganesha-grpc-regression.yaml`
- **Perf / build suite:** `suites/tentacle/nfs/tier1-nfs-ganesha-grpc-perf.yaml`
- **IBM Cloud smoke (unchanged):** `suites/tentacle/nfs/tier1-nfs-ganesha-grpc.yaml`
- **Proto definitions:** https://github.com/ffilz/nfs-ganesha/tree/next/src/grpc_server/proto
- **Feature doc:** gRPC Replacement for D-Bus (project doc)
- **Test plan:** NFS-Ganesha gRPC Migration — Test Strategy v1.1

## Module map

| File | Purpose | Test plan IDs |
|------|---------|---------------|
| `grpc_client.py` | grpcurl install, plaintext/TLS calls, service list | — |
| `grpc_deploy.py` | NFS container image override (`container_image_nfs`) | — |
| `test_grpc_ganesha_admin.py` | P0 admin matrix (`operation` config key) | GRPC-EXP-*, GRPC-CLT-*, GRPC-ADM-*, GRPC-LOG-* |
| `test_grpc_ganesha_parity.py` | Capture/compare D-Bus vs gRPC responses | GRPC-EXP-007/008, parity matrix |
| `test_grpc_ganesha_security_negative.py` | UDS, bad cert, invalid RPC, timeout | GRPC-LOC-*, GRPC-SEC-* |
| `test_grpc_ganesha_load_negative.py` | Concurrent grpcurl load / exhaustion | GRPC-SEC-* |
| `test_grpc_ganesha_perf.py` | Admin RPC latency under load | perf matrix |
| `test_grpc_ganesha_build.py` | USE_GRPC build, dual-stack, proto compile | build matrix |
| `test_grpc_ganesha_deploy.py` | NFS image override + bootstrap gRPC smoke | deploy gate |
| `test_grpc_pjdfstest.py` | pjdfstest POSIX smoke on NFS mount | regression |
| `test_grpc_ganesha_regression_gate.py` | Tier0/tier1 representative smoke | regression gates |
| `test_grpc_ganesha_stub.py` | Skipped Polarion placeholders (rc=-1) | Parked HA IDs only |

## CephCI module naming

Suite YAML references modules as:

```
module: grpc_admin.test_grpc_ganesha_admin.py
```

`tests/nfs` is on `sys.path`; the folder uses an underscore (`grpc_admin`) because
hyphens are not valid in Python import paths (same pattern as `acl.test_nfs_acl_functional.py`).

## gRPC configuration defaults

From the feature design doc (ganesha.conf `GRPC` stanza):

| Parameter | Default |
|-----------|---------|
| GRPC_ENABLE | true |
| GRPC_PORT | 50051 |
| Server cert | `/etc/ganesha/certs/server.crt` |
| Server key | `/etc/ganesha/certs/server.key` |
| Client cert | `/etc/ganesha/certs/client.crt` |
| Client key | `/etc/ganesha/certs/client.key` |
| CA cert | `/etc/ganesha/certs/ca.crt` |

Build flag: `cmake -DUSE_GRPC=ON`

Example grpcurl (TLS):

```
grpcurl -cacert ca.crt -cert client.crt -key client.key localhost:50051 list
```

## Proto status vs automation

### Implemented today (`nfsService.proto`)

- `GetClientId.GetClientIds`
- `GetNfsGrace.GetGracePeriod`
- `StartNfsGrace.StartGraceWithEvent`
- `GetSessionId.GetSessionIds`

### Planned (D-Bus replacement — tests stubbed until upstream merges)

| D-Bus path | Proposed gRPC service |
|------------|----------------------|
| `/org/ganesha/nfsd/ClientMgr` | ClientMgrService |
| `/org/ganesha/nfsd/ExportMgr` | ExportMgrService |
| `/org/ganesha/nfsd/admin` | AdminService, LogService |

## D-Bus to gRPC service mapping (target state)

| D-Bus path | Interfaces | gRPC service (proposed) | Primary CephCI hook |
|------------|------------|-------------------------|---------------------|
| `/org/ganesha/nfsd/ClientMgr` | clientmgr, clientstats | ClientMgrService | `test_grpc_ganesha_admin.py` |
| `/org/ganesha/nfsd/ExportMgr` | exportmgr, exportstats | ExportMgrService | `test_grpc_ganesha_admin.py` |
| `/org/ganesha/nfsd/admin` | admin, log | AdminService, LogService | `test_grpc_ganesha_admin.py` |

## Migration phases

| Phase | Build | CephCI behavior |
|-------|-------|-----------------|
| 0 – Baseline | USE_DBUS=ON | Capture D-Bus golden in `golden/` |
| 1 – Shadow | Dual stack | Parity diff after tier0; read-only gRPC |
| 2 – Dual write | Both paths | AddExport via gRPC during export tests |
| 3 – gRPC primary | USE_GRPC=ON | Full admin suite |
| 4 – D-Bus removed | gRPC only | Parity module retired |

## Implementation phases (this repo)

### M1 — Foundation

- [x] `grpc_client.py` — shared helpers (extract from `test_nfs_grpc.py`)
- [x] `test_grpc_ganesha_admin.py` — operations for **implemented** RPCs
- [x] `tier1-nfs-ganesha-grpc-admin.yaml` — suite skeleton
- [x] Polarion ID mapping for admin cases — see `POLARION.md`

### M2 — Parity + security

- [x] `test_grpc_ganesha_parity.py` + `golden/` baseline capture
- [x] `test_grpc_ganesha_security_negative.py` — UDS, TLS, auth negatives

### M3 — ExportMgr / Admin (blocked on proto)

- [ ] Enable stubbed operations: add_export, remove_export, update_export, shutdown, reload, log_level
- [ ] Wire to existing NFS export tests where possible

### M4 — HA (parked)

- [ ] `test_grpc_ganesha_ha_inject.py`
- [ ] `baremetal/tier1-nfs-ganesha-grpc-ha.yaml`

## Regression gates (existing suites — no new code)

Run with gRPC-enabled NFS image:

- `tier0-nfs-ganesha.yaml` / `tier1-nfs-ganesha.yaml`
- `upstream_nfs_cthon.py`, `upstream_nfs_pynfs.py`, `upstream_nfs_multilock.py`

## Polarion traceability

**37 CephFS** migration Polarion cases are mapped in [`POLARION.md`](POLARION.md)
(CEPH-83632459–CEPH-83632503, excluding three GPFS-only IDs).

The suite `tier1-nfs-ganesha-grpc-admin.yaml` wires each Ceph ID to an automated,
partial, or stub (skipped) test for `--post-results`.

**GPFS-only Polarion IDs** (not in this repo): CEPH-83632481, CEPH-83632495,
CEPH-83632498 — track on `nfs_upstream` fork.

IBM Cloud basic gRPC smoke uses separate IDs (CEPH-83630609–616) in
`tier1-nfs-ganesha-grpc.yaml`.

**Note:** CEPH-83632499 is **CephCI NFS scale** (load/scale suites in this repo),
not IBM Storage Scale.

## gRPC-enabled cephadm NFS image

All gRPC suites expect a **gRPC-built** nfs-ganesha container. Override the
default cephadm NFS image before the first `setup_nfs_cluster()` call:

```yaml
config:
  nfs_container_image: quay.io/rhceph/nfs-ganesha-grpc:<tag>
```

`test_grpc_ganesha_deploy.py` operation `set_nfs_image` applies
`mgr/cephadm/container_image_nfs` via `grpc_deploy.apply_nfs_container_image()`.

Without this image, regression and admin tests that need port 50051 will fail;
proto-blocked operations still skip cleanly (rc=-1).

## Open items

- [x] Polarion IDs for all admin test cases
- [x] `grpc_deploy.py` + suite `nfs_container_image` wiring
- [x] Regression suite (`tier0-nfs-ganesha-grpc-regression.yaml`)
- [x] Perf / build suite (`tier1-nfs-ganesha-grpc-perf.yaml`)
- [ ] Published gRPC NFS container image tag for Jenkins (set per pipeline run)
- [ ] ExportMgr/Admin proto merge timeline from nfs-ganesha upstream
- [ ] Dual-stack policy for RHCS images (D-Bus + gRPC vs gRPC-only)
- [ ] `baremetal/tier1-nfs-ganesha-grpc-ha.yaml` for 6 parked HA IDs
