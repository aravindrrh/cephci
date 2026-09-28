"""
Tier-1 functional coverage for ganesha_mgr on Spectrum Scale CES NFS.

One module, many scenarios (same pattern as upstream_nfs_acl_functional.py):
  - Exports (show/display; optional D-Bus add/update/remove of a temp export)
  - Clients (show after mount/IO)
  - Cache show + purge
  - Component logging get/set/getall
  - Conditional logging (clients, exports, policy, composite set/show/reset)
  - Grace
  - Malloc trim

Scale note: durable exports stay owned by mmnfs. D-Bus export mutate uses a
temporary Export_ID only, then removes it. shutdown is gated off by default.

Result statuses: PASS / FAIL / SKIP. Only FAIL fails the overall run.
"""

from __future__ import annotations

import re
import shlex
import traceback
from time import sleep

from cli.exceptions import ConfigError
from tests.nfs.lib.nfs_ganesha_deploy import resolve_ganesha_node
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    MMFS_BIN,
    setup_gpfs_nfs,
    teardown_gpfs_nfs,
)
from utility.log import Log

log = Log(__name__)

PASS, FAIL, SKIP = 0, 1, 2

# High ID unlikely to collide with Scale/mmnfs-assigned export IDs.
TEMP_EXPORT_ID = 9001
TEMP_EXPORT_SUBDIR = "export_mgr_t1"
# NFSv4 clients mount Pseudo, not the GPFS Path.
TEMP_EXPORT_PSEUDO = f"/{TEMP_EXPORT_SUBDIR}"
TEMP_EXPORT_CONF = "/tmp/ganesha_mgr_tier1_export.conf"
TEMP_RO_MOUNT = "/mnt/ganesha_mgr_t1_ro"

DEFAULT_RUN_GROUPS = (
    "exports",
    "clients",
    "cache",
    "logs",
    "condlog",
    "grace",
    "trim",
)

LOG_COMPONENT = "COMPONENT_FSAL"
LOG_LEVEL_DEBUG = "FULL_DEBUG"
LOG_LEVEL_RESTORE = "EVENT"
GANESHA_LOG = "/var/log/ganesha.log"
# Packaged path after USE_ADMIN_TOOLS=ON RPM install (no discovery).
GANESHA_MGR = "/usr/bin/ganesha_mgr"


def run(ceph_cluster, **kw):
    """Entry point: run all Tier-1 ganesha_mgr scenarios and report a table."""
    config = kw.get("config") or {}
    clients_all = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "2"))
    if no_clients > len(clients_all):
        raise ConfigError("The test requires more clients than available")

    enable_mutate = _as_bool(config.get("enable_dbus_export_mutate", True), True)
    enable_shutdown = _as_bool(config.get("enable_shutdown", False), False)
    abort_on_fail = _as_bool(config.get("abort_on_fail", False), False)
    run_groups = config.get("run_groups") or list(DEFAULT_RUN_GROUPS)
    if isinstance(run_groups, str):
        run_groups = [g.strip() for g in run_groups.split(",") if g.strip()]
    # enable_shutdown alone must activate the group (was dead config before).
    if enable_shutdown and "shutdown" not in run_groups:
        run_groups = list(run_groups) + ["shutdown"]
    elif not enable_shutdown and "shutdown" in run_groups:
        run_groups = [g for g in run_groups if g != "shutdown"]
        log.info("Stripped shutdown from run_groups (enable_shutdown=false)")

    gpfs = None
    ganesha = None
    results = []

    log.info(
        "\n"
        + "=" * 70
        + "\n"
        + "  GANESHA_MGR TIER-1 FUNCTIONAL (Spectrum Scale)\n"
        + "  mutate=%s  shutdown=%s  groups=%s\n"
        + "=" * 70,
        enable_mutate,
        enable_shutdown,
        run_groups,
    )

    try:
        gpfs = setup_gpfs_nfs(ceph_cluster, config)
        clients = gpfs["clients"]
        nfs_mount = gpfs["nfs_mount"]
        nfs_export = gpfs["nfs_export"]
        version = gpfs["version"]
        port = gpfs["port"]
        # Host IP already used by setup_gpfs_nfs (installer) — not CES VIP.
        nfs_server_ip = gpfs["nfs_server_host"]

        # D-Bus is local to the Ganesha process — use the nfs-role node.
        ganesha = resolve_ganesha_node(ceph_cluster)
        mgr_bin = GANESHA_MGR

        log.info(
            "ganesha_mgr=%s on %s; nfs_server=%s; export=%s mount=%s",
            mgr_bin,
            ganesha.hostname,
            nfs_server_ip,
            nfs_export,
            nfs_mount,
        )

        _client_io(clients[0], nfs_mount, "tier1_warmup")

        # Probe conditional-logging support once for the whole condlog group.
        condlog_supported = _help_has(ganesha, mgr_bin, "conditional")

        ctx = {
            "ganesha": ganesha,
            "mgr_bin": mgr_bin,
            "clients": clients,
            "nfs_mount": nfs_mount,
            "nfs_export": nfs_export,
            "nfs_server_ip": nfs_server_ip,
            "version": version,
            "port": port,
            "enable_mutate": enable_mutate,
            "scale_fs_root": _scale_fs_root(nfs_export),
            "condlog_supported": condlog_supported,
        }

        group_runners = {
            "exports": _scen_exports,
            "clients": _scen_clients,
            "cache": _scen_cache,
            "logs": _scen_logs,
            "condlog": _scen_condlog,
            "grace": _scen_grace,
            "trim": _scen_trim,
            "shutdown": _scen_shutdown,
        }

        for group in run_groups:
            runner = group_runners.get(group)
            if runner is None:
                log.warning("Unknown run_group %r — skipping", group)
                continue
            log.info("--- Scenario group: %s ---", group)
            group_results = runner(ctx)
            results.extend(group_results)
            if abort_on_fail and any(rc == FAIL for _, rc in group_results):
                log.error("abort_on_fail: stopping after group %s", group)
                break

        return _report_results(results)

    except Exception as e:
        log.error(
            "Fatal error in ganesha_mgr Tier-1: %s\n%s",
            e,
            traceback.format_exc(),
        )
        return 1
    finally:
        try:
            if ganesha:
                _mgr(ganesha, GANESHA_MGR, "reset", "log", "conditional_config")
                _mgr(
                    ganesha,
                    GANESHA_MGR,
                    "set",
                    "log",
                    LOG_COMPONENT,
                    LOG_LEVEL_RESTORE,
                )
                _mgr(ganesha, GANESHA_MGR, "remove", "export", str(TEMP_EXPORT_ID))
            if ganesha:
                fs_root = _scale_fs_root(
                    (gpfs or {}).get("nfs_export", "/ibm/scale_volume/export1")
                )
                temp_path = f"{fs_root}/{TEMP_EXPORT_SUBDIR}"
                ganesha.exec_command(
                    cmd=(
                        f"rm -rf {shlex.quote(temp_path)} "
                        f"{shlex.quote(TEMP_EXPORT_CONF)}"
                    ),
                    sudo=True,
                    check_ec=False,
                )
            if gpfs:
                # Drop leftover IO files on the shared export.
                for c in gpfs["clients"]:
                    c.exec_command(
                        cmd=(
                            f"rm -f {shlex.quote(gpfs['nfs_mount'])}/tier1_* "
                            f"; umount -f {shlex.quote(TEMP_RO_MOUNT)} 2>/dev/null || true"
                            f"; rm -rf {shlex.quote(TEMP_RO_MOUNT)}"
                        ),
                        sudo=True,
                        check_ec=False,
                    )
        except Exception as cleanup_exc:
            log.warning("ganesha_mgr restore best-effort failed: %s", cleanup_exc)

        if gpfs:
            teardown_gpfs_nfs(gpfs["clients"], gpfs["nfs_mount"])
            log.info("Client mount cleanup completed")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _as_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def _remount_clients(clients, mount, server_ip, export, version, port):
    """Remount using the NFS server host IP (not CES VIP)."""
    for client in clients:
        client.exec_command(
            cmd=(
                f"bash -lc 'umount -f {shlex.quote(mount)} 2>/dev/null || "
                f"umount -l {shlex.quote(mount)} 2>/dev/null || true; "
                f"mkdir -p {shlex.quote(mount)}; "
                f"mount -t nfs -o vers={shlex.quote(str(version))},"
                f"port={shlex.quote(str(port))} "
                f"{shlex.quote(server_ip)}:{shlex.quote(export)} "
                f"{shlex.quote(mount)}'"
            ),
            sudo=True,
        )


def _mgr(node, mgr_bin, *args, timeout=120):
    """Run ganesha_mgr; return (rc, stdout, stderr). Never raises on non-zero RC."""
    cmd = " ".join([shlex.quote(mgr_bin)] + [shlex.quote(str(a)) for a in args])
    log.info("[%s] %s", node.hostname, cmd)
    out, err, rc, _ = node.exec_command(
        cmd=cmd, sudo=True, check_ec=False, verbose=True, timeout=timeout
    )
    out = out or ""
    err = err or ""
    if rc != 0:
        log.warning("ganesha_mgr rc=%s stderr=%s stdout=%s", rc, err[:400], out[:400])
    return rc, out, err


def _mgr_ok(rc, out, err=""):
    """True when the CLI reports success (rc=0 or explicit status=True)."""
    blob = f"{out}\n{err}"
    if "status = False" in blob or "status = false" in blob.lower():
        return False
    if "status = True" in blob or "status = true" in blob.lower():
        return True
    return rc == 0


def _help_has(node, mgr_bin, needle):
    _, out, _ = _mgr(node, mgr_bin, "help")
    return needle.lower() in (out or "").lower()


def _ganesha_active(node):
    out, _, rc, _ = node.exec_command(
        cmd="systemctl is-active nfs-ganesha",
        sudo=True,
        check_ec=False,
        verbose=True,
    )
    return rc == 0 and (out or "").strip() == "active"


def _client_io(client, nfs_mount, name):
    path = f"{nfs_mount}/{name}"
    client.exec_command(
        cmd=(
            f"bash -lc 'echo mgr_tier1 > {shlex.quote(path)} "
            f"&& cat {shlex.quote(path)}'"
        ),
        sudo=True,
    )


def _scale_fs_root(nfs_export):
    """/ibm/scale_volume/export1 -> /ibm/scale_volume"""
    parts = nfs_export.rstrip("/").split("/")
    if len(parts) >= 3:
        return "/".join(parts[:3])
    return nfs_export.rsplit("/", 1)[0] or nfs_export


def _parse_export_ids(show_out):
    """Extract export IDs from `show exports` tabular output."""
    ids = []
    for line in (show_out or "").splitlines():
        line = line.strip()
        m = re.match(r"^(\d+)\s*,", line)
        if m:
            ids.append(int(m.group(1)))
    return ids


def _mmnfs_lists_export(node, nfs_export):
    """Return True only if mmnfs export list clearly references the export path."""
    out, err, rc, _ = node.exec_command(
        cmd=f"{MMFS_BIN}/mmnfs export list",
        sudo=True,
        check_ec=False,
        verbose=True,
    )
    text = out or ""
    if rc != 0:
        return False, text, err or ""
    path = nfs_export.rstrip("/")
    # Require the concrete path — do not match generic word "export".
    found = path in text or f"{path}/" in text
    return found, text, err or ""


def _record(results, name, status, detail=""):
    label = {PASS: "PASS", FAIL: "FAIL", SKIP: "SKIP"}.get(status, str(status))
    if detail:
        log.info("%s: %s — %s", name, label, detail)
    else:
        log.info("%s: %s", name, label)
    results.append((name, status))
    return results


def _run_case(results, name, fn):
    """Run one case; record PASS/FAIL/SKIP without aborting the group."""
    log.info(">>> %s", name)
    try:
        ret = fn()
        if isinstance(ret, tuple):
            status = int(ret[0])
            detail = ret[1] if len(ret) > 1 else ""
        else:
            status, detail = (PASS if ret else FAIL), ""
        if status not in (PASS, FAIL, SKIP):
            status = PASS if status == 0 else FAIL
        _record(results, name, status, detail or "")
    except Exception as exc:
        log.error("%s raised: %s\n%s", name, exc, traceback.format_exc())
        _record(results, name, FAIL, str(exc))
    return results


def _report_results(results):
    log.info("=" * 60)
    log.info("GANESHA_MGR TIER-1 RESULTS")
    log.info("=" * 60)
    hard, skipped = [], []
    for name, status in results:
        label = {PASS: "PASS", FAIL: "FAIL", SKIP: "SKIP"}.get(status, "?")
        log.info("  %-45s %s", name, label)
        if status == FAIL:
            hard.append(name)
        elif status == SKIP:
            skipped.append(name)
    log.info("=" * 60)
    if skipped:
        log.info("Skipped: %s", skipped)
    if hard:
        log.error("Unexpected failures: %s", hard)
        log.error("OVERALL RESULT: FAIL")
        return 1
    if skipped:
        log.info(
            "OVERALL RESULT: PASS (%d skipped, no failures)",
            len(skipped),
        )
        return 0
    log.info("OVERALL RESULT: PASS (all sub-tests passed)")
    return 0


def _write_temp_export_conf(node, temp_path, access_type="RW"):
    conf = f"""
EXPORT
{{
    Export_Id = {TEMP_EXPORT_ID};
    Path = {temp_path};
    Pseudo = /{TEMP_EXPORT_SUBDIR};
    Access_Type = {access_type};
    Squash = None;
    Protocols = 3,4;
    Transports = TCP;
    FSAL {{
        Name = GPFS;
    }}
}}
"""
    node.exec_command(
        cmd=f"cat > {shlex.quote(TEMP_EXPORT_CONF)} <<'EOF'\n{conf}\nEOF\n",
        sudo=True,
    )


def _add_or_update_export(node, mgr, action, temp_path, access_type):
    """action is 'add' or 'update'. Tries Export_Id then Export_ID expression."""
    _write_temp_export_conf(node, temp_path, access_type=access_type)
    for expr_key in ("Export_Id", "Export_ID"):
        rc, out, err = _mgr(
            node,
            mgr,
            action,
            "export",
            TEMP_EXPORT_CONF,
            f"EXPORT({expr_key}={TEMP_EXPORT_ID})",
        )
        if _mgr_ok(rc, out, err):
            return True, out
    return False, err or out


# ---------------------------------------------------------------------------
# Scenario groups
# ---------------------------------------------------------------------------


def _scen_exports(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    nfs_export = ctx["nfs_export"]
    enable_mutate = ctx["enable_mutate"]
    fs_root = ctx["scale_fs_root"]
    clients = ctx["clients"]
    nfs_server_ip = ctx["nfs_server_ip"]
    version = ctx["version"]
    port = ctx["port"]
    temp_path = f"{fs_root}/{TEMP_EXPORT_SUBDIR}"
    mutate_ok = {"added": False}

    def e1_show_exports():
        rc, out, err = _mgr(ganesha, mgr, "show", "exports")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        ids = _parse_export_ids(out)
        found, mm_out, mm_err = _mmnfs_lists_export(ganesha, nfs_export)
        if not found:
            return FAIL, f"mmnfs missing {nfs_export}: {mm_err or mm_out[:200]}"
        if not ids:
            # Fail closed: cannot correlate without parseable IDs.
            return FAIL, "show exports produced no parseable export IDs"
        return PASS, f"ids={ids}"

    def e2_display_export():
        rc, out, err = _mgr(ganesha, mgr, "show", "exports")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        ids = _parse_export_ids(out)
        exp_id = next((i for i in ids if i != 0), ids[0] if ids else None)
        if exp_id is None:
            return FAIL, "no export id to display"
        rc, out, err = _mgr(ganesha, mgr, "display", "export", str(exp_id))
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        if "path" not in out.lower() and "export" not in out.lower():
            return FAIL, f"unexpected display output: {out[:200]}"
        return PASS, out[:200]

    def e3_display_invalid():
        rc, out, err = _mgr(ganesha, mgr, "display", "export", "99999")
        if not _ganesha_active(ganesha):
            return FAIL, "nfs-ganesha inactive after invalid display"
        blob = (out + err).lower()
        rejected = (
            not _mgr_ok(rc, out, err)
            or "no such" in blob
            or "not found" in blob
            or "status = false" in blob
        )
        if not rejected:
            return FAIL, f"invalid id was accepted: {out[:200]}"
        return PASS, "rejected invalid id"

    _run_case(results, "E1 show exports", e1_show_exports)
    _run_case(results, "E2 display export", e2_display_export)
    _run_case(results, "E3 display invalid export", e3_display_invalid)

    if not enable_mutate:
        _record(results, "E4 add export (temp)", SKIP, "enable_dbus_export_mutate=false")
        _record(results, "E5 update export (temp RO)", SKIP, "mutate disabled")
        _record(results, "E6 remove export (temp)", SKIP, "mutate disabled")
        _record(results, "E7 main export intact", SKIP, "mutate disabled")
        return results

    def e4_add_export():
        ganesha.exec_command(
            cmd=(
                f"mkdir -p {shlex.quote(temp_path)} "
                f"&& chmod 755 {shlex.quote(temp_path)}"
            ),
            sudo=True,
        )
        ok, detail = _add_or_update_export(
            ganesha, mgr, "add", temp_path, access_type="RW"
        )
        if not ok:
            return FAIL, detail
        _, show, _ = _mgr(ganesha, mgr, "show", "exports")
        ids = _parse_export_ids(show)
        if TEMP_EXPORT_ID not in ids:
            return FAIL, f"temp id {TEMP_EXPORT_ID} not in show exports: {ids}"
        mutate_ok["added"] = True
        return PASS, f"added Export_Id={TEMP_EXPORT_ID}"

    def e5_update_export_ro():
        if not mutate_ok["added"]:
            return SKIP, "skipped — E4 did not add export"
        ok, detail = _add_or_update_export(
            ganesha, mgr, "update", temp_path, access_type="RO"
        )
        if not ok:
            return FAIL, detail
        # NFSv4 must mount Pseudo (/export_mgr_t1), not the GPFS Path.
        client = clients[0]
        ro_file = f"{TEMP_RO_MOUNT}/ro_probe"
        client.exec_command(
            cmd=(
                f"bash -lc 'umount -f {shlex.quote(TEMP_RO_MOUNT)} 2>/dev/null || true; "
                f"mkdir -p {shlex.quote(TEMP_RO_MOUNT)}; "
                f"mount -t nfs -o vers={shlex.quote(str(version))},"
                f"port={shlex.quote(str(port))} "
                f"{shlex.quote(nfs_server_ip)}:{shlex.quote(TEMP_EXPORT_PSEUDO)} "
                f"{shlex.quote(TEMP_RO_MOUNT)}'"
            ),
            sudo=True,
        )
        _, _, probe_rc, _ = client.exec_command(
            cmd=f"bash -lc 'echo should_fail > {shlex.quote(ro_file)}'",
            sudo=True,
            check_ec=False,
            verbose=True,
        )
        client.exec_command(
            cmd=(
                f"bash -lc 'umount -f {shlex.quote(TEMP_RO_MOUNT)} 2>/dev/null || true; "
                f"rm -rf {shlex.quote(TEMP_RO_MOUNT)}'"
            ),
            sudo=True,
            check_ec=False,
        )
        if probe_rc == 0:
            return FAIL, "write succeeded on RO export after update"
        return PASS, f"RO update enforced via {TEMP_EXPORT_PSEUDO} (write denied, rc={probe_rc})"

    def e6_remove_export():
        if not mutate_ok["added"]:
            return SKIP, "skipped — E4 did not add export"
        rc, out, err = _mgr(ganesha, mgr, "remove", "export", str(TEMP_EXPORT_ID))
        sleep(1)
        _, show, _ = _mgr(ganesha, mgr, "show", "exports")
        ids = _parse_export_ids(show)
        if TEMP_EXPORT_ID in ids:
            return FAIL, f"id {TEMP_EXPORT_ID} still present after remove ({err or out})"
        if not _ganesha_active(ganesha):
            return FAIL, "nfs-ganesha inactive after remove"
        # remove_export is often void; absence from show is the contract.
        if rc != 0:
            log.warning("remove export rc=%s (accepted if id gone)", rc)
        mutate_ok["added"] = False
        return PASS, "removed"

    def e7_main_export_intact():
        found, mm_out, mm_err = _mmnfs_lists_export(ganesha, nfs_export)
        if not found:
            return FAIL, f"main export missing from mmnfs: {mm_err or mm_out[:200]}"
        if not _ganesha_active(ganesha):
            return FAIL, "nfs-ganesha inactive"
        _, show, _ = _mgr(ganesha, mgr, "show", "exports")
        ids = _parse_export_ids(show)
        if not ids:
            return FAIL, "show exports empty after mutate cycle"
        return PASS, f"mmnfs+show ok ids={ids}"

    _run_case(results, "E4 add export (temp)", e4_add_export)
    _run_case(results, "E5 update export (temp RO)", e5_update_export_ro)
    _run_case(results, "E6 remove export (temp)", e6_remove_export)
    _run_case(results, "E7 main export intact", e7_main_export_intact)

    ganesha.exec_command(
        cmd=f"rm -rf {shlex.quote(temp_path)} {shlex.quote(TEMP_EXPORT_CONF)}",
        sudo=True,
        check_ec=False,
    )
    return results


def _scen_clients(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    nfs_mount = ctx["nfs_mount"]

    def c1_show_after_io():
        _client_io(clients[0], nfs_mount, "tier1_client_show")
        sleep(2)
        rc, out, err = _mgr(ganesha, mgr, "show", "clients")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        if "No clients" in out:
            return FAIL, "show clients empty right after IO"
        client_ip = clients[0].ip_address
        if client_ip not in out:
            return FAIL, f"client IP {client_ip} not in show clients: {out[:300]}"
        return PASS, f"found {client_ip}"

    def c2_second_client():
        if len(clients) < 2:
            return SKIP, "only one client configured"
        _client_io(clients[1], nfs_mount, "tier1_client2")
        sleep(2)
        rc, out, err = _mgr(ganesha, mgr, "show", "clients")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        ip2 = clients[1].ip_address
        if ip2 not in out:
            return FAIL, f"second client IP {ip2} not listed: {out[:300]}"
        return PASS, f"found {ip2}"

    def c3_add_remove_client():
        ip = clients[0].ip_address
        rc1, out1, err1 = _mgr(ganesha, mgr, "add", "client", ip)
        if not _mgr_ok(rc1, out1, err1):
            return FAIL, f"add client failed: {err1 or out1}"
        rc2, out2, err2 = _mgr(ganesha, mgr, "remove", "client", ip)
        if not _mgr_ok(rc2, out2, err2):
            return FAIL, f"remove client failed: {err2 or out2}"
        if not _ganesha_active(ganesha):
            return FAIL, "nfs-ganesha down after add/remove client"
        return PASS, "add/remove ok"

    _run_case(results, "C1 show clients after IO", c1_show_after_io)
    _run_case(results, "C2 second client show", c2_second_client)
    _run_case(results, "C3 add/remove client", c3_add_remove_client)
    return results


def _scen_cache(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    nfs_mount = ctx["nfs_mount"]

    def p1_posix_fs():
        rc, out, err = _mgr(ganesha, mgr, "show", "posix_fs")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        return PASS, out[:200]

    def p2_idmapper_shows():
        # users + groups are required. uid2grp D-Bus is known-broken on some
        # builds (nfs-ganesha#1322) — do not hard-fail the group on that alone.
        hard = []
        notes = []
        for sub in ("idmapper_users", "idmapper_groups"):
            rc, out, err = _mgr(ganesha, mgr, "show", sub)
            if not _mgr_ok(rc, out, err):
                hard.append(f"{sub}:{err or out}")
        rc_u, out_u, err_u = _mgr(ganesha, mgr, "show", "idmapper_uid2grp")
        if not _mgr_ok(rc_u, out_u, err_u):
            notes.append(
                "idmapper_uid2grp status=False "
                "(known ganesha_mgr/D-Bus gap; not failing P2)"
            )
            log.warning("show idmapper_uid2grp: %s", err_u or out_u)
        else:
            notes.append("idmapper_uid2grp ok")
        if hard:
            return FAIL, "; ".join(hard)
        return PASS, "; ".join(notes)

    def p3_purge_idmapper():
        _client_io(clients[0], nfs_mount, "tier1_idmap")
        rc, out, err = _mgr(ganesha, mgr, "purge", "idmapper")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        try:
            _client_io(clients[0], nfs_mount, "tier1_idmap_after")
        except Exception as exc:
            return FAIL, f"IO after purge failed: {exc}"
        return PASS, "purged + IO ok"

    def p4_purge_others():
        failed = []
        for name in ("idmapper_negative", "gids", "netgroups"):
            rc, out, err = _mgr(ganesha, mgr, "purge", name)
            if not _ganesha_active(ganesha):
                return FAIL, f"ganesha down after purge {name}"
            if name == "netgroups" and not _mgr_ok(rc, out, err):
                # Netgroups unused on many Scale setups — allow skip.
                log.info("purge netgroups not ok (tolerated): %s", err or out)
                continue
            if not _mgr_ok(rc, out, err):
                failed.append(f"{name}:{err or out}")
        if failed:
            return FAIL, "; ".join(failed)
        return PASS, "purge gids/idmapper_negative ok"

    _run_case(results, "P1 show posix_fs", p1_posix_fs)
    _run_case(results, "P2 show idmapper caches", p2_idmapper_shows)
    _run_case(results, "P3 purge idmapper + IO", p3_purge_idmapper)
    _run_case(results, "P4 purge gids/netgroups/neg", p4_purge_others)
    return results


def _scen_logs(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    nfs_mount = ctx["nfs_mount"]

    def _resolve_comp():
        rc, before, err = _mgr(ganesha, mgr, "get", "log", LOG_COMPONENT)
        if _mgr_ok(rc, before, err):
            return LOG_COMPONENT, before
        rc, before, err = _mgr(ganesha, mgr, "get", "log", "FSAL")
        if _mgr_ok(rc, before, err):
            return "FSAL", before
        return None, err or before

    def l1_getall():
        rc, out, err = _mgr(ganesha, mgr, "getall", "logs")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        lines = [ln for ln in out.splitlines() if ln.strip()]
        if len(lines) < 1:
            return FAIL, "getall logs returned no components"
        return PASS, f"{len(lines)} lines"

    def l2_get_set_roundtrip():
        comp, before = _resolve_comp()
        if comp is None:
            return FAIL, f"cannot get log component: {before}"
        rc_s, out, err = _mgr(ganesha, mgr, "set", "log", comp, LOG_LEVEL_DEBUG)
        if not _mgr_ok(rc_s, out, err):
            _mgr(ganesha, mgr, "set", "log", comp, LOG_LEVEL_RESTORE)
            return FAIL, err or out
        rc2, after, err2 = _mgr(ganesha, mgr, "get", "log", comp)
        _mgr(ganesha, mgr, "set", "log", comp, LOG_LEVEL_RESTORE)
        if not _mgr_ok(rc2, after, err2):
            return FAIL, err2 or after
        if not any(
            tok in after for tok in (LOG_LEVEL_DEBUG, "FULL_DEBUG", "NIV_FULL_DEBUG")
        ):
            return FAIL, f"set FULL_DEBUG but get returned: {after[:120]!r}"
        return PASS, f"comp={comp}"

    def l3_io_while_debug():
        comp, _ = _resolve_comp()
        if comp is None:
            return FAIL, "cannot resolve log component"
        # Snapshot log size, enable debug, IO, require growth (best-effort).
        size_before, _, _, _ = ganesha.exec_command(
            cmd=f"stat -c %s {shlex.quote(GANESHA_LOG)} 2>/dev/null || echo 0",
            sudo=True,
            check_ec=False,
            verbose=True,
        )
        _mgr(ganesha, mgr, "set", "log", comp, LOG_LEVEL_DEBUG)
        try:
            _client_io(clients[0], nfs_mount, "tier1_log_io")
            sleep(1)
        finally:
            _mgr(ganesha, mgr, "set", "log", comp, LOG_LEVEL_RESTORE)
        if not _ganesha_active(ganesha):
            return FAIL, "daemon down"
        size_after, _, _, _ = ganesha.exec_command(
            cmd=f"stat -c %s {shlex.quote(GANESHA_LOG)} 2>/dev/null || echo 0",
            sudo=True,
            check_ec=False,
            verbose=True,
        )
        try:
            b = int((size_before or "0").strip().splitlines()[-1])
            a = int((size_after or "0").strip().splitlines()[-1])
        except ValueError:
            return PASS, "daemon ok (log size unreadable)"
        if a < b:
            return FAIL, f"ganesha.log shrank {b} -> {a}"
        # Growth is expected under FULL_DEBUG; if flat, still require daemon ok
        # but fail — otherwise we are not testing logging.
        if a == b:
            return FAIL, f"ganesha.log did not grow under FULL_DEBUG ({a} bytes)"
        return PASS, f"log grew {b} -> {a}"

    def l4_bad_component():
        rc, out, err = _mgr(ganesha, mgr, "set", "log", "COMPONENT_NOT_REAL", "EVENT")
        if not _ganesha_active(ganesha):
            return FAIL, "daemon died on bad component"
        if _mgr_ok(rc, out, err):
            return FAIL, "invalid component was accepted"
        return PASS, f"rejected rc={rc}"

    _run_case(results, "L1 getall logs", l1_getall)
    _run_case(results, "L2 get/set log round-trip", l2_get_set_roundtrip)
    _run_case(results, "L3 IO under FULL_DEBUG", l3_io_while_debug)
    _run_case(results, "L4 bad log component", l4_bad_component)
    return results


def _scen_condlog(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    client_ip = clients[0].ip_address
    supported = ctx["condlog_supported"]

    if not supported:
        for name in (
            "CL1 conditional clients",
            "CL2 conditional exports",
            "CL3 match policy",
            "CL4 set/show conditional_config",
            "CL5 reset conditional_config",
            "CL6 remove conditional_clients *",
        ):
            _record(results, name, SKIP, "build lacks conditional logging")
        return results

    _, show, _ = _mgr(ganesha, mgr, "show", "exports")
    ids = _parse_export_ids(show)
    export_id = next((i for i in ids if i != 0), ids[0] if ids else None)
    if export_id is None:
        for name in (
            "CL1 conditional clients",
            "CL2 conditional exports",
            "CL3 match policy",
            "CL4 set/show conditional_config",
            "CL5 reset conditional_config",
            "CL6 remove conditional_clients *",
        ):
            _record(results, name, FAIL, "no export id for conditional_exports")
        return results

    has_composite = _help_has(ganesha, mgr, "conditional_config")

    def cl1_clients():
        rc, out, err = _mgr(ganesha, mgr, "add", "conditional_clients", client_ip)
        if not _mgr_ok(rc, out, err) and "OK" not in out:
            return FAIL, err or out
        rc2, show_c, err2 = _mgr(ganesha, mgr, "show", "conditional_clients")
        if not _mgr_ok(rc2, show_c, err2):
            return FAIL, err2 or show_c
        if client_ip not in show_c:
            return FAIL, f"client {client_ip} not listed after add: {show_c[:200]}"
        rc3, out3, err3 = _mgr(ganesha, mgr, "remove", "conditional_clients", client_ip)
        if not _mgr_ok(rc3, out3, err3) and "OK" not in out3:
            return FAIL, err3 or out3
        return PASS, "add/show/remove ok"

    def cl2_exports():
        rc, out, err = _mgr(
            ganesha, mgr, "add", "conditional_exports", str(export_id)
        )
        if not _mgr_ok(rc, out, err) and "OK" not in out:
            return FAIL, err or out
        rc2, show_e, err2 = _mgr(ganesha, mgr, "show", "conditional_exports")
        if not _mgr_ok(rc2, show_e, err2):
            return FAIL, err2 or show_e
        if str(export_id) not in show_e:
            return FAIL, f"export {export_id} not listed: {show_e[:200]}"
        _mgr(ganesha, mgr, "remove", "conditional_exports", str(export_id))
        return PASS, show_e[:200]

    def cl3_policy():
        for pol in ("ANY", "ALL"):
            rc, out, err = _mgr(
                ganesha, mgr, "update", "conditional_match_policy", pol
            )
            if not _mgr_ok(rc, out, err) and "OK" not in out and "MATCH" not in out:
                return FAIL, err or out
        rc, show_p, err = _mgr(ganesha, mgr, "show", "conditional_match_policy")
        if not _mgr_ok(rc, show_p, err):
            return FAIL, err or show_p
        return PASS, show_p[:200]

    def cl4_composite_set_show():
        if not has_composite:
            return SKIP, "no conditional_config in this build"
        rc, out, err = _mgr(
            ganesha,
            mgr,
            "set",
            "log",
            "conditional_config",
            "--components",
            "FSAL,DISPATCH",
            "--level",
            "FULL_DEBUG",
            "--clients",
            client_ip,
            "--export-ids",
            str(export_id),
            "--policy",
            "ANY",
        )
        if not _mgr_ok(rc, out, err) and "OK" not in out:
            return FAIL, err or out
        rc2, show_cfg, err2 = _mgr(ganesha, mgr, "show", "log", "conditional_config")
        if not _mgr_ok(rc2, show_cfg, err2):
            return FAIL, err2 or show_cfg
        if client_ip not in show_cfg:
            return FAIL, f"client missing from conditional_config show: {show_cfg[:300]}"
        return PASS, show_cfg[:300]

    def cl5_reset_full():
        if not has_composite:
            return SKIP, "no conditional_config in this build"
        rc, out, err = _mgr(ganesha, mgr, "reset", "log", "conditional_config")
        if not _mgr_ok(rc, out, err) and "OK" not in out and "default" not in out.lower():
            return FAIL, err or out
        return PASS, out[:200]

    def cl6_star_client_quoted():
        _mgr(ganesha, mgr, "add", "conditional_clients", "*")
        rc, out, err = _mgr(ganesha, mgr, "remove", "conditional_clients", "*")
        if not _ganesha_active(ganesha):
            return FAIL, "daemon down"
        if not _mgr_ok(rc, out, err) and "OK" not in out:
            # Some builds reject '*'; still require no crash.
            return FAIL, err or out
        return PASS, f"rc={rc}"

    _run_case(results, "CL1 conditional clients", cl1_clients)
    _run_case(results, "CL2 conditional exports", cl2_exports)
    _run_case(results, "CL3 match policy", cl3_policy)
    _run_case(results, "CL4 set/show conditional_config", cl4_composite_set_show)
    _run_case(results, "CL5 reset conditional_config", cl5_reset_full)
    _run_case(results, "CL6 remove conditional_clients *", cl6_star_client_quoted)
    return results


def _scen_grace(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    nfs_mount = ctx["nfs_mount"]
    nfs_export = ctx["nfs_export"]
    nfs_server_ip = ctx["nfs_server_ip"]
    version = ctx["version"]
    port = ctx["port"]
    # D-Bus grace is for cluster/node reclaim (peer IP), not NFS client IPs.
    node_ip = ganesha.ip_address

    def g1_grace_protocol_node():
        """
        Invoke grace for the protocol-node IP, then prove daemon + mount recover.

        Why we do not require status=True: without a real HA peer reclaim
        context, many builds return status=False even for a valid node IP.
        Tier-1 asserts the CLI is callable and service/IO stay healthy.
        """
        _client_io(clients[0], nfs_mount, "tier1_grace")
        rc, out, err = _mgr(ganesha, mgr, "grace", node_ip)
        if not _ganesha_active(ganesha):
            return FAIL, "daemon down after grace"
        # Remount — grace can disturb client state.
        _remount_clients(
            [clients[0]], nfs_mount, nfs_server_ip, nfs_export, version, port
        )
        try:
            _client_io(clients[0], nfs_mount, "tier1_grace_after")
        except Exception as exc:
            return FAIL, f"IO after grace+remount: {exc}"
        detail = f"node={node_ip} rc={rc} status_ok={_mgr_ok(rc, out, err)}"
        if not _mgr_ok(rc, out, err):
            detail += " (status=False tolerated without HA reclaim peers)"
        return PASS, detail

    def g2_grace_bogus_ip():
        """Bogus IP: require daemon stays up; status=False is fine."""
        rc, out, err = _mgr(ganesha, mgr, "grace", "203.0.113.99")
        if not _ganesha_active(ganesha):
            return FAIL, "daemon down after bogus grace"
        return PASS, (
            f"rc={rc} status_ok={_mgr_ok(rc, out, err)} "
            f"out={(out or err)[:120]}"
        )

    _run_case(results, "G1 grace protocol-node IP", g1_grace_protocol_node)
    _run_case(results, "G2 grace bogus IP", g2_grace_bogus_ip)
    return results


def _scen_trim(ctx):
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    nfs_mount = ctx["nfs_mount"]
    trim_supported = _help_has(ganesha, mgr, "trim")

    def t1_status():
        if not trim_supported:
            return SKIP, "trim not in ganesha_mgr help"
        rc, out, err = _mgr(ganesha, mgr, "trim", "status")
        if not _mgr_ok(rc, out, err):
            return FAIL, err or out
        return PASS, out[:200]

    def t2_enable_call_disable():
        if not trim_supported:
            return SKIP, "trim not in ganesha_mgr help"
        r1, o1, e1 = _mgr(ganesha, mgr, "trim", "enable")
        if not _mgr_ok(r1, o1, e1):
            return FAIL, e1 or o1
        r2, o2, e2 = _mgr(ganesha, mgr, "trim", "call")
        if not _mgr_ok(r2, o2, e2):
            return FAIL, e2 or o2
        try:
            _client_io(clients[0], nfs_mount, "tier1_trim")
        except Exception as exc:
            return FAIL, f"IO during trim: {exc}"
        r3, o3, e3 = _mgr(ganesha, mgr, "trim", "disable")
        if not _mgr_ok(r3, o3, e3):
            return FAIL, e3 or o3
        r4, o4, e4 = _mgr(ganesha, mgr, "trim", "status")
        if not _mgr_ok(r4, o4, e4):
            return FAIL, e4 or o4
        if not _ganesha_active(ganesha):
            return FAIL, "daemon down"
        return PASS, "enable/call/disable/status ok"

    _run_case(results, "T1 trim status", t1_status)
    _run_case(results, "T2 trim enable/call/disable", t2_enable_call_disable)
    return results


def _scen_shutdown(ctx):
    """Only reached when enable_shutdown injects this group into run_groups."""
    results = []
    ganesha = ctx["ganesha"]
    mgr = ctx["mgr_bin"]
    clients = ctx["clients"]
    nfs_mount = ctx["nfs_mount"]
    nfs_export = ctx["nfs_export"]
    nfs_server_ip = ctx["nfs_server_ip"]
    version = ctx["version"]
    port = ctx["port"]

    def s1_shutdown_and_restart():
        rc, out, err = _mgr(ganesha, mgr, "shutdown")
        sleep(2)
        ganesha.exec_command(
            cmd="systemctl start nfs-ganesha",
            sudo=True,
            check_ec=False,
        )
        sleep(5)
        if not _ganesha_active(ganesha):
            return FAIL, f"restart failed after shutdown ({err or out})"
        _remount_clients(clients, nfs_mount, nfs_server_ip, nfs_export, version, port)
        try:
            _client_io(clients[0], nfs_mount, "tier1_after_shutdown")
        except Exception as exc:
            return FAIL, f"IO after restart: {exc}"
        return PASS, f"shutdown_rc={rc}"

    _run_case(results, "S1 shutdown + restart", s1_shutdown_and_restart)
    return results
