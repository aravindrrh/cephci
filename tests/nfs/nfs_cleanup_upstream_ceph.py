"""Cleanup artefacts from upstream NFS + Ceph (dnf / cephadm) setup.

Removes:
  - cephadm cluster (rm-cluster --zap-osds)
  - filesystem / LVM signatures on OSD disks from conf volumes
  - dnf-installed ceph / cephadm packages and repo leftovers
  - nfs-ganesha build/install leftovers from the bootstrap test

Why disk wipe is explicit:
  cephadm --zap-osds is not always enough for a clean re-run of
  `ceph orch apply osd --all-available-devices`; leftover GPT/LVM/ceph
  labels cause devices to be skipped on the next suite run.

Why we seed cephadm and tear down LVM carefully:
  Non-installer OSD nodes often have no cephadm package/repo, so rm-cluster
  never runs there. Bare sgdisk/wipefs while ceph-* LVs are still active
  leaves /dev/mapper/ceph--* devices intact (seen on node2/node3).
"""

from utility.log import Log

log = Log(__name__)

CEPH_DIRS = [
    "/etc/ceph",
    "/var/lib/ceph",
    "/var/lib/cephadm",
    "/var/log/ceph",
]

GANESHA_PATHS = [
    "/etc/ganesha",
    "/root/nfs-ganesha",
    "/var/log/ganesha",
    "/var/run/ganesha",
]


def _exec(node, cmd, check_ec=False):
    """Run a sudo command; log and continue on failure by default.

    Uses check_ec=False (not verbose=True) so non-zero exits do not raise —
    verbose short-circuits check_ec in CephNode.exec_command.
    """
    log.info(f"[{node.ip_address}] {cmd}")
    try:
        out = node.exec_command(sudo=True, cmd=cmd, check_ec=check_ec)
        return out
    except Exception as exc:
        log.warning(f"[{node.ip_address}] cmd failed (continuing): {cmd} -> {exc}")
        return None


def _get_fsid(installer):
    """Best-effort FSID lookup when the cluster may already be broken."""
    cmds = [
        "cephadm shell -- ceph fsid",
        "ceph fsid",
        "ls /var/lib/ceph 2>/dev/null | head -1",
    ]
    for cmd in cmds:
        try:
            out, _ = installer.exec_command(sudo=True, cmd=cmd, check_ec=False)
            fsid = (out or "").strip().splitlines()[0].strip() if out else ""
            # FSID is a UUID; skip empty / non-uuid junk
            if fsid and len(fsid) >= 32 and " " not in fsid:
                log.info(f"Found cluster FSID via '{cmd}': {fsid}")
                return fsid
        except Exception:
            continue
    return None


def _stop_nfs_ganesha(nodes):
    for node in nodes:
        _exec(node, "systemctl stop nfs-ganesha")
        _exec(node, "systemctl disable nfs-ganesha")
        _exec(node, "pkill -9 ganesha.nfsd")


def _umount_nfs_clients(clients):
    for client in clients:
        # Drop any leftover NFS mounts from prior cthon / IO tests
        _exec(
            client,
            "mount | awk '/type nfs/{print $3}' | xargs -r -n1 umount -fl",
        )


def _stop_ceph_containers(node):
    """Stop/remove ceph containers that may hold OSD device locks."""
    _exec(node, "podman ps -aq --filter name=ceph | xargs -r podman stop")
    _exec(node, "podman ps -aq --filter name=ceph | xargs -r podman rm -f")
    # systemd scopes left by cephadm (glob via list-units, not shell filename expand)
    _exec(
        node,
        "systemctl list-units --type=scope --no-legend 'ceph*' 2>/dev/null | "
        "awk '{print $1}' | xargs -r systemctl stop",
    )
    _exec(
        node,
        "systemctl list-units --type=scope --failed --no-legend 'ceph*' 2>/dev/null | "
        "awk '{print $1}' | xargs -r systemctl reset-failed",
    )


def _ensure_cephadm(installer, nodes):
    """Make sure every node has a usable cephadm binary.

    Non-installer nodes often lack the Ceph yum repo, so `dnf install cephadm`
    fails. Prefer copying the binary via SFTP (no inter-node SSH dependency);
    fall back to scp from installer if needed.
    """
    try:
        out, _ = installer.exec_command(
            sudo=True, cmd="command -v cephadm", check_ec=False
        )
        src = (out or "").strip().splitlines()[0] if out else ""
    except Exception:
        src = ""

    if not src:
        log.warning(
            "No cephadm on installer — cannot seed other nodes; "
            "will rely on LVM/disk wipe only"
        )
        return

    log.info("Seeding cephadm from installer (%s) at %s", installer.ip_address, src)

    # Read once from installer; push to each peer
    try:
        with installer.remote_file(
            sudo=True, file_name=src, file_mode="rb"
        ) as src_fh:
            cephadm_blob = src_fh.read()
    except Exception as exc:
        log.warning("Failed to read cephadm from installer (%s): %s", src, exc)
        cephadm_blob = None

    for node in nodes:
        if node.ip_address == installer.ip_address:
            continue
        # Skip if already present
        try:
            out, _ = node.exec_command(
                sudo=True, cmd="command -v cephadm", check_ec=False
            )
            if out and out.strip():
                log.info("[%s] cephadm already present", node.ip_address)
                continue
        except Exception:
            pass

        dest = "/usr/sbin/cephadm"
        if cephadm_blob is not None:
            try:
                with node.remote_file(
                    sudo=True, file_name=dest, file_mode="wb"
                ) as dst_fh:
                    dst_fh.write(cephadm_blob)
                    dst_fh.flush()
                _exec(node, f"chmod 755 {dest}")
                log.info("[%s] seeded cephadm via SFTP -> %s", node.ip_address, dest)
                continue
            except Exception as exc:
                log.warning(
                    "[%s] SFTP seed failed (%s); trying scp fallback",
                    node.ip_address,
                    exc,
                )

        # Fallback: scp from installer (needs passwordless SSH between nodes)
        _exec(
            installer,
            f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            f"{src} root@{node.ip_address}:{dest}",
        )
        _exec(
            installer,
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            f"root@{node.ip_address} 'chmod 755 {dest}'",
        )


def _rm_cluster(nodes, fsid):
    """cephadm rm-cluster --force --zap-osds on every node."""
    if not fsid:
        log.warning("No FSID found; skipping cephadm rm-cluster")
        return

    for node in nodes:
        _stop_ceph_containers(node)
        _exec(
            node,
            f"cephadm rm-cluster --force --zap-osds --fsid {fsid}",
        )


def _teardown_ceph_lvm(node):
    """Deactivate and destroy ONLY ceph-* VGs/LVs/PVs (never touch rootvg).

    Must run before sgdisk/wipefs/dd — otherwise kernel keeps /dev/mapper/ceph--*
    devices and lsblk still shows OSD LVs after a "successful" wipe.

    Note: vgs/lvs/pvs --noheadings still space-pads columns, so match on $1/$2
    (awk fields), not /^ceph/ on the whole line.
    """
    # Deactivate only Ceph volume groups
    _exec(
        node,
        "vgs --noheadings -o vg_name 2>/dev/null | "
        "awk '$1 ~ /^ceph/ {print $1}' | xargs -r -n1 vgchange -an",
    )
    # Remove Ceph logical volumes
    _exec(
        node,
        "lvs --noheadings -o lv_name,vg_name 2>/dev/null | "
        "awk '$2 ~ /^ceph/ {print $2\"/\"$1}' | xargs -r -n1 lvremove -fy",
    )
    # Remove Ceph volume groups
    _exec(
        node,
        "vgs --noheadings -o vg_name 2>/dev/null | "
        "awk '$1 ~ /^ceph/ {print $1}' | xargs -r -n1 vgremove -fy",
    )
    # Remove PVs still tagged to a ceph VG
    _exec(
        node,
        "pvs --noheadings -o pv_name,vg_name 2>/dev/null | "
        "awk '$2 ~ /^ceph/ {print $1}' | xargs -r -n1 pvremove -ff",
    )
    # Force-remove any leftover device-mapper entries named ceph*
    _exec(
        node,
        "dmsetup ls 2>/dev/null | awk '$1 ~ /ceph/ {print $1}' | "
        "xargs -r -n1 dmsetup remove -f",
    )


def _wipe_osd_disks(nodes):
    """Wipe every disk listed in conf volumes so next OSD apply sees clean devices."""
    for node in nodes:
        volumes = getattr(node, "volume_list", None) or []
        if not volumes:
            log.info(f"[{node.ip_address}] no OSD volumes in conf; skipping disk wipe")
            continue

        # Containers may still hold OSD devices open
        _stop_ceph_containers(node)
        _teardown_ceph_lvm(node)

        for vol in volumes:
            path = vol.path if hasattr(vol, "path") else str(vol)
            if not path or not path.startswith("/dev/"):
                log.warning(
                    f"[{node.ip_address}] skipping unexpected volume entry: {path}"
                )
                continue

            log.info(f"[{node.ip_address}] wiping OSD disk {path}")
            # Orphan PVs (no VG) are missed by vg-name filters — clear per-disk
            _exec(node, f"pvremove -ff {path}")
            # Order: LVM gone -> zap GPT -> wipe signatures -> zero headers
            _exec(node, f"sgdisk --zap-all {path}")
            _exec(node, f"wipefs -af {path}")
            _exec(
                node,
                f"dd if=/dev/zero of={path} bs=1M count=100 status=none conv=fsync",
            )
            # Best-effort if ceph-volume happens to be present
            _exec(node, f"ceph-volume lvm zap --destroy {path}")

        # Reload partition table so kernel drops stale mapper state
        _exec(node, "partprobe")
        _exec(node, "udevadm settle")
        # Final sweep in case anything reappeared after partprobe
        _teardown_ceph_lvm(node)


def _remove_ceph_packages(nodes):
    pkgs = "cephadm ceph ceph-common ceph-base libcephfs2 libcephfs-devel"
    for node in nodes:
        _exec(node, f"dnf remove -y {pkgs}")
        _exec(node, "podman ps -aq --filter name=ceph | xargs -r podman rm -f")
        _exec(
            node,
            "podman images --format '{{.ID}} {{.Repository}}' | "
            "awk '/ceph/{print $1}' | xargs -r podman rmi -f",
        )


def _remove_ceph_dirs(nodes):
    for node in nodes:
        for path in CEPH_DIRS:
            _exec(node, f"rm -rf {path}")


def _remove_ganesha_artefacts(installer):
    """Remove source tree + install leftovers from make install."""
    for path in GANESHA_PATHS:
        _exec(installer, f"rm -rf {path}")

    # Binaries / libs commonly installed by cmake -- without DESTDIR
    _exec(
        installer,
        "rm -f /usr/bin/ganesha.nfsd /usr/local/bin/ganesha.nfsd "
        "/usr/bin/ganeshactl /usr/local/bin/ganeshactl "
        "/usr/sbin/ganesha.nfsd /usr/local/sbin/ganesha.nfsd",
    )
    _exec(
        installer,
        "rm -rf /usr/lib64/ganesha /usr/local/lib64/ganesha "
        "/usr/lib/ganesha /usr/local/lib/ganesha "
        "/usr/share/ganesha /usr/local/share/ganesha",
    )
    _exec(
        installer,
        "rm -f /usr/lib/systemd/system/nfs-ganesha.service "
        "/usr/local/lib/systemd/system/nfs-ganesha.service",
    )
    _exec(installer, "systemctl daemon-reload")


def run(ceph_cluster, **kw):
    """Cleanup Ceph + NFS-Ganesha artefacts from upstream bootstrap suite."""
    config = kw.get("config") or {}
    wipe_disks = config.get("wipe_disks", True)
    remove_packages = config.get("remove_packages", True)
    remove_ganesha = config.get("remove_ganesha", True)

    installer = ceph_cluster.get_nodes("installer")[0]
    clients = ceph_cluster.get_nodes("client")
    nodes = ceph_cluster.get_nodes()

    log.info("=== Upstream NFS/Ceph cleanup start ===")

    _umount_nfs_clients(clients)
    _stop_nfs_ganesha(nodes)

    # Prefer orderly cephadm teardown before raw disk wipe
    try:
        _exec(installer, "cephadm shell -- ceph mgr module disable cephadm")
    except Exception:
        pass

    # Seed cephadm onto OSD nodes that lack the yum package/repo
    _ensure_cephadm(installer, nodes)

    fsid = _get_fsid(installer)
    _rm_cluster(nodes, fsid)

    if wipe_disks:
        _wipe_osd_disks(nodes)
    else:
        log.info("wipe_disks=false; skipping OSD disk wipe")

    if remove_packages:
        _remove_ceph_packages(nodes)
    _remove_ceph_dirs(nodes)

    if remove_ganesha:
        _remove_ganesha_artefacts(installer)

    log.info("=== Upstream NFS/Ceph cleanup complete ===")
    return 0
