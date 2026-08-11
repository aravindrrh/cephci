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
    """Run a sudo command; log and continue on failure by default."""
    log.info(f"[{node.ip_address}] {cmd}")
    try:
        out = node.exec_command(sudo=True, cmd=cmd, check_ec=check_ec, verbose=True)
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
        _exec(node, "systemctl stop nfs-ganesha || true")
        _exec(node, "systemctl disable nfs-ganesha || true")
        _exec(node, "pkill -9 ganesha.nfsd || true")


def _umount_nfs_clients(clients):
    for client in clients:
        # Drop any leftover NFS mounts from prior cthon / IO tests
        _exec(
            client,
            "mount | awk '/type nfs/{print $3}' | xargs -r -n1 umount -fl || true",
        )


def _rm_cluster(nodes, fsid):
    """cephadm rm-cluster --force --zap-osds on every node that has cephadm."""
    if not fsid:
        log.warning("No FSID found; skipping cephadm rm-cluster")
        return

    for node in nodes:
        _exec(node, "dnf install -y cephadm || true")
        _exec(
            node,
            f"cephadm rm-cluster --force --zap-osds --fsid {fsid} || true",
        )


def _wipe_osd_disks(nodes):
    """Wipe every disk listed in conf volumes so next OSD apply sees clean devices."""
    for node in nodes:
        volumes = getattr(node, "volume_list", None) or []
        if not volumes:
            log.info(f"[{node.ip_address}] no OSD volumes in conf; skipping disk wipe")
            continue

        # Tear down any leftover ceph LVM first so wipefs can clear the base disk
        _exec(node, "vgchange -an || true")
        _exec(
            node,
            "vgs --noheadings -o vg_name 2>/dev/null | "
            "grep -E 'ceph|osd' | xargs -r -n1 vgremove -y || true",
        )

        for vol in volumes:
            path = vol.path if hasattr(vol, "path") else str(vol)
            if not path or not path.startswith("/dev/"):
                log.warning(f"[{node.ip_address}] skipping unexpected volume entry: {path}")
                continue

            log.info(f"[{node.ip_address}] wiping OSD disk {path}")
            # Order matters: deactivate -> zap GPT -> wipe signatures -> zero headers
            _exec(node, f"sgdisk --zap-all {path} || true")
            _exec(node, f"wipefs -af {path} || true")
            _exec(
                node,
                f"dd if=/dev/zero of={path} bs=1M count=100 status=none conv=fsync || true",
            )
            # If ceph-volume is still present, destroy any residual LV tags
            _exec(node, f"ceph-volume lvm zap --destroy {path} || true")


def _remove_ceph_packages(nodes):
    pkgs = "cephadm ceph ceph-common ceph-base libcephfs2 libcephfs-devel"
    for node in nodes:
        _exec(node, f"dnf remove -y {pkgs} || true")
        # Drop containers left behind if rm-cluster was incomplete
        _exec(
            node,
            "podman ps -aq --filter name=ceph | xargs -r podman rm -f || true",
        )
        _exec(
            node,
            "podman ps -aq --filter name=ceph | xargs -r true; "
            "podman images --format '{{.ID}} {{.Repository}}' | "
            "awk '/ceph/{print $1}' | xargs -r podman rmi -f || true",
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
        "/usr/sbin/ganesha.nfsd /usr/local/sbin/ganesha.nfsd || true",
    )
    _exec(
        installer,
        "rm -rf /usr/lib64/ganesha /usr/local/lib64/ganesha "
        "/usr/lib/ganesha /usr/local/lib/ganesha "
        "/usr/share/ganesha /usr/local/share/ganesha || true",
    )
    _exec(
        installer,
        "rm -f /usr/lib/systemd/system/nfs-ganesha.service "
        "/usr/local/lib/systemd/system/nfs-ganesha.service || true",
    )
    _exec(installer, "systemctl daemon-reload || true")


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
        _exec(installer, "cephadm shell -- ceph mgr module disable cephadm || true")
    except Exception:
        pass

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
