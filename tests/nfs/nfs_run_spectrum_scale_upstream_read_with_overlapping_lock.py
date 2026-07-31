"""Overlapping NFSv4 read-lock stress on a client mount."""

from tests.nfs.lib.spectrum_scale_custom_cases import (
    build_lock_binaries,
    cleanup_mount,
    merge_custom_config,
    prepare_mount,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    conf = merge_custom_config(kw.get("config"))
    duration = int(conf.get("duration", 300))
    gpfs = None

    try:
        gpfs = prepare_mount(ceph_cluster, conf)
        client = gpfs["clients"][0]
        mount_path = gpfs["nfs_mount"]

        read_bin, _ = build_lock_binaries(client)
        # Seed file for readers
        client.exec_command(
            sudo=True,
            cmd=f"echo lockseed > {mount_path}/testfile.txt",
        )
        rc = client.exec_command(
            sudo=True,
            cmd=f"{read_bin} {mount_path} {duration}",
            long_running=True,
            timeout=duration + 120,
        )
        log.info("read overlapping lock finished (rc=%s)", rc)
        return 0 if rc in (0, None) else 1
    except Exception as exc:
        log.error("read_with_overlapping_lock failed: %s", exc)
        return 1
    finally:
        if gpfs:
            cleanup_mount(gpfs["clients"], gpfs["nfs_mount"])
