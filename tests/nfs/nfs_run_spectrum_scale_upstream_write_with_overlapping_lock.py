"""Overlapping NFSv4 write-lock stress on a client mount."""

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

        _, write_bin = build_lock_binaries(client)
        rc = client.exec_command(
            sudo=True,
            cmd=f"{write_bin} {mount_path} {duration}",
            long_running=True,
            timeout=duration + 120,
        )
        log.info("write overlapping lock finished (rc=%s)", rc)
        return 0 if rc in (0, None) else 1
    except Exception as exc:
        log.error("write_with_overlapping_lock failed: %s", exc)
        return 1
    finally:
        if gpfs:
            cleanup_mount(gpfs["clients"], gpfs["nfs_mount"])
