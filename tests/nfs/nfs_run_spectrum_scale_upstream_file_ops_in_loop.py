"""Create / list / delete loop on a client NFS mount (Spectrum Scale upstream)."""

from tests.nfs.lib.spectrum_scale_custom_cases import (
    cleanup_mount,
    merge_custom_config,
    prepare_mount,
    write_remote,
)
from utility.log import Log

log = Log(__name__)

_LOOP_SH = r"""#!/usr/bin/sh
dir="$1"
duration="${2:-300}"

if [ -z "$dir" ]; then
  echo "Usage: $0 <directory> [duration_sec]"
  exit 1
fi

start_time=$(date +%s)

while true; do
  elapsed_time=$(( $(date +%s) - start_time ))
  if [ "$elapsed_time" -gt "$duration" ]; then
    echo "Exiting after $elapsed_time seconds"
    sync
    exit 0
  fi

  dd if=/dev/urandom of="$dir/testfile.txt" bs=1k count=1 status=none
  ls -lrt "$dir" > /dev/null
  rm -f "$dir/testfile.txt"
done
"""


def run(ceph_cluster, **kw):
    """
    Mount export on the NFS client and run create/list/delete for *duration* seconds.
    """
    conf = merge_custom_config(kw.get("config"))
    duration = int(conf.get("duration", 300))
    gpfs = None

    try:
        gpfs = prepare_mount(ceph_cluster, conf)
        client = gpfs["clients"][0]
        mount_path = gpfs["nfs_mount"]

        script = f"{mount_path}/cr_rm_loop.sh"
        write_remote(client, script, _LOOP_SH)
        client.exec_command(sudo=True, cmd=f"chmod +x {script}")
        rc = client.exec_command(
            sudo=True,
            cmd=f"sh {script} {mount_path} {duration}",
            long_running=True,
            timeout=duration + 120,
        )
        log.info("file_ops_in_loop finished (rc=%s)", rc)
        return 0 if rc in (0, None) else 1
    except Exception as exc:
        log.error("file_ops_in_loop failed: %s", exc)
        return 1
    finally:
        if gpfs:
            cleanup_mount(gpfs["clients"], gpfs["nfs_mount"])
