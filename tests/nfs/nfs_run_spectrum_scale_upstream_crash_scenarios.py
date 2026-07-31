"""
Kill lock-stress processes under load (client-side), not Ganesha crashes.

Builds read/write lock binaries, runs them under mount subdirs, randomly
pkills readers or writers for *duration* seconds.
"""

import shlex

from tests.nfs.lib.spectrum_scale_custom_cases import (
    build_lock_binaries,
    cleanup_mount,
    merge_custom_config,
    prepare_mount,
    write_remote,
)
from utility.log import Log

log = Log(__name__)

_START_TEST_SH = r"""#!/bin/bash
# Usage: start_test.sh <directory> <read_bin> <write_bin>
DIR=$1
READ_BIN=$2
WRITE_BIN=$3
SCRIPT_START=$(date +%s)
PIDS=()
loop_counter=1

cleanup() {
   for pid in "${PIDS[@]}"; do
       kill -9 "$pid" 2>/dev/null || true
   done
   pkill -f "$READ_BIN $DIR" 2>/dev/null || true
   pkill -f "$WRITE_BIN $DIR" 2>/dev/null || true
   exit 0
}
trap cleanup SIGINT SIGTERM

"$READ_BIN" "$DIR" 3600 > /dev/null 2>&1 &
PIDS+=($!)

while true; do
   "$READ_BIN" "$DIR" 3600 > /dev/null 2>&1 &
   PIDS+=($!)
   "$WRITE_BIN" "$DIR" 3600 > /dev/null 2>&1 &
   PIDS+=($!)
   sleep 2
   if (( RANDOM % 2 == 0 )); then
       pkill -f "$READ_BIN $DIR" 2>/dev/null || true
   else
       pkill -f "$WRITE_BIN $DIR" 2>/dev/null || true
   fi
   ((loop_counter++))
   sleep 1
done
"""

_RUN_ALL_SH = r"""#!/bin/bash
# Usage: run_all_tests.sh <duration> <start_script> <read_bin> <write_bin> <dir1> [dir2 ...]
DURATION=$1
START_SH=$2
READ_BIN=$3
WRITE_BIN=$4
shift 4
DIRS=("$@")
PIDS=()
HOSTNAME=$(hostname)
mkdir -p /tmp/nfs_custom_logs

cleanup() {
   for pid in "${PIDS[@]}"; do
       kill -TERM "$pid" 2>/dev/null || true
   done
   wait 2>/dev/null || true
   exit 0
}
trap cleanup SIGINT SIGTERM

for dir in "${DIRS[@]}"; do
   dir_suffix=$(basename "$dir")
   log_file="/tmp/nfs_custom_logs/${HOSTNAME}_start_test_${dir_suffix}.log"
   echo "Starting test for $dir (log: $log_file)"
   bash "$START_SH" "$dir" "$READ_BIN" "$WRITE_BIN" > "$log_file" 2>&1 &
   PIDS+=($!)
   sleep 1
done

END_TIME=$(($(date +%s) + DURATION))
while [ $(date +%s) -lt $END_TIME ]; do
   sleep 1
done
echo "Duration elapsed. Cleaning up..."
cleanup
"""


def run(ceph_cluster, **kw):
    conf = merge_custom_config(kw.get("config"))
    duration = int(conf.get("duration", 356))
    gpfs = None

    try:
        gpfs = prepare_mount(ceph_cluster, conf)
        client = gpfs["clients"][0]
        mount_path = gpfs["nfs_mount"]

        read_bin, write_bin = build_lock_binaries(client)
        subdirs = [
            f"{mount_path}/exp1_mt1",
            f"{mount_path}/exp1_mt2",
            f"{mount_path}/exp1_mt3",
        ]
        for d in subdirs:
            client.exec_command(sudo=True, cmd=f"mkdir -p {shlex.quote(d)}")
            client.exec_command(
                sudo=True, cmd=f"echo seed > {shlex.quote(d)}/testfile.txt"
            )

        work = "/tmp/nfs_custom_locks"
        start_sh = f"{work}/start_test.sh"
        run_all = f"{work}/run_all_tests.sh"
        write_remote(client, start_sh, _START_TEST_SH)
        write_remote(client, run_all, _RUN_ALL_SH)
        client.exec_command(sudo=True, cmd=f"chmod +x {start_sh} {run_all}")

        dirs_arg = " ".join(shlex.quote(d) for d in subdirs)
        cmd = (
            f"bash {run_all} {duration} {start_sh} {read_bin} {write_bin} {dirs_arg}"
        )
        rc = client.exec_command(
            sudo=True,
            cmd=cmd,
            long_running=True,
            timeout=duration + 180,
        )
        log.info("crash/lock-kill scenarios finished (rc=%s)", rc)
        return 0 if rc in (0, None) else 1
    except Exception as exc:
        log.error("crash_scenarios failed: %s", exc)
        return 1
    finally:
        if gpfs:
            cleanup_mount(gpfs["clients"], gpfs["nfs_mount"])
