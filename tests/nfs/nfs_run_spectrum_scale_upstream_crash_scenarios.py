from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify scale upstream with custom test scenarios
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]



    mount_path = "/mnt/nfsv4"
    EXPORT = "/ibm/scale_volume"

    # Test Scenario : 2
    test_file = """#!/bin/bash

if [ $# -ne 1 ]; then
   echo "Usage: $0 <directory-path>"
   exit 1
fi

DIR=$1
SCRIPT_START=$(date +%s)
PIDS=()
loop_counter=1

# Cleanup on exit
cleanup() {
   echo ""
   echo "Cleaning up..."
   for pid in "${PIDS[@]}"; do
       if kill -0 "$pid" 2>/dev/null; then
           kill -9 "$pid"
       fi
   done

   pkill -f "read_lookc_thr $DIR"
   pkill -f "write_lookc_thr $DIR"

   SCRIPT_END=$(date +%s)
   TOTAL_RUNTIME=$((SCRIPT_END - SCRIPT_START))
   echo "==== Script terminated ===="
   echo "Total script runtime: ${TOTAL_RUNTIME}s"
   exit 0
}

trap cleanup SIGINT SIGTERM

# Start initial read process
./read_lookc_thr "$DIR" > /dev/null 2>&1 &
PIDS+=($!)
echo "Started initial read_lookc_thr in background (PID: $!)"

while true; do
   LOOP_START=$(date +%s)

   echo "----- Loop $loop_counter -----"

   ./read_lookc_thr "$DIR" > /dev/null 2>&1 &
   PIDS+=($!)
   ./write_lookc_thr "$DIR" > /dev/null 2>&1 &
   PIDS+=($!)

   sleep 2

   if (( RANDOM % 2 == 0 )); then
       echo "Killing all read_lookc_thr processes..."
       pkill -f "read_lookc_thr $DIR"
   else
       echo "Killing all write_lookc_thr processes..."
       pkill -f "write_lookc_thr $DIR"
   fi

   LOOP_END=$(date +%s)
   SCRIPT_NOW=$(date +%s)
   LOOP_RUNTIME=$((LOOP_END - LOOP_START))
   SCRIPT_RUNTIME=$((SCRIPT_NOW - SCRIPT_START))

   echo "Loop $loop_counter runtime: ${LOOP_RUNTIME}s | Total script runtime: ${SCRIPT_RUNTIME}s"
   echo ""

   ((loop_counter++))
   sleep 1
done
"""
    cmd = f"touch {mount_path}/start_test.sh"
    server.exec_command(cmd=cmd, sudo=True, long_running=True)
    with server.remote_file(sudo=True, file_name=f"{mount_path}/start_test.sh", file_mode="w") as _f:
        _f.write(test_file)

    test_file = """#!/bin/bash

# List of test directories
DIRS=(
   "/mnt/scale/exp1_mt1"
   "/mnt/scale/exp1_mt2"
   "/mnt/scale/exp1_mt3"
)

PIDS=()
HOSTNAME=$(hostname)

# Create logs directory if not exists
mkdir -p logs

# Cleanup on exit
cleanup() {
   echo ""
   echo "Cleaning up all start_test.sh instances..."

   for pid in "${PIDS[@]}"; do
       if kill -0 "$pid" 2>/dev/null; then
           echo "Killing PID $pid"
           kill -SIGTERM "$pid"
       fi
   done

   wait
   echo "All test processes terminated."
   exit 0
}

trap cleanup SIGINT SIGTERM

# Start start_test.sh for each directory, with output redirected to logfile
for dir in "${DIRS[@]}"; do
   dir_suffix=$(basename "$dir")
   log_file="logs/${HOSTNAME}_start_test_${dir_suffix}.log"
   echo "Starting test for directory: $dir (log: $log_file)"
   ./start_test.sh "$dir" > "$log_file" 2>&1 &
   PIDS+=($!)
   sleep 1
done

echo "All tests started. Press Ctrl+C to stop."

# Keep script running to allow trap to catch signal
while true; do
   sleep 1
done"""
    cmd = f"touch {mount_path}/run_all_tests.sh"
    server.exec_command(cmd=cmd, sudo=True, long_running=True)
    with server.remote_file(sudo=True, file_name=f"{mount_path}/run_all_tests.sh", file_mode="w") as _f:
        _f.write(test_file)

    with server.remote_file(sudo=True, file_name=f"{mount_path}/run_all_tests.sh", file_mode="w") as _f:
        _f.write(test_file)
    out = server.exec_command(cmd=f"sh {mount_path}/run_all_tests.sh {mount_path}", sudo=True, long_running=True)
    log.info(out)
    return 0
