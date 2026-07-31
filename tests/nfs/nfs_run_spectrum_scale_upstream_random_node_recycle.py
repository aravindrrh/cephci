"""
CES NFS stop/start + address move using discovered Scale node names.

Replaces hardcoded storage-scale-ces-00x hostnames from the old lab.
"""

import shlex

from cli.exceptions import OperationFailedError
from tests.nfs.lib.spectrum_scale_custom_cases import (
    cleanup_mount,
    discover_ces_nodes,
    merge_custom_config,
    prepare_mount,
    write_remote,
)
from utility.log import Log

log = Log(__name__)

MMFS = "/usr/lpp/mmfs/bin"

_RECYCLE_SH = r"""#!/bin/bash
# Injected: NODES_CSV DURATION
IFS=',' read -r -a nodes <<< "$NODES_CSV"
log_file="/var/log/nfs_restart.log"
last_node=""
PATH="/usr/lpp/mmfs/bin:$PATH"

if [ ${#nodes[@]} -lt 1 ]; then
  echo "ERROR: no CES nodes configured" | tee -a "$log_file"
  exit 1
fi

# Short CI durations use reduced waits so at least one stop/start fits.
if [ "$DURATION" -lt 900 ]; then
  WAIT_STOP=20
  WAIT_MOVE=30
  WAIT_START=30
  WAIT_NEXT=15
else
  WAIT_STOP=60
  WAIT_MOVE=180
  WAIT_START=180
  WAIT_NEXT=60
fi

END_TIME=$(($(date +%s) + DURATION))
echo "recycle: nodes=${nodes[*]} duration=$DURATION waits=$WAIT_STOP/$WAIT_MOVE/$WAIT_START" | tee -a "$log_file"

while [ $(date +%s) -lt $END_TIME ]; do
   random_node=${nodes[$RANDOM % ${#nodes[@]}]}

   if [[ "$random_node" == "$last_node" ]] && [ ${#nodes[@]} -gt 1 ]; then
       continue
   fi
   last_node="$random_node"

   {
       echo "=================================================================="
       echo "[$(date '+%Y-%m-%d %H:%M:%S')] [HOST: $(hostname)] Starting NFS recycle cycle"
       echo "Stopping NFS on $random_node..."
       mmces service stop nfs -N "$random_node" || true

       echo "Waiting ${WAIT_STOP}s..."
       sleep "$WAIT_STOP"

       # CES IP for the stopped node (last IPv4-looking field on matching row)
       IP=$(mmlscluster --ces 2>/dev/null | awk -v n="$random_node" '
         $0 ~ n {
           for (i=1;i<=NF;i++) if ($i ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/) ip=$i
           print ip
         }' | head -1)

       if [[ -z "$IP" ]]; then
           echo "WARN: no CES IP for $random_node; skip address move"
       elif [ ${#nodes[@]} -lt 2 ]; then
           echo "WARN: single CES node; skip address move for $IP"
       else
           remaining=()
           for n in "${nodes[@]}"; do
             [[ "$n" != "$random_node" ]] && remaining+=("$n")
           done
           failover_node=${remaining[$RANDOM % ${#remaining[@]}]}
           echo "Moving CES IP $IP to $failover_node..."
           mmces address move --ces-ip "$IP" --ces-node "$failover_node" || true
       fi

       echo "Waiting ${WAIT_MOVE}s..."
       sleep "$WAIT_MOVE"

       echo "Starting NFS on $random_node..."
       mmces service start nfs -N "$random_node" || true

       echo "Waiting ${WAIT_START}s..."
       sleep "$WAIT_START"

       echo "Rebalancing CES addresses..."
       mmces address move --rebalance || true

       echo "Waiting ${WAIT_NEXT}s before next cycle..."
       sleep "$WAIT_NEXT"
   } >> "$log_file" 2>&1
done
echo "recycle complete" | tee -a "$log_file"
"""


def run(ceph_cluster, **kw):
    conf = merge_custom_config(kw.get("config"))
    duration = int(conf.get("duration", 356))
    gpfs = None

    try:
        # Mount so IO path stays warm; recycle runs on installer (Scale CLI).
        gpfs = prepare_mount(ceph_cluster, conf)
        installer = gpfs["server"]
        nodes = discover_ces_nodes(installer, conf)
        if not nodes:
            raise OperationFailedError(
                "No CES nodes discovered. Set config ces_nodes: [host1, host2, ...] "
                "or ensure mmlscluster --ces works on the installer."
            )

        nodes_csv = ",".join(nodes)
        script_path = "/tmp/random_node_recycle.sh"
        write_remote(installer, script_path, _RECYCLE_SH)
        installer.exec_command(sudo=True, cmd=f"chmod +x {script_path}")

        # Soften sleep totals for short CI durations: script still works for 356+.
        env = f"NODES_CSV={shlex.quote(nodes_csv)} DURATION={duration}"
        rc = installer.exec_command(
            sudo=True,
            cmd=f"{env} bash {script_path}",
            long_running=True,
            timeout=max(duration + 600, 900),
        )
        log.info("node recycle finished (rc=%s)", rc)
        return 0 if rc in (0, None) else 1
    except Exception as exc:
        log.error("random_node_recycle failed: %s", exc)
        return 1
    finally:
        if gpfs:
            cleanup_mount(gpfs["clients"], gpfs["nfs_mount"])
