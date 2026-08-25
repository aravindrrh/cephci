import json
import re
from datetime import datetime
from threading import Thread
from time import sleep

import yaml

from ceph.waiter import WaitUntil
from cli.ceph.ceph import Ceph
from cli.cephadm.cephadm import CephAdm
from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.utils import check_coredump_generated, get_ip_from_node, reboot_node
from utility.log import Log

log = Log(__name__)

ceph_cluster_obj = None
setup_start_time = None


class NfsCleanupFailed(Exception):
    pass


def setup_nfs_cluster(
    clients,
    nfs_server,
    port,
    version,
    nfs_name,
    nfs_mount,
    fs_name,
    export,
    fs,
    ha=False,
    vip=None,
    ceph_cluster=None,
):
    installer_node = ceph_cluster.get_nodes("installer")[0]

    ganesha_conf = """NFS_CORE_PARAM {
    Enable_NLM = false;
    Enable_RQUOTA = false;
    Protocols = 3,4;
    mount_path_pseudo = true;
}

EXPORT_DEFAULTS {
    Access_Type = RW;
}
MDCACHE {
        Dir_Chunk = 0;
}
"""
    conf_template = """
EXPORT {
    Export_ID = %s;
    Path = "%s";
    Pseudo = "/export_%s";
    Protocols = 3,4;
    mount_path_pseudo = true;
    Transports = TCP;
    Access_Type = RW;
    Squash = None;
    FSAL {
        Name = "CEPH";
    }
}
"""

    # Step 3: Create export
    i = 0
    export_list = []
    for _ in clients:
        export_name = f"{export}_{i}"
        # Step 1: Check if the subvolume group is present.If not, create subvolume group
        cmd = "ceph fs subvolumegroup ls cephfs"
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        subvol_name = export_name.replace("/", "")
        if "[]" in out[0]:
            cmd = "ceph fs subvolumegroup create cephfs ganeshagroup"
            installer_node.exec_command(sudo=True, cmd=cmd)
            log.info("Subvolume group created successfully")

        # Step 2: Create subvolume (idempotent — keep static exports across chained tests)
        # Note: SSH exec_command is not a shell, so do not use `|| true` here.
        cmd = (
            f"ceph fs subvolume create cephfs {subvol_name} "
            f"--group_name ganeshagroup --namespace-isolated"
        )
        installer_node.exec_command(sudo=True, cmd=cmd, check_ec=False)

        # Get volume path
        cmd = (
            f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
        )
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        path = out[0].strip()
        ganesha_conf += conf_template % (100+i, path, i)
        i += 1
        export_list.append(export_name)
        sleep(1)
    # stop ganesha service
    pid = ""
    try:
        cmd = "pgrep ganesha"
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        print("PID : ", pid)
    except Exception:
        pass

    if pid:
        cmd = f"kill -9 {pid}"
        installer_node.exec_command(sudo=True, cmd=cmd)
    cmds = ["mkdir -p /var/run/ganesha",
           "chmod 755 /var/run/ganesha",
           "chown root:root /var/run/ganesha"]
    for cmd in cmds:
        installer_node.exec_command(cmd=cmd, sudo=True)
    # Update ganesha.conf file
    cmd = f"echo \"{ganesha_conf}\" > /etc/ganesha/ganesha.conf"
    installer_node.exec_command(sudo=True, cmd=cmd)
    ganesha_conf_file = "/etc/ganesha/ganesha.conf"
    with installer_node.remote_file(sudo=True, file_name=ganesha_conf_file, file_mode="w") as _f:
        _f.write(ganesha_conf)

    # Restart Ganesha
    cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
    installer_node.exec_command(sudo=True, cmd=cmd)

    # Check if ganesha service is up
    cmd = "pgrep ganesha"
    out = installer_node.exec_command(sudo=True, cmd=cmd)
    pid = out[0].strip()
    if not pid:
        raise OperationFailedError("Failed to restart nfs service")

    # Get the mount versions specific to clients
    mount_versions = _get_client_specific_mount_versions(version, clients)

    #  Perform nfs mount
    # Check if the mount version v3 is included in the list of versions and
    # if the mount version is v3, make necessary changes
    #if 3 in mount_versions.keys():
    #    ports_to_open = ["portmapper", "mountd"]
    #    open_mandatory_v3_ports(installer_node, ports_to_open)

    # If there are multiple nfs servers provided, only one is required for mounting
    if isinstance(nfs_server, list):
        nfs_server = nfs_server[0]
    if ha:
        nfs_server = vip.split("/")[0]  # Remove the port

    i = 0
    for version, clients in mount_versions.items():
        for client in clients:
            client.create_dirs(dir_path=nfs_mount, sudo=True)
            if Mount(client).nfs(
                mount=nfs_mount,
                version=version,
                port=port,
                server=installer_node.ip_address,
                export=f"/export_{i}",
            ):
                raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
            i += 1
            sleep(1)
    log.info("Mount succeeded on all clients")

    # Step 5: Enable nfs coredump to nfs nodes
    nfs_nodes = ceph_cluster.get_nodes("installer")
    Enable_nfs_coredump(nfs_nodes)


def restart_upstream_ganesha(installer_node):
    """
    Restart the manually built nfs-ganesha daemon on the installer node
    (same binary and config as setup_nfs_cluster).
    """
    pid = ""
    try:
        out = installer_node.exec_command(sudo=True, cmd="pgrep ganesha")
        pid = out[0].strip()
    except Exception:
        pass
    if pid:
        installer_node.exec_command(sudo=True, cmd=f"kill -9 {pid}")
    installer_node.exec_command(
        sudo=True,
        cmd=(
            "nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf "
            "-L /var/log/ganesha.log"
        ),
    )
    out = installer_node.exec_command(sudo=True, cmd="pgrep ganesha")
    pid = out[0].strip()
    if not pid:
        raise OperationFailedError("Failed to restart upstream nfs-ganesha")
    sleep(15)
    log.info("Upstream nfs-ganesha restarted on %s", installer_node.hostname)


def create_export(installer_node, nfs_export, squash="None"):
    conf_template = """
    EXPORT {
        Export_ID = %s;
        Path = "%s";
        Pseudo = "%s";
        Protocols = 3,4;
        mount_path_pseudo = true;
        Transports = TCP;
        Access_Type = RW;
        Squash = %s;
        FSAL {
            Name = "CEPH";
        }
    }"""
    # stop ganesha service
    pid = ""
    try:
        cmd = "pgrep ganesha"
        out = installer_node.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        print("PID : ", pid)
    except Exception:
        print("Ganesha process not running")

    if pid:
        cmd = f"kill -9 {pid}"
        installer_node.exec_command(sudo=True, cmd=cmd)

    subvol_name = nfs_export.replace("/", "")
    # Create subvolume
    cmd = f"ceph fs subvolume create cephfs {subvol_name} --group_name ganeshagroup --namespace-isolated"
    installer_node.exec_command(sudo=True, cmd=cmd)

    # Get volume path
    cmd = (
        f"ceph fs subvolume getpath cephfs {subvol_name} --group_name ganeshagroup"
    )
    out = installer_node.exec_command(sudo=True, cmd=cmd)
    path = out[0].strip()

    cmd = "cat /etc/ganesha/ganesha.conf | grep -o -P '(?<=Export_ID = ).*(?=;)' | tail -1"
    out = installer_node.exec_command(cmd=cmd, sudo=True)
    _id = out[0].strip()
    id = str(int(_id) + 1)
    ganesha_conf = conf_template % (id, path, nfs_export,squash)

    ganesha_conf_file = "/etc/ganesha/ganesha.conf"
    with installer_node.remote_file(sudo=True, file_name=ganesha_conf_file, file_mode="a") as _f:
        _f.write(ganesha_conf)

    # Restart Ganesha
    cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
    installer_node.exec_command(sudo=True, cmd=cmd)

    # Check if ganesha service is up
    cmd = "pgrep ganesha"
    out = installer_node.exec_command(sudo=True, cmd=cmd)
    pid = out[0].strip()
    if not pid:
        raise OperationFailedError("Failed to restart nfs service")


def _remove_export_block(conf_text, nfs_export):
    """Drop the EXPORT { ... } block whose Pseudo matches nfs_export.

    Leaves all other exports untouched. Handles nested braces (FSAL / CLIENT).
    Matches only a real EXPORT block — not EXPORT_DEFAULTS.
    """
    if isinstance(conf_text, bytes):
        conf_text = conf_text.decode("utf-8", errors="replace")

    lines = conf_text.splitlines(keepends=True)
    out = []
    i = 0
    # Exact EXPORT { opener — must not match EXPORT_DEFAULTS {
    export_start = re.compile(r"^\s*EXPORT\s*\{")
    while i < len(lines):
        if export_start.match(lines[i]):
            block = [lines[i]]
            depth = lines[i].count("{") - lines[i].count("}")
            i += 1
            while i < len(lines) and depth > 0:
                block.append(lines[i])
                depth += lines[i].count("{") - lines[i].count("}")
                i += 1
            block_text = "".join(block)
            # Match Pseudo = "/exportRO" (with optional spaces / quotes)
            if (
                f'Pseudo = "{nfs_export}"' in block_text
                or f"Pseudo = '{nfs_export}'" in block_text
                or f'Pseudo="{nfs_export}"' in block_text
            ):
                log.info(
                    "Removing EXPORT block for Pseudo %s from ganesha.conf", nfs_export
                )
                continue
            out.extend(block)
        else:
            out.append(lines[i])
            i += 1
    return "".join(out)


def delete_export(installer_node, nfs_export):
    """Remove one extra export created by create_export / Ceph.nfs.export.create(installer=...).

    Why: upstream Ganesha is conf-file based — `ceph nfs export delete` is a no-op here.
    This strips only the matching EXPORT block, restarts ganesha, and removes the
    CephFS subvolume. Static /export_N exports are left alone.
    """
    subvol_name = nfs_export.replace("/", "")
    ganesha_conf_file = "/etc/ganesha/ganesha.conf"

    # Stop ganesha before rewriting conf
    try:
        installer_node.exec_command(sudo=True, cmd="pkill -9 ganesha", check_ec=False)
    except Exception as exc:
        log.warning("Could not stop ganesha before delete_export: %s", exc)

    try:
        out = installer_node.exec_command(
            sudo=True, cmd=f"cat {ganesha_conf_file}", check_ec=False
        )
        conf_text = out[0] if out else ""
        new_conf = _remove_export_block(conf_text or "", nfs_export)
        with installer_node.remote_file(
            sudo=True, file_name=ganesha_conf_file, file_mode="w"
        ) as _f:
            _f.write(new_conf)
            _f.flush()
    except Exception as exc:
        log.warning("Failed to strip EXPORT %s from ganesha.conf: %s", nfs_export, exc)

    # Bring ganesha back with the updated conf
    try:
        installer_node.exec_command(
            sudo=True,
            cmd=(
                "nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf "
                "-L /var/log/ganesha.log"
            ),
            check_ec=False,
        )
        sleep(5)
    except Exception as exc:
        log.warning("Failed to restart ganesha after delete_export: %s", exc)

    # Drop the CephFS subvolume created alongside the export
    try:
        installer_node.exec_command(
            sudo=True,
            cmd=(
                f"ceph fs subvolume rm cephfs {subvol_name} "
                f"--group_name ganeshagroup --force"
            ),
            check_ec=False,
        )
        log.info("Removed CephFS subvolume %s (group ganeshagroup)", subvol_name)
    except Exception as exc:
        log.warning("Failed to remove subvolume %s: %s", subvol_name, exc)


def cleanup_export_mount(clients, mount_path):
    """Wipe files under an extra mount, unmount it, and remove the mount dir."""
    if not isinstance(clients, list):
        clients = [clients]
    for client in clients:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"find {mount_path} -mindepth 1 -delete",
                check_ec=False,
                long_running=True,
            )
        except Exception as exc:
            log.warning("Failed wiping %s on %s: %s", mount_path, client.hostname, exc)
        # Prefer lazy umount; ignore failures so cleanup always proceeds
        try:
            Unmount(client).unmount(mount_path)
        except Exception:
            try:
                client.exec_command(
                    sudo=True, cmd=f"umount -l {mount_path}", check_ec=False
                )
            except Exception as exc:
                log.warning(
                    "Failed unmounting %s on %s: %s", mount_path, client.hostname, exc
                )
        try:
            client.exec_command(sudo=True, cmd=f"rm -rf {mount_path}", check_ec=False)
        except Exception as exc:
            log.warning(
                "Failed removing mount dir %s on %s: %s",
                mount_path,
                client.hostname,
                exc,
            )


def wipe_export_contents(client, server_ip, export_pseudo, version="4.2", port="2049"):
    """Mount an export briefly, delete files inside it, then unmount.

    Used when a workload wrote into a static export (e.g. /export_1 via pynfs)
    that must stay defined for the next test.
    """
    tmp_mount = f"/mnt/nfs_wipe_{export_pseudo.strip('/').replace('/', '_')}"
    try:
        client.exec_command(sudo=True, cmd=f"mkdir -p {tmp_mount}", check_ec=False)
        client.exec_command(
            sudo=True,
            cmd=(
                f"mount -t nfs -o vers={version},port={port} "
                f"{server_ip}:{export_pseudo} {tmp_mount}"
            ),
            check_ec=False,
        )
        client.exec_command(
            sudo=True,
            cmd=f"find {tmp_mount} -mindepth 1 -delete",
            check_ec=False,
            long_running=True,
        )
    except Exception as exc:
        log.warning("wipe_export_contents failed for %s: %s", export_pseudo, exc)
    finally:
        try:
            client.exec_command(
                sudo=True,
                cmd=f"umount -l {tmp_mount}; rm -rf {tmp_mount}",
                check_ec=False,
            )
        except Exception:
            pass


def cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export):
    """
    Clean up client-side mount artefacts after an nfs operation.

    Steps:
        1. rm -rf of the content inside the mount folder --> rm -rf /mnt/nfs/*
        2. Unmount the volume
        3. rm -rf of the mount point

    Does NOT delete static Ganesha exports or the nfs "cluster". Extra exports
    created by a test must be removed via delete_export() in that test's finally.
    Args:
        clients (ceph): Client nodes
        nfs_mount (str): nfs mount path
        nfs_name (str): nfs cluster name (unused; kept for call-site compat)
        nfs_export (str): nfs export path (unused; kept for call-site compat)
    """
    if not isinstance(clients, list):
        clients = [clients]

    # Check nfs coredump
    if ceph_cluster_obj:
        nfs_nodes = ceph_cluster_obj.get_nodes("nfs")
        coredump_path = "/var/lib/systemd/coredump"
        for nfs_node in nfs_nodes:
            if check_coredump_generated(nfs_node, coredump_path, setup_start_time):
                raise NfsCleanupFailed(
                    "Coredump generated post execution of the current test case"
                )

    # Wait until the rm operation is complete
    timeout, interval = 600, 10
    for client in clients:
        # Clear the nfs_mount, at times rm operation can fail
        # as the dir is not empty, this being an expected behaviour,
        # the solution is to repeat the rm operation.
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                client.exec_command(
                    sudo=True, cmd=f"rm -rf {nfs_mount}/*", long_running=True
                )
                break
            except Exception as e:
                log.warning(f"rm operation failed, repeating!. Error {e}")
        if w.expired:
            raise NfsCleanupFailed(
                "Failed to cleanup nfs mount dir even after multiple iterations. Timed out!"
            )

        log.info("Unmounting nfs-ganesha mount on client:")
        sleep(3)
        if Unmount(client).unmount(nfs_mount):
            raise OperationFailedError(f"Failed to unmount nfs on {client.hostname}")
        log.info("Removing nfs-ganesha mount dir on client:")
        client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_mount}")
        sleep(3)


def _get_client_specific_mount_versions(versions, clients):
    # Identify the multi mount versions specific to clients
    version_dict = {}
    if not isinstance(versions, list):
        version_dict[versions] = clients
        return version_dict
    ctr = 0
    for entry in versions:
        ver = list(entry.keys())[0]
        count = list(entry.values())[0]
        version_dict[ver] = clients[ctr : ctr + int(count)]
        ctr = ctr + int(count)
    return version_dict


def perform_failover(nfs_nodes, failover_node, vip):
    # Trigger reboot on the failover node
    th = Thread(target=reboot_node, args=(failover_node,))
    th.start()

    # Validate any of the other nodes has got the VIP
    flag = False

    # Remove the port from vip
    if "/" in vip:
        vip = vip.split("/")[0]

    # Perform the check with a timeout of 60 seconds
    for w in WaitUntil(timeout=120, interval=5):
        for node in nfs_nodes:
            if node != failover_node:
                assigned_ips = get_ip_from_node(node)
                log.info(f"IP addrs assigned to node : {assigned_ips}")
                # If vip is assigned, set the flag and exit
                if vip in assigned_ips:
                    flag = True
                    log.info(f"Failover success, VIP reassigned to {node.hostname}")
        if flag:
            break
    if w.expired:
        raise OperationFailedError(
            "The failover process failed and vip is not assigned to the available nodes"
        )
    # Wait for the node to complete reboot
    th.join()


def Enable_nfs_coredump(nfs_nodes, conf_file="/etc/systemd/coredump.conf"):
    """nfs_coredump
    Args:
        nfs_nodes(obj): nfs server node
        conf_file: conf file path
    """
    if not isinstance(nfs_nodes, list):
        nfs_nodes = [nfs_nodes]

    for nfs_node in nfs_nodes:
        try:
            nfs_node.exec_command(
                sudo=True, cmd=f"echo Storage=external >> {conf_file}"
            )
            nfs_node.exec_command(
                sudo=True, cmd=f"echo DefaultLimitCORE=infinity >> {conf_file}"
            )
            nfs_node.exec_command(sudo=True, cmd="systemctl daemon-reexec")
        except Exception:
            raise OperationFailedError(f"failed enable coredump for {nfs_node}")


def get_nfs_pid_and_memory(nfs_nodes):
    """get nfs-ganesha pid and memory consumption(RSS)
    Args:
        nfs_nodes(obj): nfs server node
    Returns:
        nfs_server_info(dic): {"nfs server1": ["PID","RSS(MB)"], "nfs server2": ["PID","RSS(MB)"]}
    """
    nfs_server_info = {}
    if not isinstance(nfs_nodes, list):
        nfs_nodes = [nfs_nodes]

    for nfs_node in nfs_nodes:
        try:
            pid = nfs_node.exec_command(sudo=True, cmd="pgrep ganesha")[0].strip()
            rss = nfs_node.exec_command(sudo=True, cmd=f"ps -p {pid} -o rss=")[
                0
            ].strip()
            nfs_server_info[nfs_node.hostname] = [pid, rss]
        except Exception:
            raise OperationFailedError(
                f"failed get nfs process ID and rss for {nfs_node}"
            )
    return nfs_server_info


def permission(client, nfs_name, nfs_export, old_permission, new_permission):
    # Change export permissions to RO
    out = Ceph(client).nfs.export.get(nfs_name, f"{nfs_export}_0")
    client.exec_command(sudo=True, cmd=f"echo '{out}' > export.conf")
    client.exec_command(
        sudo=True,
        cmd=f'sed -i \'s/"access_type": "{old_permission}"/"access_type": "{new_permission}"/\' export.conf',
    )
    Ceph(client).nfs.export.apply(nfs_name, "export.conf")

    # Wait till the NFS daemons are up
    sleep(10)


def prepare_v3_lock_clients(clients):
    """Ensure NFSv3 lock manager dependencies are running on NFS clients."""
    if not isinstance(clients, list):
        clients = [clients]
    for client in clients:
        for svc in ("rpcbind", "rpc-statd"):
            client.exec_command(
                sudo=True,
                cmd=f"systemctl enable --now {svc}",
                check_ec=False,
            )


def enable_v3_locking(nfs_node):
    # stop ganesha service
    pid = ""
    try:
        cmd = "pgrep ganesha"
        out = nfs_node.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        print("PID : ", pid)
    except Exception:
        print("Ganesha process not running")

    if pid:
        cmd = f"kill -9 {pid}"
        nfs_node.exec_command(sudo=True, cmd=cmd)

    # enable NLOCKMGR ie., Enable_NLM = true; in ganesha.conf
    cmd = "sed -i 's/^\(\s*Enable_NLM\s*=\s*\)false;/    Enable_NLM = true;/I' /etc/ganesha/ganesha.conf"
    nfs_node.exec_command(sudo=True, cmd=cmd)

    for svc in ("rpcbind", "rpc-statd"):
        nfs_node.exec_command(
            sudo=True,
            cmd=f"systemctl enable --now {svc}",
            check_ec=False,
        )

    # Restart Ganesha
    cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
    nfs_node.exec_command(sudo=True, cmd=cmd)

    # Check if ganesha service is up
    cmd = "pgrep ganesha"
    out = nfs_node.exec_command(sudo=True, cmd=cmd)
    pid = out[0].strip()

    if not pid:
        raise OperationFailedError("Failed to restart nfs service")

    # nlockmgr must be reachable from clients once NLM is enabled
    sleep(5)
    try:
        open_mandatory_v3_ports(nfs_node, ["nlockmgr"])
    except Exception as exc:
        log.warning("Could not open nlockmgr firewall port: %s", exc)


def getfattr(client, file_path, attribute_name=None):
    # Fetch the extended attribute for file or directory
    """
    Args:
    attribute_name (str): Specific attribute name to retrieve. If None, retrieves all attributes.
    file_path (str): Path to the file/dir whose extended attribute is to be retrieved.
    """
    cmd = f"getfattr -d {file_path}"
    if attribute_name:
        cmd += " -n user.{attribute_name}"
    out = client.exec_command(sudo=True, cmd=cmd)
    log.info(out)
    return out


def setfattr(client, file_path, attribute_name, attribute_value):
    """
    Sets the value of an extended attribute on a file using setfattr command.

    Args:
    - file_path (str): Path to the file/dir where the extended attribute is to be set.
    - attribute_name (str): Name of the extended attribute to set.
    - attribute_value (str): Value to set for the extended attribute.
    """
    cmd = f"setfattr -n user.{attribute_name} -v {attribute_value} {file_path}"
    out = client.exec_command(sudo=True, cmd=cmd)
    return out


def removeattr(client, file_path, attribute_name):
    """
    Remove the value of an extended attribute on a file.

    Args:
    - file_path (str): Path to the file/dir where the extended attribute needs tp be removed.
    - attribute_name (str): Name of the extended attribute to be removed.
    """
    cmd = f"setfattr -x user.{attribute_name} {file_path}"
    out = client.exec_command(sudo=True, cmd=cmd)
    return out


def check_nfs_daemons_removed(client):
    """
    Check if NFS daemons are removed.
    Wait until there are no NFS daemons listed by 'ceph orch ls'.
    """
    while True:
        try:
            cmd = "ceph orch ls | grep nfs"
            out = client.exec_command(sudo=True, cmd=cmd)

            if out:
                print("NFS daemons are still present. Waiting...")
                sleep(10)  # Wait before checking again
            else:
                print("All NFS daemons have been removed.")
                break
        except Exception as e:
            print(f"Unexpected error: {e}")
            break


def open_mandatory_v3_ports(nfs_node, ports_to_open):
    """
    Open the required ports for v3 mount (portmapper, mountd, nlockmgr) based on rpcinfo output.
    """
    # Initialize the service_ports_mapping dictionary to store the port lists
    service_ports_mapping = {"portmapper": None, "mountd": None, "nlockmgr": None}

    # Execute rpcinfo command to get the port information
    cmd = "sudo rpcinfo -p"
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd)
    if not out:
        log.error(f"Failed to execute rpcinfo -p on {nfs_node}")
        return

    # Split the output into lines and iterate over them
    lines = out.splitlines()
    port_mapper_found = False  # Have the first portmapper port

    for line in lines:
        # Skip header and empty lines
        if "program vers proto port service" in line or not line.strip():
            continue

        # Split line into columns
        columns = line.split()
        port, service = columns[3], columns[4]

        # Check for relevant services
        if service == "portmapper" and not port_mapper_found:
            service_ports_mapping["portmapper"] = port
            port_mapper_found = True
        elif service == "mountd":
            service_ports_mapping["mountd"] = port
        elif service == "nlockmgr":
            service_ports_mapping["nlockmgr"] = port

    # Open firewall ports based on services in ports_to_open
    for service in ports_to_open:
        port_to_open = service_ports_mapping.get(service)

        if port_to_open:
            # Open the port using the firewall command
            nfs_node.exec_command(
                sudo=True,
                cmd=f"sudo firewall-cmd --zone=public --add-port={port_to_open}/tcp --permanent",
            )
            log.info(f"Opened {service} port: {port_to_open}")
        else:
            log.warning(f"{service} port not found or not needed.")

    # Reload the firewall to apply the changes
    nfs_node.exec_command(sudo=True, cmd="sudo firewall-cmd --reload")
    log.info("Firewall rules reloaded.")


def analyze_ganesha_cores(node, ganesha_exe="nfs-ganesha/build/ganesha.nfsd", filter_only_ganesha=True):
    """
    On the remote `node` (object exposing node.exec_command(sudo=True, cmd)),
    list all coredumps and run `gdb <ganesha_exe> <core> -ex bt` for each core found.

    Args:
        node: remote node object with method exec_command(sudo=True, cmd)
        ganesha_exe: path to the ganesha executable on the remote host (default /usr/bin/ganesha.nfsd)
        filter_only_ganesha: if True, only consider coredumps whose list line contains 'ganesha.nfsd'

    Returns:
        The return value of node.exec_command(...) so the caller can inspect stdout/stderr/rc as appropriate.
    """
    # Build a robust shell snippet to run on the remote host. It:
    # - extracts identifiers (2nd column from coredumpctl list)
    # - for each identifier obtains the Storage path via coredumpctl info <id>
    # - runs gdb --batch -ex bt <ganesha_exe> <core_path>
    # - prints markers so outputs are separable
    grepcmd = "| grep ganesha.nfsd " if filter_only_ganesha else ""
#     cmd = f"""bash -lc 'ids=$(coredumpctl list --no-legend {grepcmd}| awk "{{print $2}}" | sort -u);
# if [ -z "$ids" ]; then
#   echo "NO_COREDUMPS_FOUND";
#   exit 0;
# fi;
# for id in $ids; do
#   echo "===== COREDUMP ID: $id =====";
#   core=$(coredumpctl info "$id" 2>/dev/null | awk -F"Storage:" '/Storage:/ {gsub(/\(present\)/,""); print $2}' | xargs)
#   if [ -z "$core" ]; then
#     echo "NO_CORE_PATH for id:$id";
#     continue;
#   fi;
#   echo "CORE_PATH=$core";
#   echo "----- GDB BACKTRACE START for $core -----";
#   # run gdb; if it fails, print message but continue to next core
#   gdb --batch -ex bt {ganesha_exe} "$core" 2>&1 || echo "GDB_FAILED for $core";
#   echo "----- GDB BACKTRACE END for $core -----";
# done'"""

    cmd = f"""bash -lc '
ids=$(coredumpctl list --no-legend {grepcmd} | awk "{{print \\$2}}" | sort -u);

if [ -z "$ids" ]; then
  echo "NO_COREDUMPS_FOUND";
  exit 0;
fi;

for id in $ids; do
  echo "===== COREDUMP ID: $id =====";

  core=$(coredumpctl info "$id" 2>/dev/null | awk -F"Storage:" "/Storage:/ {{gsub(/\\(present\\)/,\\"\\"); print \\$2}}" | xargs);

  if [ -z "$core" ]; then
    echo "NO_CORE_PATH for id:$id";
    continue;
  fi;

  echo "CORE_PATH=$core";
  echo "----- GDB BACKTRACE START for $core -----";

  gdb --batch -ex bt "{ganesha_exe}" "$core" 2>&1 || echo "GDB_FAILED for $core";

  echo "----- GDB BACKTRACE END for $core -----";
done
'"""
    # Execute remotely with the provided node interface
    # return whatever node.exec_command returns so the caller can handle stdout/stderr
    out = node.exec_command(sudo=True, cmd=cmd)
    log.info("COREDUMP")
    log.info(out)
