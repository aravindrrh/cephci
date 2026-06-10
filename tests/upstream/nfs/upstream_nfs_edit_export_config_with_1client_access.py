from time import sleep

from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from cli.utilities.windows_utils import setup_windows_clients
from utility.log import Log

log = Log(__name__)


def update_export_conf(installer,
    client, nfs_name, nfs_export_client, original_clients_value, new_clients_values
):
    try:
        pid = ""
        try:
            cmd = "pgrep ganesha"
            out = installer.exec_command(sudo=True, cmd=cmd)
            pid = out[0].strip()
            print("PID : ", pid)
        except Exception:
            print("Ganesha process not running")

        if pid:
            cmd = f"kill -9 {pid}"
            installer.exec_command(sudo=True, cmd=cmd)

        ganesha_conf_file = "/etc/ganesha/ganesha.conf"
        client_ip = new_clients_values
        client_ip_sed = client_ip.replace(".", r"\.")
        # EXPORT_DEFAULTS grants RW when no CLIENT matches — remove it so only
        # the explicit CLIENT ACL applies. Use bare IP (no /32) with Access_Type
        # in the CLIENT block (verified on upstream Ganesha test beds).
        installer.exec_command(
            sudo=True,
            cmd=f"sed -i '/EXPORT_DEFAULTS {{/,/}}/d' {ganesha_conf_file}",
        )
        installer.exec_command(
            sudo=True,
            cmd=(
                f"sed -i 's|Clients *= *client_address|"
                f"Clients = {client_ip};|' {ganesha_conf_file}"
            ),
        )
        installer.exec_command(
            sudo=True,
            cmd=(
                f"sed -i '/Clients = {client_ip_sed};/a\\        Access_Type = RW;' "
                f"{ganesha_conf_file}"
            ),
        )
        # export.create adds Access_Type at EXPORT level; that overrides CLIENT ACL
        # matching. Drop it for this export only (Pseudo..Squash spans export-level
        # Access_Type but not the CLIENT block below FSAL).
        pseudo_escaped = nfs_export_client.replace("/", r"\/")
        installer.exec_command(
            sudo=True,
            cmd=(
                f"sed -i '/Pseudo = \"{pseudo_escaped}\";/,/Squash = None;/ "
                f"{{ /^[[:space:]]*Access_Type = RW;$/d; }}' {ganesha_conf_file}"
            ),
        )
        out = installer.exec_command(sudo=True, cmd=f"cat {ganesha_conf_file}")
        log.info("ganesha.conf after CLIENT ACL update:\n%s", out[0])
        cmd = f"nfs-ganesha/build/ganesha.nfsd -f /etc/ganesha/ganesha.conf -L /var/log/ganesha.log"
        installer.exec_command(sudo=True, cmd=cmd)

        # Check if ganesha service is up
        cmd = "pgrep ganesha"
        out = installer.exec_command(sudo=True, cmd=cmd)
        pid = out[0].strip()
        if not pid:
            raise OperationFailedError("Failed to restart nfs service")
        # Allow ganesha to finish loading exports before clients mount (see restart_upstream_ganesha)
        sleep(15)
    except Exception:
        raise OperationFailedError("failed to edit clients in export conf file")


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    no_servers = int(config.get("servers", "1"))
    if no_servers > len(nfs_nodes):
        raise ConfigError("The test requires more servers than available")
    servers = nfs_nodes[:no_servers]

    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))
    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    installer = ceph_cluster.get_nodes("installer")[0]
    nfs_server_name = installer.ip_address

    # Export Conf Parameter
    nfs_export_client = "/export_client_access"
    nfs_client_mount = "/mnt/nfs_client_mount"
    original_clients_value = "client_address"
    new_clients_values = clients[0].ip_address

    window_nfs_mount = "Z:"
    # nfs_server_name = [nfs_node.hostname for nfs_node in servers]
    ha = bool(config.get("ha", False))
    vip = config.get("vip", None)

    # Linux clients
    linux_clients = ceph_cluster.get_nodes("client")
    no_linux_clients = int(config.get("linux_clients", "1"))
    linux_clients = linux_clients[:no_linux_clients]
    if no_linux_clients > len(linux_clients):
        raise ConfigError("The test requires more linux clients than available")

    # Windows clients
    windows_clients = []
    is_windows = config.get("windows_clients", None)
    if is_windows:
        for windows_client_obj in setup_windows_clients(config.get("windows_clients")):
            windows_clients.append(windows_client_obj)
        if windows_clients:
            new_clients_values = windows_clients[0].ip_address

    try:
        # Setup nfs cluster
        setup_nfs_cluster(
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs_name,
            ha,
            vip,
            ceph_cluster=ceph_cluster,
        )

        # Create export
        Ceph(clients[0]).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=nfs_export_client,
            fs=fs_name,
            client_addr="client_address",
            installer=installer
        )

        # Edit the export config to mount with client 1 access value
        update_export_conf(installer,
            clients[0],
            nfs_name,
            nfs_export_client,
            original_clients_value,
            new_clients_values,
        )
        sleep(10)

        # Mount the export on client1 which is unauthorized.Mount should fail
        clients[1].create_dirs(dir_path=nfs_client_mount, sudo=True)
        cmd = (
            f"mount -t nfs -o vers={version},port={port} "
            f"{nfs_server_name}:{nfs_export_client} {nfs_client_mount}"
        )
        _, err, exit_code, _ = clients[1].exec_command(
            cmd=cmd, sudo=True, check_ec=False, verbose=True
        )

        if exit_code != 0 and "access denied by server" in f"{err or ''}":
            log.info("As expected, Mount on unauthorized client failed")
        else:
            log.error(
                "Mount unexpectedly succeeded on unauthorized client %s: exit=%s err=%r",
                clients[1].hostname,
                exit_code,
                err,
            )
            return 1

        sleep(15)

        # Mount the export on client0 which is authorized.Mount should pass
        if is_windows:
            cmd = f"mount {nfs_nodes[0].ip_address}:/export_1 {window_nfs_mount}"
            out = windows_clients[0].exec_command(cmd=cmd)
            if "is now successfully connected" not in out[0]:
                raise OperationFailedError(
                    f"Failed to mount nfs on {clients[0].ip_address}"
                )
            sleep(3)
        else:
            clients[0].create_dirs(dir_path=nfs_client_mount, sudo=True)
            if isinstance(nfs_server_name, list):
                nfs_server_name = nfs_server_name[0]
            if ha:
                nfs_server_name = vip.split("/")[0]  # Remove the port
            if Mount(clients[0]).nfs(
                mount=nfs_client_mount,
                version=version,
                port=port,
                server=nfs_server_name,
                export=nfs_export_client,
            ):
                raise OperationFailedError(
                    f"Failed to mount nfs on {clients[0].hostname}"
                )
        log.info("Mount succeeded on client0")

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        log.info("Cleaning up")
        if is_windows:
            for windows_client in windows_clients:
                cmd = f"del /q /f {window_nfs_mount}\\*.*"
                windows_client.exec_command(cmd=cmd)
                cmd = f"umount {window_nfs_mount}"
                windows_client.exec_command(cmd=cmd)

        # Cleaning up the client export and mount dir
        for client in clients[:2]:
            if Unmount(client).unmount(nfs_client_mount):
                raise OperationFailedError(
                    f"Failed to unmount nfs on {client.hostname}"
                )
            client.exec_command(sudo=True, cmd=f"rm -rf  {nfs_client_mount}")
        # Ceph(clients[0]).nfs.export.delete(nfs_name, nfs_export_client)

        # Cleaning up the remaining export and deleting the nfs cluster
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successfull")
    return 0
