from nfs_operations import cleanup_cluster, getfattr, setfattr

from cli.exceptions import ConfigError
from spectrumscale.spectrum_scale_nfs_helpers import (
    resolve_nfs_service_nodes,
    setup_nfs_cluster_or_scale,
)
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify the basic getfattr and setfattr
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    _, nfs_server_name = resolve_nfs_service_nodes(ceph_cluster, config)
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
    no_clients = int(config.get("clients", "2"))
    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    filename = "Testfile"

    try:
        # Setup nfs cluster
        setup_nfs_cluster_or_scale(
            ceph_cluster,
            clients,
            nfs_server_name,
            port,
            version,
            nfs_name,
            nfs_mount,
            fs_name,
            nfs_export,
            fs,
            config=config,
        )

        # Create a file on Mount point
        cmd = f"touch {nfs_mount}/{filename}"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Set the extended attribute of the file
        setfattr(
            client=clients[0],
            file_path=f"{nfs_mount}/{filename}",
            attribute_name="myattr",
            attribute_value="value",
        )

        # Fetch the extended attribute of the file
        out = getfattr(client=clients[0], file_path=f"{nfs_mount}/{filename}")

        # Extract attribute name and value from the output
        for item in out:
            lines = item.splitlines()
            attr_name = lines[1].split(".")[1].split("=")[0]
            attr_value = lines[1].split("=")[1].strip('"')
            log.info(f"Attribute Name: {attr_name}")
            log.info(f"Attribute Value: {attr_value}")
            if attr_name == "myattr" and attr_value == "value":
                log.info(
                    "Validated :Attribute 'myattr' is set to 'value' in the output."
                )
                break
            else:
                log.info("Attribute 'myattr' set to 'value' not found in the output.")
                return 1

    except Exception as e:
        log.error(f"Failed to perform export client addr validation : {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
        return 1

    finally:
        log.info("Cleaning up")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        log.info("Cleaning up successful")
    return 0
