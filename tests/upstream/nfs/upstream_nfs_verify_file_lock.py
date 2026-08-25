from threading import Thread
from time import sleep

from upstream_nfs_operations import (
    cleanup_cluster,
    cleanup_export_mount,
    create_export,
    delete_export,
    enable_v3_locking,
    setup_nfs_cluster,
    analyze_ganesha_cores,
)

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount
from utility.log import Log

log = Log(__name__)


def get_file_lock(client, file_path="/mnt/nfs_lock_mount/sample_file", hold_seconds=30):
    """
    Gets the file lock on the file
    Args:
        client (ceph): Ceph client node
        file_path (str): Path to the file to lock
        hold_seconds (int): How long to hold the lock before releasing
    """
    cmd = (
        "python3 -c 'from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN;"
        "from time import sleep;"
        f'f = open("{file_path}", "w");'
        "flock(f.fileno(), LOCK_EX | LOCK_NB);"
        f"sleep({hold_seconds});"
        "flock(f.fileno(), LOCK_UN)'"
    )
    client.exec_command(cmd=cmd, sudo=True)


def try_acquire_file_lock(client, file_path="/mnt/nfs_lock_mount/sample_file"):
    """Acquire and immediately release a lock; used after client 1 has released."""
    cmd = (
        "python3 -c 'from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN;"
        f'f = open("{file_path}", "w");'
        "flock(f.fileno(), LOCK_EX | LOCK_NB);"
        "flock(f.fileno(), LOCK_UN)'"
    )
    client.exec_command(cmd=cmd, sudo=True)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.2")
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
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname
    installer = ceph_cluster.get_nodes("installer")[0]
    nfs_lock_mount = "/mnt/nfs_lock_mount"
    nfs_lock_export = "/nfs_lock_export"
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
            fs,
            ceph_cluster=ceph_cluster,
        )
    except Exception as e:
        log.error(f"Failed to setup nfs cluster {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1

    # Create export for locking test
    create_export(installer, nfs_lock_export)

    # Enable NLM and restart Ganesha before mounting — remounting after a
    # ganesha kill leaves stale handles and breaks v3 flock (ENOLCK).
    if version == 3:
        enable_v3_locking(installer)
        sleep(5)

    rc = 1
    try:
        # Mount the export on 2 clients in parallel
        for client in clients[:2]:
            client.create_dirs(dir_path=nfs_lock_mount, sudo=True)
            if Mount(client).nfs(
                mount=nfs_lock_mount,
                version=version,
                port=port,
                server=installer.ip_address,
                export=nfs_lock_export,
                other_opts="local_lock=posix",
            ):
                raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
        log.info("Mount succeeded on client")

        # Create a file on Client 1 (drop stale file/locks from prior runs)
        file_path = f"{nfs_lock_mount}/sample_file"
        clients[0].exec_command(cmd=f"rm -f {file_path}", sudo=True, check_ec=False)
        clients[0].exec_command(cmd=f"touch {file_path}", sudo=True)

        # Perform File Lock from client 1
        c1 = Thread(target=get_file_lock, args=(clients[0],))
        c1.start()

        # Adding a constant sleep as its required for the thread call to start the lock process
        sleep(5)
        rc = 0
        try:
            get_file_lock(clients[1])
            log.error(
                "Unexpected: Client 2 was able to access file lock while client 1 lock was active"
            )
            rc = 1
        except Exception as e:
            log.info(
                f"Expected: Failed to acquire lock from client 2 while client 1 lock is in on {e}"
            )

        c1.join()

        if rc == 0:
            # Allow Ganesha to propagate lock release before client 2 retries
            sleep(5)
            try:
                try_acquire_file_lock(clients[1], file_path)
                log.info(
                    "Expected: Successfully acquired lock from client 2 while client 1 lock is released"
                )
            except Exception as e:
                log.error(
                    f"Unexpected: Failed to acquire lock from client 2 while client 1 lock is in removed {e}"
                )
                rc = 1
    finally:
        # Extra lock export/mount — remove only what this test created
        try:
            cleanup_export_mount(clients[:2], nfs_lock_mount)
            delete_export(installer, nfs_lock_export)
            cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
            analyze_ganesha_cores(nfs_node)
        except Exception as exc:
            log.warning("file_lock cleanup failed: %s", exc)
    return rc
