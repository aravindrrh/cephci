from threading import Thread
from time import sleep

from upstream_nfs_operations import (
    cleanup_cluster,
    cleanup_export_mount,
    create_export,
    delete_export,
    enable_v3_locking,
    prepare_v3_lock_clients,
    setup_nfs_cluster,
)

from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount
from utility.log import Log

log = Log(__name__)


def _is_lock_contention_error(exc):
    """True when flock failed because another client holds the lock."""
    err = str(exc)
    return "Errno 11" in err or "Resource temporarily unavailable" in err


def get_file_lock(client, file_path="/mnt/nfs_squash/sample_file", hold_seconds=30):
    """
    Gets the file lock on the file with root_squash enabled
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


def try_acquire_file_lock(client, file_path="/mnt/nfs_squash/sample_file"):
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
    nfs_server_name = nfs_node.hostname
    installer = ceph_cluster.get_nodes("installer")[0]

    # Squashed export parameters
    nfs_export_squash = "/export_squash"
    nfs_squash_mount = "/mnt/nfs_squash"

    try:
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
            ceph_cluster=ceph_cluster,
        )

        # Create rootsquash export via ganesha.conf (upstream manual ganesha path)
        create_export(installer, nfs_export_squash, squash="rootsquash")

        if version == 3:
            enable_v3_locking(installer)
            prepare_v3_lock_clients(clients[:2])
            sleep(5)

        # Mount the squashed export on client 1 and 2
        for client in clients[:2]:
            client.create_dirs(dir_path=nfs_squash_mount, sudo=True)

        for client in clients[:2]:
            if Mount(client).nfs(
                mount=nfs_squash_mount,
                version=version,
                port=port,
                server=installer.ip_address,
                export=nfs_export_squash,
            ):
                raise OperationFailedError(f"Failed to mount nfs on {client.hostname}")
            client.exec_command(sudo=True, cmd=f"chmod 777 {nfs_squash_mount}/")
        log.info("Mount succeeded on client")

    except Exception as e:
        log.error(f"Failed to setup nfs cluster with rootsquash enabled : Error - {e}")
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        return 1

    file_path = f"{nfs_squash_mount}/sample_file"
    rc = 1
    try:
        # Create file on squashed dir (drop stale file/locks from prior runs)
        clients[0].exec_command(
            sudo=True,
            cmd=f"rm -f {file_path}",
            check_ec=False,
        )
        clients[0].exec_command(
            sudo=True,
            cmd=f"touch {file_path}",
        )

        # Perform File Lock from client 1
        c1 = Thread(target=get_file_lock, args=(clients[0],))
        c1.start()

        # Adding a constant sleep as it is required for the thread call to start the lock process
        sleep(2)
        rc = 0
        try:
            get_file_lock(clients[1])
            log.error(
                "Unexpected: Client 2 was able to access file lock while client 1 lock was active"
            )
            rc = 1
        except Exception as e:
            if _is_lock_contention_error(e):
                log.info(
                    f"Expected: Failed to acquire lock from client 2 while client 1 lock is in on {e}"
                )
            else:
                log.error(
                    f"Unexpected lock error during contention (expected EAGAIN/EWOULDBLOCK, not ENOLCK): {e}"
                )
                rc = 1

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
    except Exception as e:
        log.error(f"Failed file lock test on rootsquash export. Error: {e}")
        rc = 1
    finally:
        log.info("Cleaning up")
        # Extra squash export/mount — remove only what this test created
        try:
            cleanup_export_mount(clients[:2], nfs_squash_mount)
            delete_export(installer, nfs_export_squash)
            cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export)
        except Exception as exc:
            log.warning("file_lock_root_squash cleanup failed: %s", exc)
        log.info("Cleaning up successfull")
    return rc
