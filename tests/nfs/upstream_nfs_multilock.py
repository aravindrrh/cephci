"""
Multilock (nfs-ganesha src/tools/multilock) against NFS.

- **Two client nodes** (installer + 2× client): ml_console on the **installer**;
  ml_posix_client c1/c2 on the two clients. multilock_control_ip defaults to the
  installer. NFS/Ganesha stays on the installer.

"""
from threading import Thread
from time import sleep

from cli.exceptions import OperationFailedError
from cli.utilities.filesys import Mount
from nfs_operations import setup_nfs_cluster

from utility.log import Log

log = Log(__name__)

GANESHA_REPO = "https://github.com/nfs-ganesha/nfs-ganesha.git"
CLONE_ROOT = "/root/nfs-ganesha"
MULTILOCK_BUILD = f"{CLONE_ROOT}/src/tools/multilock/build"

_SCALE_SERVER_CMDS = [
    "rm -rf ci-tests/",
    "yum install -y git wget",
    "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
    "sh ci-tests/build_scripts/common/basic-storage-scale.sh",
]


def _spectrum_scale_server_setup(server):
    """Same server-side Scale / Ganesha prep as upstream_nfs_cthon."""
    for cmd in _SCALE_SERVER_CMDS:
        exit_code = server.exec_command(
            cmd=cmd, sudo=True, long_running=True, timeout=5400
        )
        if exit_code != 0:
            raise OperationFailedError(
                f"Spectrum Scale server command failed (exit {exit_code}): {cmd}"
            )


def _spectrum_mount_posix_clients(
    client_c1, client_c2, server, nfs_mount, version, nfs_port, export_path
):
    """Mount the shared Scale export on both POSIX clients (lock-test style)."""
    mount_opts = f"vers={version},port={nfs_port}"
    for node in (client_c1, client_c2):
        cmds = [
            "yum install -y nfs-utils || dnf -y install nfs-utils",
            f"mkdir -p {nfs_mount}",
            f"umount -l {nfs_mount} 2>/dev/null || true",
            (
                f"mount -t nfs -o {mount_opts} "
                f"{server.ip_address}:{export_path} {nfs_mount}"
            ),
        ]
        for cmd in cmds:
            node.exec_command(cmd=cmd, sudo=True, long_running=True)


def _build_multilock_on_node(node):
    """Clone nfs-ganesha (if needed), build multilock tools, verify binaries."""
    cmds = (
        f"(dnf install -y cmake gcc gcc-c++ make git || "
        f"yum install -y cmake gcc gcc-c++ make git) && "
        f"(test -d {CLONE_ROOT}/.git || "
        f"(rm -rf {CLONE_ROOT} && git clone --depth 1 {GANESHA_REPO} {CLONE_ROOT})) && "
        f"cd {CLONE_ROOT}/src/tools/multilock && rm -rf build && mkdir -p build && "
        f"cd build && cmake .. && make && "
        f"test -x ./ml_console && test -x ./ml_posix_client && echo MULTILOCK_BUILD_OK"
    )
    out, err = node.exec_command(
        cmd=cmds,
        sudo=True,
        long_running=False,
        timeout=600,
        check_ec=True,
    )
    log.info(f"multilock build on {node.hostname}: {out[-500:] if out else ''}")
    if err:
        log.info(f"multilock build stderr: {err[-500:] if err else ''}")
    combined = (out or "") + (err or "")
    if "MULTILOCK_BUILD_OK" not in combined:
        raise RuntimeError(
            f"multilock build or binary check failed on {node.hostname}: {out} {err}"
        )


def _cleanup_posix_mounts(posix_nodes, nfs_mount):
    """Clear mount contents, lazy-umount NFS, remove mount directory."""
    for node in posix_nodes:
        host = node.hostname
        try:
            node.exec_command(
                cmd=f"bash -lc 'sync; rm -rf {nfs_mount}/* 2>/dev/null; true'",
                sudo=True,
                check_ec=False,
            )
        except Exception as exc:
            log.warning("multilock cleanup rm %s on %s: %s", nfs_mount, host, exc)
        sleep(2)
        try:
            node.exec_command(
                cmd=f"umount -l {nfs_mount} 2>/dev/null || true",
                sudo=True,
                check_ec=False,
            )
        except Exception as exc:
            log.warning("multilock cleanup umount %s on %s: %s", nfs_mount, host, exc)
        sleep(2)
        try:
            node.exec_command(
                cmd=f"rm -rf {nfs_mount}",
                sudo=True,
                check_ec=False,
            )
        except Exception as exc:
            log.warning("multilock cleanup rmdir %s on %s: %s", nfs_mount, host, exc)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    spectrum_scale = config.get("spectrum_scale", False)
    nfs_mount = config.get("mount_point", "/mnt/multilock_test")
    multilock_coord_port = str(config.get("multilock_port", "8000"))
    nfs_port = str(config.get("nfs_port", config.get("port", "2049")))
    raw_versions = config.get("nfs_versions")
    if raw_versions is None:
        nfs_versions = [str(config.get("nfs_version", "4.0"))]
    elif isinstance(raw_versions, (list, tuple)):
        nfs_versions = [str(v) for v in raw_versions]
    else:
        nfs_versions = [str(raw_versions)]
    multilock_test = config.get(
        "multilock_sample_test", "../sample_tests/split_lock"
    )
    client_timeout = config.get("multilock_client_timeout", 10400)
    controller_startup_delay = config.get("multilock_controller_delay", 10)
    scale_export = config.get("nfs_export", "/ibm/scale_volume")

    iterations = int(config.get("multilock_iterations", config.get("iterations", 1)))
    iterations = max(1, iterations)
    do_cleanup = config.get("cleanup", True)

    clients = ceph_cluster.get_nodes("client")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    installer_node = nfs_nodes[0]

    if len(clients) < 2:
        log.error("multilock requires at least two client nodes (c1 and c2)")
        return 1

    if len(clients) == 2:
        controller_client = installer_node
        client_c1 = clients[0]
        client_c2 = clients[1]
        default_control_ip = installer_node.ip_address
        log.info(
            "Multilock layout: 2 clients — ml_console on installer %s; "
            "c1=%s c2=%s",
            installer_node.hostname,
            client_c1.hostname,
            client_c2.hostname,
        )
    else:
        controller_client = clients[0]
        client_c1 = clients[1]
        client_c2 = clients[2]
        default_control_ip = controller_client.ip_address
        log.info(
            "Multilock layout: 3+ clients — ml_console on %s; c1=%s c2=%s",
            controller_client.hostname,
            client_c1.hostname,
            client_c2.hostname,
        )

    nfs_server_name = nfs_node.hostname
    multilock_control_ip = config.get("multilock_controller_ip") or default_control_ip

    shared_export = "nfs_0"
    posix_nodes = [client_c1, client_c2]
    build_nodes = [controller_client, client_c1, client_c2]

    try:
        spectrum_server_ready = False
        overall_rc = 0

        for vi, version in enumerate(nfs_versions):
            log.info(
                "=== multilock NFS version %s/%s (mount vers=%s) ===",
                vi + 1,
                len(nfs_versions),
                version,
            )
            try:
                controller_client.exec_command(
                    cmd="pkill -f '[m]l_console' || pkill -f ml_console || true",
                    sudo=True,
                    long_running=False,
                    timeout=30,
                    check_ec=False,
                )
            except Exception:
                pass
            sleep(1)

            if vi > 0:
                _cleanup_posix_mounts(posix_nodes, nfs_mount)
                sleep(2)

            if spectrum_scale:
                if not spectrum_server_ready:
                    log.info(
                        "Spectrum Scale: server prep, then mount %s on %s and %s",
                        scale_export,
                        client_c1.hostname,
                        client_c2.hostname,
                    )
                    _spectrum_scale_server_setup(installer_node)
                    spectrum_server_ready = True
                _spectrum_mount_posix_clients(
                    client_c1,
                    client_c2,
                    installer_node,
                    nfs_mount,
                    version,
                    nfs_port,
                    scale_export,
                )
            else:
                if vi == 0:
                    log.info(
                        "Ceph NFS: export %s on posix clients %s and %s (vers=%s)",
                        shared_export,
                        client_c1.hostname,
                        client_c2.hostname,
                        version,
                    )
                    setup_nfs_cluster(
                        [client_c1],
                        nfs_server_name,
                        nfs_port,
                        version,
                        config.get("nfs_name", "cephfs-nfs"),
                        nfs_mount,
                        config.get("fs_name", "cephfs"),
                        config.get("nfs_export", "/export"),
                        config.get("fs", "cephfs"),
                        ceph_cluster=ceph_cluster,
                    )
                    client_c2.create_dirs(dir_path=nfs_mount, sudo=True)
                    Mount(client_c2).nfs(
                        mount=nfs_mount,
                        version=version,
                        port=nfs_port,
                        server=installer_node.ip_address,
                        export=shared_export,
                    )
                else:
                    log.info(
                        "Ceph NFS: remount %s on both clients with vers=%s",
                        nfs_mount,
                        version,
                    )
                    for c in posix_nodes:
                        c.create_dirs(dir_path=nfs_mount, sudo=True)
                        Mount(c).nfs(
                            mount=nfs_mount,
                            version=version,
                            port=nfs_port,
                            server=installer_node.ip_address,
                            export=shared_export,
                        )

            if vi == 0:
                for node in build_nodes:
                    log.info(f"Building multilock on {node.hostname}")
                    _build_multilock_on_node(node)

            for it in range(1, iterations + 1):
                log.info(
                    "Multilock iteration %s/%s (vers=%s, port %s, test %s)",
                    it,
                    iterations,
                    version,
                    multilock_coord_port,
                    multilock_test,
                )
                try:
                    controller_client.exec_command(
                        cmd="pkill -f '[m]l_console' || pkill -f ml_console || true",
                        sudo=True,
                        long_running=False,
                        timeout=30,
                        check_ec=False,
                    )
                except Exception:
                    pass
                sleep(2)

                log.info(
                    "Starting multilock controller on %s (port %s)",
                    controller_client.hostname,
                    multilock_coord_port,
                )
                start_cmd = (
                    f"bash -lc 'cd {MULTILOCK_BUILD} && "
                    f"nohup ./ml_console -p {multilock_coord_port} -x {multilock_test} "
                    f">/root/multilock_controller.log 2>&1 </dev/null &'"
                )
                _ctrl_ssh_timeout = int(
                    config.get("multilock_controller_ssh_timeout", 30)
                )
                controller_client.exec_command(
                    cmd=start_cmd,
                    sudo=True,
                    long_running=False,
                    timeout=_ctrl_ssh_timeout,
                    check_ec=False,
                )
                log.info(
                    "Waiting %s s for ml_console to listen on port %s",
                    controller_startup_delay,
                    multilock_coord_port,
                )
                sleep(controller_startup_delay)
                log.info(
                    "Starting ml_posix_client on c1 (%s) and c2 (%s)",
                    client_c1.hostname,
                    client_c2.hostname,
                )

                client_errors = []

                def _posix_worker(node, client_name, log_path):
                    try:
                        log.info(
                            "ml_posix_client %s on %s (log %s)",
                            client_name,
                            node.hostname,
                            log_path,
                        )
                        cmd = (
                            f"bash -lc 'cd {MULTILOCK_BUILD} && "
                            f"./ml_posix_client -s {multilock_control_ip} "
                            f"-p {multilock_coord_port} "
                            f"-n {client_name} -c {nfs_mount} >{log_path} 2>&1 </dev/null'"
                        )
                        node.exec_command(
                            cmd=cmd,
                            sudo=True,
                            long_running=True,
                            timeout=client_timeout,
                            check_ec=True,
                        )
                    except Exception as exc:
                        client_errors.append(exc)
                        log.error(f"ml_posix_client ({client_name}) failed: {exc}")

                threads = [
                    Thread(
                        target=_posix_worker,
                        args=(client_c1, "c1", "/root/multilock_client1.log"),
                    ),
                    Thread(
                        target=_posix_worker,
                        args=(client_c2, "c2", "/root/multilock_client2.log"),
                    ),
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                if client_errors:
                    for path, node in (
                        ("/root/multilock_client1.log", client_c1),
                        ("/root/multilock_client2.log", client_c2),
                        ("/root/multilock_controller.log", controller_client),
                    ):
                        try:
                            tail, _ = node.exec_command(
                                cmd=f"tail -n 80 {path}", sudo=True, check_ec=False
                            )
                            log.error(
                                f"--- tail {path} on {node.hostname} ---\n{tail}"
                            )
                        except Exception:
                            pass
                    overall_rc = 1
                    break

                log.info(
                    "Multilock iteration %s/%s (vers=%s) completed successfully",
                    it,
                    iterations,
                    version,
                )

            if overall_rc != 0:
                break

            log.info(
                "multilock NFS vers=%s: all %s iteration(s) completed",
                version,
                iterations,
            )

        if overall_rc == 0:
            log.info(
                "multilock split_lock finished for NFS versions %s (%s iteration(s) each)",
                nfs_versions,
                iterations,
            )
        return overall_rc
    except OperationFailedError:
        raise
    except Exception as e:
        log.error(f"Error: {e}")
        return 1
    finally:
        try:
            controller_client.exec_command(
                cmd="pkill -f '[m]l_console' || pkill -f ml_console || true",
                sudo=True,
                long_running=False,
                timeout=30,
                check_ec=False,
            )
        except Exception:
            pass
        sleep(1)
        if do_cleanup:
            log.info(
                "Multilock post-test cleanup on posix clients (mount %s, spectrum_scale=%s)",
                nfs_mount,
                spectrum_scale,
            )
            try:
                _cleanup_posix_mounts(posix_nodes, nfs_mount)
            except Exception as exc:
                log.warning("multilock cleanup failed: %s", exc)
