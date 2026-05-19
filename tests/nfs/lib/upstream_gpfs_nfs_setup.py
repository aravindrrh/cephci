"""Spectrum Scale (GPFS) NFS bootstrap and client mounts for upstream cephci tests."""

from os import environ
from time import sleep

from ceph.waiter import WaitUntil
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, MountFailedError, Unmount
from utility.log import Log

log = Log(__name__)

CI_TESTS_REPO = "https://github.com/aravindrrh/ci-tests"
DEFAULT_CI_TESTS_BRANCH = "scale_downstream"
MULTI_NODE_SCALE_SCRIPT = (
    "sh ci-tests/build_scripts/common/basic-storage-scale-multi-node.sh"
)
DEPLOY_PREREQ_PACKAGES = (
    "elfutils elfutils-devel kernel-devel-$(uname -r) "
    "kernel-headers-$(uname -r) gcc-c++"
)

# Mount paths used across upstream Scale NFS suites (combined-suite cleanup).
COMMON_UPSTREAM_MOUNT_POINTS = (
    "/mnt/nfs",
    "/mnt/nfsv3",
    "/mnt/nfsv4",
    "/mnt/nfsv4_1",
    "/mnt/multilock_test",
)


def should_skip_deployment(config):
    """Return True when cluster deploy should be skipped (already prepared)."""
    conf = config or {}
    skip_deploy = environ.get("SKIP_DEPLOYMENT", "").lower() == "true"
    if "skip_deployment" in conf:
        sd = conf.get("skip_deployment")
        if isinstance(sd, str):
            skip_deploy = sd.strip().lower() in ("true", "1", "yes")
        else:
            skip_deploy = bool(sd)
    return skip_deploy


def add_etc_host_entries(nodes):
    """Append cluster host entries to /etc/hosts on every node."""
    etc_hosts_string = ""
    for node in nodes:
        etc_hosts_string += f"{node.ip_address} {node.hostname}\n"

    for node in nodes:
        node.exec_command(cmd=f"echo '{etc_hosts_string}' >> /etc/hosts", sudo=True)


def setup_passwordless_ssh(nodes):
    """Configure passwordless SSH between all nodes."""
    log.info("Setting up passwordless SSH between all nodes")

    for node in nodes:
        log.info("Generating SSH key on %s", node.hostname)
        node.exec_command(
            cmd="[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa",
            sudo=True,
        )

    public_keys = {}
    for node in nodes:
        log.info("Collecting public key from %s", node.hostname)
        out, _ = node.exec_command(cmd="cat ~/.ssh/id_rsa.pub", sudo=True)
        public_keys[node.hostname] = out.strip()

    for node in nodes:
        log.info("Distributing public keys to %s", node.hostname)
        node.exec_command(cmd="mkdir -p ~/.ssh && chmod 700 ~/.ssh", sudo=True)
        node.exec_command(
            cmd="touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys",
            sudo=True,
        )
        for pub_key in public_keys.values():
            check_cmd = (
                f"grep -q '{pub_key}' ~/.ssh/authorized_keys || "
                f"echo '{pub_key}' >> ~/.ssh/authorized_keys"
            )
            node.exec_command(cmd=check_cmd, sudo=True)

        ssh_config = """Host *
StrictHostKeyChecking no
UserKnownHostsFile=/dev/null"""
        node.exec_command(
            cmd=f"echo '{ssh_config}' > ~/.ssh/config && chmod 600 ~/.ssh/config",
            sudo=True,
        )

    log.info("Passwordless SSH setup completed successfully")


def install_deploy_prereq_packages(nodes):
    """Install kernel/elfutils build deps required by multi-node Scale deploy."""
    log.info("Installing deploy prerequisite packages on all nodes")
    cmd = f"yum install -y {DEPLOY_PREREQ_PACKAGES}"
    for node in nodes:
        node.exec_command(cmd=cmd, sudo=True)


def deploy_gpfs_scale(ceph_cluster, config=None):
    """
    Deploy multi-node IBM Spectrum Scale / NFS via ci-tests on the installer.

    Expects installer + at least two client nodes (node2/node3 hostnames are
    exported for basic-storage-scale-multi-node.sh).

    Config keys:
        ci_tests_branch: git branch (default scale_downstream)
        deploy_timeout: per-command timeout in seconds (default 7200)
    """
    conf = config or {}
    branch = conf.get("ci_tests_branch", DEFAULT_CI_TESTS_BRANCH)
    timeout = int(conf.get("deploy_timeout", 7200))

    server = ceph_cluster.get_nodes("installer")[0]
    clients = ceph_cluster.get_nodes("client")
    if len(clients) < 2:
        raise ConfigError(
            "Multi-node Spectrum Scale deploy requires at least two client nodes"
        )

    node2 = clients[0].hostname
    node3 = clients[1].hostname
    nodes = ceph_cluster.get_nodes()

    add_etc_host_entries(nodes)
    install_deploy_prereq_packages(nodes)
    setup_passwordless_ssh(nodes)

    server_cmds = [
        "rm -rf ci-tests/",
        "yum install -y git wget",
        f'echo "export node2=\\"{node2}\\"" >> ~/.bashrc && source ~/.bashrc',
        f'echo "export node3=\\"{node3}\\"" >> ~/.bashrc && source ~/.bashrc',
        f"git clone {CI_TESTS_REPO}; cd ci-tests; git checkout {branch}",
        MULTI_NODE_SCALE_SCRIPT,
    ]

    log.info(
        "Deploying multi-node Spectrum Scale / NFS on installer %s "
        "(node2=%s node3=%s branch=%s)",
        server.hostname,
        node2,
        node3,
        branch,
    )
    for cmd in server_cmds:
        rc = server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=timeout)
        if rc != 0:
            raise OperationFailedError(
                f"GPFS multi-node deploy command failed (exit {rc}): {cmd}"
            )

    log.info("Multi-node Spectrum Scale / NFS deployment completed")
    return {"server": server, "node2": node2, "node3": node3}


def setup_gpfs_nfs(ceph_cluster, config):
    """
    Optionally deploy Scale NFS via ci-tests, then mount the export on clients.

    Environment:
        SKIP_DEPLOYMENT: if ``true``, skip server bootstrap (cluster already prepared).
        EXPORT_NAME: export path when not set in config (default ``/ibm/scale_volume``).

    Config keys:
        mount_point, nfs_export, port, nfs_version, clients, mount_type
        skip_deployment: if present (bool), overrides SKIP_DEPLOYMENT for this run.
            Use ``true`` after a suite-local deploy step; ``false`` or omit on deploy.

    Returns:
        dict with server, clients, nfs_mount, nfs_export, nfs_server_host, port, version, mount_type
    """
    conf = config or {}
    mount_point = conf.get("mount_point", "/mnt/nfs")
    nfs_export = conf.get("nfs_export") or environ.get("EXPORT_NAME", "/ibm/scale_volume")
    port = str(conf.get("port", "2049"))
    version = str(conf.get("nfs_version", "4.1"))
    no_clients = int(conf.get("clients", "1"))
    mount_type = conf.get("mount_type", "nfs")
    skip_deploy = should_skip_deployment(conf)

    server = ceph_cluster.get_nodes("installer")[0]
    clients_all = ceph_cluster.get_nodes("client")
    if no_clients > len(clients_all):
        raise ConfigError("The test requires more clients than available")
    clients = clients_all[:no_clients]

    if not skip_deploy:
        deploy_gpfs_scale(ceph_cluster, conf)
    else:
        log.info("skip_deployment set — skipping multi-node Scale deploy")

    nfs_server_host = server.ip_address

    if mount_type != "nfs":
        raise ConfigError(f"Unsupported mount_type {mount_type}")

    for client in clients:
        client.exec_command(
            sudo=True,
            cmd="yum install -y nfs-utils || dnf install -y nfs-utils",
            long_running=True,
            check_ec=False,
        )
        client.exec_command(sudo=True, cmd=f"mkdir -p {mount_point}")
        client.exec_command(
            sudo=True, cmd=f"umount -f {mount_point}", check_ec=False
        )
        client.exec_command(
            sudo=True, cmd=f"umount -l {mount_point}", check_ec=False
        )
        try:
            Mount(client).nfs(
                mount=mount_point,
                version=version,
                port=port,
                server=nfs_server_host,
                export=nfs_export,
            )
        except MountFailedError as e:
            raise OperationFailedError(
                f"NFS mount failed on {client.hostname}: {e}"
            ) from e
        sleep(1)

    log.info(
        "GPFS NFS ready: %s:%s -> %s on %d client(s)",
        nfs_server_host,
        nfs_export,
        mount_point,
        len(clients),
    )

    return {
        "server": server,
        "clients": clients,
        "nfs_mount": mount_point,
        "nfs_export": nfs_export,
        "nfs_server_host": nfs_server_host,
        "port": port,
        "version": version,
        "mount_type": mount_type,
    }


def get_suite_cleanup_mount_points(config):
    """Return mount paths to clear between combined-suite tests."""
    conf = config or {}
    points = set(COMMON_UPSTREAM_MOUNT_POINTS)
    mount_point = conf.get("mount_point")
    if mount_point:
        points.add(mount_point)
    extra = conf.get("cleanup_mount_points") or conf.get("mount_points") or []
    if isinstance(extra, str):
        extra = [extra]
    points.update(extra)
    return sorted(points)


def cleanup_nfs_mount_on_node(node, nfs_mount, remove_mount_dir=True):
    """rm -rf mount contents, unmount, and optionally remove the mount directory."""
    host = node.hostname
    try:
        node.exec_command(
            cmd=f"bash -lc 'sync; rm -rf {nfs_mount}/* 2>/dev/null; true'",
            sudo=True,
            check_ec=False,
        )
    except Exception as exc:
        log.warning("cleanup rm under %s on %s: %s", nfs_mount, host, exc)
    sleep(1)
    for umount_cmd in (
        f"umount -f {nfs_mount} 2>/dev/null || true",
        f"umount -l {nfs_mount} 2>/dev/null || true",
    ):
        try:
            node.exec_command(cmd=umount_cmd, sudo=True, check_ec=False)
        except Exception as exc:
            log.warning("cleanup %s on %s (%s): %s", umount_cmd, host, nfs_mount, exc)
    sleep(1)
    try:
        out = Unmount(node).unmount(nfs_mount)
        if out:
            log.warning("Unmount helper %s on %s: %s", nfs_mount, host, out)
    except Exception as exc:
        log.warning("Unmount helper failed for %s on %s: %s", nfs_mount, host, exc)
    if remove_mount_dir:
        try:
            node.exec_command(cmd=f"rm -rf {nfs_mount}", sudo=True, check_ec=False)
        except Exception as exc:
            log.warning("cleanup rmdir %s on %s: %s", nfs_mount, host, exc)


def cleanup_upstream_nfs_mounts(nodes, mount_points=None, remove_mount_dir=True):
    """
    Clear NFS mount data and unmount on all given nodes.

    Used between tests in a combined suite so the next test starts clean.
    """
    if not nodes:
        return
    if not isinstance(nodes, list):
        nodes = [nodes]
    points = mount_points or list(COMMON_UPSTREAM_MOUNT_POINTS)
    log.info(
        "Suite cleanup: clearing %d mount point(s) on %d node(s)",
        len(points),
        len(nodes),
    )
    for node in nodes:
        for nfs_mount in points:
            cleanup_nfs_mount_on_node(node, nfs_mount, remove_mount_dir=remove_mount_dir)
    log.info("Suite NFS mount cleanup completed")


def run_suite_cleanup(ceph_cluster, config):
    """Run combined-suite mount cleanup on all clients when enabled in config."""
    conf = config or {}
    if conf.get("suite_cleanup", True) is False:
        return
    nodes = ceph_cluster.get_nodes("client")
    cleanup_upstream_nfs_mounts(
        nodes, get_suite_cleanup_mount_points(conf), remove_mount_dir=True
    )


def teardown_gpfs_nfs(clients, nfs_mount):
    """Remove data under the mount, unmount, and delete the mount point."""
    if not isinstance(clients, list):
        clients = [clients]
    timeout, interval = 600, 10
    for client in clients:
        for w in WaitUntil(timeout=timeout, interval=interval):
            try:
                client.exec_command(
                    sudo=True, cmd=f"rm -rf {nfs_mount}/*", long_running=True
                )
                break
            except Exception as e:
                log.warning("rm under %s failed, retrying: %s", nfs_mount, e)
        if w.expired:
            log.error("Timeout clearing %s on %s", nfs_mount, client.hostname)
        cleanup_nfs_mount_on_node(client, nfs_mount, remove_mount_dir=True)
