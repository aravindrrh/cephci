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


def ensure_rpcbind_running(nodes):
    """
    Unmask and start rpcbind before Scale/Ganesha install.

    Ganesha bring-up fails if rpcbind is masked or inactive on the node.
    Raises OperationFailedError if rpcbind is not active after start.
    """
    log.info("Ensuring rpcbind is unmasked and active on all nodes")
    setup_cmds = [
        "systemctl unmask rpcbind",
        "systemctl unmask rpcbind.socket",
        "systemctl enable rpcbind",
        "systemctl start rpcbind",
    ]
    for node in nodes:
        for cmd in setup_cmds:
            log.info("[%s] %s", node.hostname, cmd)
            node.exec_command(cmd=cmd, sudo=True, long_running=True)

        # Fail deploy early if rpcbind did not come up (Ganesha will fail later).
        out, err = node.exec_command(
            cmd="systemctl is-active rpcbind", sudo=True, check_ec=False
        )
        state = (out or "").strip()
        log.info("[%s] rpcbind is-active: %s", node.hostname, state)
        if state != "active":
            node.exec_command(
                cmd="systemctl status rpcbind --no-pager || true",
                sudo=True,
                long_running=True,
                check_ec=False,
            )
            raise OperationFailedError(
                f"rpcbind is not active on {node.hostname} (state={state!r}); "
                f"stderr={err}"
            )


def deploy_gpfs_scale(ceph_cluster, config=None):
    """
    Deploy Spectrum Scale + NFS-Ganesha via CephCI Python stages.

    Stages:
      1. deploy_spectrum_scale (mgr nodes; installer runs spectrumscale)
      2. build_install_ganesha (node with role ``nfs``)
      3. create_nfs_export

    Role model:
      - mgr: Scale cluster members
      - nfs: Ganesha node
      - client: NFS test clients

    Config keys:
        deploy_timeout: per-command timeout in seconds (default 7200)
        cloud-type: from run.py ``--cloud``; if baremetal, skip /etc/hosts
            and passwordless SSH setup
        skip_scale / skip_ganesha / skip_export: skip individual stages
        force_scale_redeploy: ignore version match and reinstall Scale
        gerrit_host / gerrit_project / gerrit_refspec (or ganesha_repo /
            ganesha_branch): Ganesha source location
        scale_fs, ces_ip, scale_installer_source: see scale_deploy.py
        use_ci_tests_script: if true, fall back to legacy shell script path

    When S3 VERSION_TO_USE matches the installed Scale version and the cluster
    is healthy, Scale install is skipped (Ganesha-only). If the version changed
    (or Scale is unhealthy), a full Scale wipe runs first, then Scale install.
    """
    from tests.nfs.lib.nfs_ganesha_deploy import (
        build_install_ganesha,
        create_nfs_export,
        resolve_ganesha_node,
    )
    from tests.nfs.lib.scale_deploy import (
        clear_scale_reuse_marker,
        deploy_spectrum_scale,
        resolve_scale_roles,
        scale_residue_present,
        should_reuse_existing_scale,
        write_scale_reuse_marker,
    )

    conf = config or {}
    timeout = int(conf.get("deploy_timeout", 7200))
    nodes = ceph_cluster.get_nodes()

    # Legacy escape hatch: keep old ci-tests shell path if explicitly requested.
    if conf.get("use_ci_tests_script"):
        return _deploy_gpfs_scale_via_ci_tests(ceph_cluster, conf)

    roles = resolve_scale_roles(ceph_cluster)
    installer = roles["installer"]
    clients = roles["nfs_clients"]
    if len(clients) < 1:
        raise ConfigError(
            "Upstream Scale NFS deploy requires at least one node with role 'client'"
        )

    cloud_type = str(conf.get("cloud-type", "")).lower()
    is_baremetal = "baremetal" in cloud_type or any(
        getattr(getattr(n, "vm_node", None), "node_type", "") == "baremetal"
        for n in nodes
    )

    if is_baremetal:
        log.info(
            "cloud-type=%s — skipping /etc/hosts and passwordless SSH setup",
            cloud_type or "baremetal",
        )
    else:
        add_etc_host_entries(nodes)
        setup_passwordless_ssh(nodes)

    install_deploy_prereq_packages(nodes)
    # Include Ganesha node explicitly (may differ from mgr set in odd layouts).
    ganesha_node = resolve_ganesha_node(ceph_cluster)
    rpcbind_nodes = list(
        {id(n): n for n in (roles["scale_nodes"] + [ganesha_node])}.values()
    )
    ensure_rpcbind_running(rpcbind_nodes)

    result = {
        "server": installer,
        "installer": installer,
        "scale_nodes": roles["scale_nodes"],
        "nfs_clients": clients,
        "ganesha_node": ganesha_node,
    }

    if conf.get("skip_scale"):
        log.info("skip_scale set — skipping Spectrum Scale deploy")
        clear_scale_reuse_marker(installer)
    else:
        reuse, reuse_ver = should_reuse_existing_scale(ceph_cluster, conf)
        if reuse:
            write_scale_reuse_marker(installer, reuse_ver)
            result["scale_reused"] = True
            result["scale_version"] = reuse_ver
            log.info(
                "VERSION_TO_USE matches installed Scale (%s) — "
                "skipping Scale install; Ganesha only",
                reuse_ver,
            )
        else:
            # Version change / broken Scale / force: wipe before reinstall.
            if scale_residue_present(installer):
                log.info(
                    "Scale version mismatch or unclean install — "
                    "full Scale cleanup before redeploy"
                )
                uninstall_gpfs_scale(
                    ceph_cluster, {**conf, "uninstall_scale": True}
                )
            clear_scale_reuse_marker(installer)
            scale_info = deploy_spectrum_scale(ceph_cluster, conf)
            result.update(scale_info)
            result["scale_reused"] = False

    if not conf.get("skip_ganesha"):
        ganesha_info = build_install_ganesha(ceph_cluster, conf)
        result.update(ganesha_info)
    else:
        log.info("skip_ganesha set — skipping NFS-Ganesha build/install")

    if not conf.get("skip_export"):
        export_info = create_nfs_export(ceph_cluster, conf)
        result.update(export_info)
    else:
        log.info("skip_export set — skipping NFS export create")

    log.info(
        "Multi-node Spectrum Scale / NFS deployment completed "
        "(installer=%s clients=%s timeout=%s)",
        installer.hostname,
        [c.hostname for c in clients],
        timeout,
    )
    return result


def _deploy_gpfs_scale_via_ci_tests(ceph_cluster, conf):
    """Legacy path: clone ci-tests and run basic-storage-scale-multi-node.sh."""
    branch = conf.get("ci_tests_branch", DEFAULT_CI_TESTS_BRANCH)
    timeout = int(conf.get("deploy_timeout", 7200))
    server = ceph_cluster.get_nodes("installer")[0]
    clients = ceph_cluster.get_nodes("client")
    if len(clients) < 2:
        raise ConfigError(
            "Legacy ci-tests Scale deploy requires at least two client nodes"
        )
    node2, node3 = clients[0].hostname, clients[1].hostname
    nodes = ceph_cluster.get_nodes()

    cloud_type = str(conf.get("cloud-type", "")).lower()
    is_baremetal = "baremetal" in cloud_type or any(
        getattr(getattr(n, "vm_node", None), "node_type", "") == "baremetal"
        for n in nodes
    )
    if not is_baremetal:
        add_etc_host_entries(nodes)
        setup_passwordless_ssh(nodes)
    install_deploy_prereq_packages(nodes)
    ensure_rpcbind_running(nodes)

    server_cmds = [
        "rm -rf ci-tests/",
        "yum install -y git wget",
        f'echo "export node2=\\"{node2}\\"" >> ~/.bashrc && source ~/.bashrc',
        f'echo "export node3=\\"{node3}\\"" >> ~/.bashrc && source ~/.bashrc',
        f"git clone {CI_TESTS_REPO}; cd ci-tests; git checkout {branch}",
        MULTI_NODE_SCALE_SCRIPT,
    ]
    log.info(
        "Legacy ci-tests Scale deploy on %s (node2=%s node3=%s)",
        server.hostname,
        node2,
        node3,
    )
    for cmd in server_cmds:
        rc = server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=timeout)
        if rc != 0:
            raise OperationFailedError(
                f"GPFS multi-node deploy command failed (exit {rc}): {cmd}"
            )
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


# Paths left by basic-storage-scale-multi-node.sh and related upstream tests.
DEFAULT_SCALE_FS = "scale_volume"
MMFS_BIN = "/usr/lpp/mmfs/bin"
CLEANUP_CLONE_DIRS = (
    "ci-tests",
    "nfs-ganesha",
    "DOWNLOAD_STORAGE_SCALE",
    "rpmbuild",
    "/root/ci-tests",
    "/root/nfs-ganesha",
    "/root/nfstest",
    "/root/DOWNLOAD_STORAGE_SCALE",
    "/root/rpmbuild",
)
# Deploy artifacts on the installer/_admin node (prefer absolute /root paths).
ADMIN_CLEANUP_DIRS = (
    "/root/DOWNLOAD_STORAGE_SCALE",
    "/root/nfs-ganesha",
    "/root/rpmbuild",
    "/root/ci-tests",
    "DOWNLOAD_STORAGE_SCALE",
    "ci-tests",
    "rpmbuild",
    "nfs-ganesha",
)
CLEANUP_RESIDUAL_DIRS = (
    "/var/mmfs",
    "/usr/lpp/mmfs",
    "/tmp/mmfs",
)
GANESHA_RPM_GREP = r"^(nfs-ganesha|libntirpc|gpfs\.nfs-ganesha)"
SCALE_RPM_GREP = r"^(gpfs|spectrum)"


def _best_effort(node, cmd, timeout=600):
    """Run a command and log failures without raising (teardown must keep going)."""
    host = getattr(node, "hostname", str(node))
    try:
        log.info("[%s] %s", host, cmd)
        node.exec_command(
            cmd=cmd,
            sudo=True,
            long_running=True,
            timeout=timeout,
            check_ec=False,
        )
    except Exception as exc:
        log.warning("[%s] best-effort failed (%s): %s", host, cmd, exc)


def _mm_cmd(cmd):
    """Prefix Scale CLI with PATH so mm* tools resolve after install."""
    return f"bash -lc 'export PATH=\"$PATH:{MMFS_BIN}\"; {cmd}'"


def _stop_nfs_ganesha_stack(nodes, timeout=600, disable_ces_nfs=False):
    """
    Stop nfs-ganesha on every node (installer first).

    By default do **not** ``mmces service disable nfs`` — that clears the local
    /var/mmfs/ces/nfs-config cache while CCR still holds the objects. Ganesha-only
    cleanup should leave CES NFS config on disk for the next reuse run.

    Set disable_ces_nfs=True when doing a full Scale wipe.
    """
    for node in nodes:
        cmds = [
            "systemctl stop nfs-ganesha || true",
            "systemctl disable nfs-ganesha || true",
        ]
        if disable_ces_nfs:
            cmds.extend(
                (
                    _mm_cmd(f"{MMFS_BIN}/mmces service stop nfs || true"),
                    _mm_cmd(f"{MMFS_BIN}/mmces service disable nfs --force || true"),
                )
            )
        for cmd in cmds:
            _best_effort(node, cmd, timeout=timeout)


def _teardown_scale_cluster(installer, scale_fs, timeout=600):
    """
    Unmount FS, delete filesystem/NSDs, and shut down GPFS on the cluster.

    Cluster-wide mm* commands are issued from the installer node.
    """
    cmds = [
        _mm_cmd(f"{MMFS_BIN}/mmumount all -a || true"),
        # Force-delete the test FS created by the multi-node deploy script.
        _mm_cmd(f"{MMFS_BIN}/mmdelfs {scale_fs} -p || true"),
        _mm_cmd(f"{MMFS_BIN}/mmdelfs {scale_fs} -f || true"),
        # Drop remaining NSDs so a later deploy can recreate file-backed disks.
        _mm_cmd(
            f"for nsd in $({MMFS_BIN}/mmlsnsd -L 2>/dev/null | "
            f"awk '/nsd/ {{print $1}}'); do "
            f"{MMFS_BIN}/mmdelnsd $nsd -f || true; done"
        ),
        _mm_cmd(f"{MMFS_BIN}/mmshutdown -a || true"),
    ]
    for cmd in cmds:
        _best_effort(installer, cmd, timeout=timeout)


def _remove_rpms_matching(node, pattern, timeout=600):
    """Remove RPMs whose names match egrep pattern (nodeps for stuck deps)."""
    cmd = (
        f"bash -lc \"pkgs=$(rpm -qa | grep -E '{pattern}' || true); "
        f'if [ -n \\"$pkgs\\" ]; then rpm -e --nodeps $pkgs || true; fi"'
    )
    _best_effort(node, cmd, timeout=timeout)


def _remove_ganesha_clone_dirs(node, timeout=600):
    """Remove Ganesha/ci-tests clones and download trees (not /var/mmfs)."""
    paths = " ".join(CLEANUP_CLONE_DIRS)
    _best_effort(
        node,
        f"bash -lc 'rm -rf {paths} 2>/dev/null || true'",
        timeout=timeout,
    )


def _remove_cleanup_dirs(node, timeout=600, remove_scale_dirs=True):
    """Remove git clones, download trees; optionally Scale residual dirs."""
    paths = list(CLEANUP_CLONE_DIRS)
    if remove_scale_dirs:
        paths.extend(CLEANUP_RESIDUAL_DIRS)
    path_str = " ".join(paths)
    nsd_rm = (
        " /home/nsd1_* /root/nsd1_*" if remove_scale_dirs else ""
    )
    _best_effort(
        node,
        f"bash -lc 'rm -rf {path_str}{nsd_rm} 2>/dev/null || true'",
        timeout=timeout,
    )
    if remove_scale_dirs:
        # Scale leaves ras logs even after package removal.
        _best_effort(
            node,
            "bash -lc 'rm -rf /var/adm/ras/* 2>/dev/null || true'",
            timeout=timeout,
        )


def _remove_admin_deploy_dirs(installer, timeout=600):
    """
    Remove Scale/NFS deploy workdirs from the installer (_admin) node.

    Targets DOWNLOAD_STORAGE_SCALE, ci-tests, and rpmbuild under /root and
    /home/cephuser (deploy may have run as either).
    """
    names = " ".join(ADMIN_CLEANUP_DIRS)
    log.info(
        "Removing admin deploy dirs on %s: %s",
        installer.hostname,
        ", ".join(ADMIN_CLEANUP_DIRS),
    )
    _best_effort(
        installer,
        "bash -lc '"
        f"for d in {names}; do "
        "rm -rf \"$d\" \"/root/$d\" \"/home/cephuser/$d\" "
        "\"$HOME/$d\" 2>/dev/null || true; "
        "done"
        "'",
        timeout=timeout,
    )


def _strip_deploy_bashrc_exports(installer):
    """Remove node2/node3 exports appended by deploy_gpfs_scale."""
    _best_effort(
        installer,
        "bash -lc \"sed -i '/^export node2=/d; /^export node3=/d' ~/.bashrc || true\"",
    )


def _remove_cesip_hosts_entries(installer):
    """
    Remove CES IP host aliases from /etc/hosts on the admin/installer node.

    basic-storage-scale-multi-node.sh appends lines like: ``<ip>    cesip1``.
    """
    _best_effort(
        installer,
        # Match cesip / cesip1 / cesipN as a whole hostname token.
        "bash -lc \"sed -i -E '/[[:space:]]cesip[0-9]*([[:space:]]|$)/d' "
        '/etc/hosts || true"',
    )


def uninstall_gpfs_scale(ceph_cluster, config=None):
    """
    Tear down NFS-Ganesha (default) and optionally the Spectrum Scale cluster.

    Default (suite cleanup): Ganesha-only — unmounts, stop Ganesha/CES NFS,
    remove Ganesha RPMs/clones. Leaves Scale cluster and CES IP intact so the
    next run can reuse Scale when VERSION_TO_USE is unchanged.

    Full Scale wipe when ``uninstall_scale`` / ``force_scale_uninstall`` is
    true (used before redeploy when VERSION_TO_USE changes).

    Config keys:
        scale_fs: GPFS filesystem name (default scale_volume)
        cleanup_timeout: per-command timeout seconds (default 600)
        uninstall_scale / force_scale_uninstall: also tear down Scale
    """
    conf = config or {}
    scale_fs = conf.get("scale_fs", DEFAULT_SCALE_FS)
    timeout = int(conf.get("cleanup_timeout", 600))
    wipe_scale = bool(
        conf.get("uninstall_scale") or conf.get("force_scale_uninstall")
    )

    installer = ceph_cluster.get_nodes("installer")[0]
    nodes = ceph_cluster.get_nodes()
    # Installer first so cluster-wide mm* commands prefer the quorum node.
    ordered = [installer] + [n for n in nodes if n != installer]

    if wipe_scale:
        log.info(
            "Uninstalling Spectrum Scale + NFS-Ganesha on %d node(s) "
            "(installer=%s fs=%s)",
            len(ordered),
            installer.hostname,
            scale_fs,
        )
    else:
        log.info(
            "Uninstalling NFS-Ganesha only on %d node(s) "
            "(installer=%s; Scale cluster preserved)",
            len(ordered),
            installer.hostname,
        )

    run_suite_cleanup(ceph_cluster, conf)
    # Ganesha-only: leave CES NFS enabled so nfs-config stays on disk / in CCR sync.
    _stop_nfs_ganesha_stack(
        ordered, timeout=timeout, disable_ces_nfs=wipe_scale
    )

    if wipe_scale:
        _teardown_scale_cluster(installer, scale_fs, timeout=timeout)

    for node in ordered:
        _remove_rpms_matching(node, GANESHA_RPM_GREP, timeout=timeout)
        if wipe_scale:
            _remove_rpms_matching(node, SCALE_RPM_GREP, timeout=timeout)
            _remove_cleanup_dirs(node, timeout=timeout, remove_scale_dirs=True)
        else:
            _remove_ganesha_clone_dirs(node, timeout=timeout)

    # Always drop admin download/clone dirs (not the live Scale install).
    _remove_admin_deploy_dirs(installer, timeout=timeout)
    _strip_deploy_bashrc_exports(installer)
    if wipe_scale:
        _remove_cesip_hosts_entries(installer)
        try:
            from tests.nfs.lib.scale_deploy import clear_scale_reuse_marker

            clear_scale_reuse_marker(installer)
        except Exception as exc:
            log.warning("clear_scale_reuse_marker failed: %s", exc)

    log.info(
        "Cleanup completed (best-effort, wipe_scale=%s)",
        wipe_scale,
    )
    return {
        "installer": installer,
        "nodes": ordered,
        "scale_fs": scale_fs,
        "wipe_scale": wipe_scale,
    }
