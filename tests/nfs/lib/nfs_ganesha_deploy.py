"""
NFS-Ganesha build-from-source and export setup for Spectrum Scale (CephCI).

Role model:
  - nfs: node(s) where Ganesha is built/installed (usually installer + mgr)
  - client: NFS test clients (mount only; not used here)

Config defaults match basic-storage-scale-multi-node.sh:
  gerrit_host=github.com
  gerrit_project=nfs-ganesha/nfs-ganesha
  gerrit_refspec=refs/heads/next
"""

import shlex

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

DEFAULT_GERRIT_HOST = "github.com"
DEFAULT_GERRIT_PROJECT = "nfs-ganesha/nfs-ganesha"
DEFAULT_GERRIT_REFSPEC = "refs/heads/next"
DEFAULT_SCALE_FS = "scale_volume"
MMFS_BIN = "/usr/lpp/mmfs/bin"

GANESHA_BASE_PACKAGES = (
    "git bison flex cmake gcc-c++ libacl-devel krb5-devel dbus-devel "
    "rpm-build redhat-rpm-config gdb libblkid-devel libcap-devel "
    "libgfapi-devel xfsprogs-devel libnsl2-devel libnfsidmap-devel "
    "libwbclient-devel userspace-rcu-devel libcephfs-devel "
    "selinux-policy-devel sqlite unzip"
)


def _run(node, cmd, timeout=7200, check=True):
    """Run a remote command; raise on non-zero when check=True."""
    log.info("[%s] %s", node.hostname, cmd)
    rc = node.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=timeout)
    if check and rc != 0:
        raise OperationFailedError(
            f"Command failed on {node.hostname} (exit {rc}): {cmd}"
        )
    return rc


def resolve_ganesha_node(ceph_cluster):
    """Return the primary NFS-Ganesha node (role ``nfs``), fallback to installer."""
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    if nfs_nodes:
        return nfs_nodes[0]
    installers = ceph_cluster.get_nodes("installer")
    if installers:
        log.warning(
            "No node with role 'nfs'; falling back to installer %s",
            installers[0].hostname,
        )
        return installers[0]
    raise ConfigError(
        "Ganesha deploy requires a node with role 'nfs' (or 'installer' fallback)"
    )


def _resolve_git_source(conf):
    """Return (git_url, refspec, local_clone_dir) from config."""
    host = conf.get("gerrit_host", DEFAULT_GERRIT_HOST)
    project = conf.get("gerrit_project", DEFAULT_GERRIT_PROJECT)
    if conf.get("ganesha_repo"):
        repo = conf["ganesha_repo"].rstrip("/")
        if repo.startswith("http://") or repo.startswith("https://"):
            git_url = repo
            project = "/".join(repo.split("/")[-2:])
        else:
            project = repo
            git_url = f"https://{host}/{project}"
    else:
        git_url = f"https://{host}/{project}"

    refspec = (
        conf.get("ganesha_branch")
        or conf.get("gerrit_refspec")
        or DEFAULT_GERRIT_REFSPEC
    )
    git_repo = project.split("/")[-1]
    return git_url, refspec, git_repo


def build_install_ganesha(ceph_cluster, config=None):
    """
    Clone NFS-Ganesha, build RPMs from source, and install on the ``nfs`` node.

    Config keys:
        gerrit_host: git host (default github.com)
        gerrit_project: org/repo (default nfs-ganesha/nfs-ganesha)
        gerrit_refspec: branch/ref (default refs/heads/next)
        ganesha_repo / ganesha_branch: aliases for project URL pieces / refspec
        deploy_timeout: per-command timeout (default 7200)
        yum_repo: if set, install Ganesha from yum instead of source build
    """
    conf = config or {}
    timeout = int(conf.get("deploy_timeout", 7200))
    node = resolve_ganesha_node(ceph_cluster)
    git_url, refspec, git_repo = _resolve_git_source(conf)

    log.info(
        "build_install_ganesha on %s (url=%s refspec=%s)",
        node.hostname,
        git_url,
        refspec,
    )

    # Preflight: rpcbind, SELinux permissive, firewalld off (as in ci-tests script).
    _run(node, "dnf install -y rpcbind || yum install -y rpcbind", timeout=timeout)
    _run(node, "systemctl unmask rpcbind", timeout=timeout, check=False)
    _run(node, "systemctl unmask rpcbind.socket", timeout=timeout, check=False)
    _run(node, "systemctl start rpcbind", timeout=timeout)
    _run(node, "setenforce 0", timeout=60, check=False)
    _run(node, "systemctl stop firewalld || true", timeout=60, check=False)

    yum_repo = conf.get("yum_repo") or conf.get("YUM_REPO")
    if yum_repo:
        _run(
            node,
            "yum-config-manager --add-repo="
            "http://artifacts.ci.centos.org/nfs-ganesha/nightly/libntirpc/"
            "libntirpc-latest.repo",
            timeout=timeout,
            check=False,
        )
        _run(node, f"yum-config-manager --add-repo={yum_repo}", timeout=timeout)
        _run(
            node,
            "dnf -y install nfs-ganesha nfs-ganesha-gluster glusterfs-ganesha",
            timeout=timeout,
        )
        _run(node, "systemctl start nfs-ganesha", timeout=timeout)
        return {"ganesha_node": node, "mode": "yum", "yum_repo": yum_repo}

    # Source build path
    _run(
        node,
        "dnf install -y centos-release-gluster yum-utils centos-release-ceph "
        "epel-release unzip --skip-broken || true",
        timeout=timeout,
        check=False,
    )
    _run(
        node,
        "subscription-manager repos --enable "
        "codeready-builder-for-rhel-9-$(arch)-rpms || true",
        timeout=timeout,
        check=False,
    )
    _run(
        node,
        f"dnf install -y {GANESHA_BASE_PACKAGES} --skip-broken",
        timeout=timeout,
        check=False,
    )

    # Fresh clone under /root for a stable path across sudo sessions.
    clone_dir = f"/root/{git_repo}"
    _run(node, f"rm -rf {clone_dir}", timeout=timeout, check=False)
    _run(node, f"git init {clone_dir}", timeout=timeout)
    fetch_script = (
        f"cd {clone_dir} && "
        f'(git fetch --depth=1 "{git_url}" "{refspec}" || '
        f'git fetch "{git_url}" "{refspec}") && '
        "git checkout -B build FETCH_HEAD && "
        "(git submodule update --recursive --init || git submodule sync)"
    )
    _run(node, f"bash -lc {shlex.quote(fetch_script)}", timeout=timeout)

    # Disable CES NFS / stop service before replacing bundled IBM Ganesha RPMs.
    _run(
        node,
        f"{MMFS_BIN}/mmces service disable nfs --force || true",
        timeout=timeout,
        check=False,
    )
    _run(node, "systemctl stop nfs-ganesha || true", timeout=timeout, check=False)

    # Build RPMs into <clone>/build/{x86_64,noarch}/ (not build/nfs-ganesha/build/).
    build_script = (
        f"cd {clone_dir} && mkdir -p build && cd build && "
        "cmake -DCMAKE_BUILD_TYPE=Maintainer -DUSE_FSAL_GPFS=ON -DUSE_DBUS=ON "
        "-D_MSPAC_SUPPORT=OFF -DMONITORING=ON -DUSE_MONITORING=ON ../src && "
        "make dist && "
        'rpmbuild -ta --define "_srcrpmdir $PWD" --define "_rpmdir $PWD" *.tar.gz'
    )
    _run(node, f"bash -lc {shlex.quote(build_script)}", timeout=timeout)

    # Purge all IBM gpfs.nfs-ganesha* packages (file conflicts with upstream RPMs).
    remove_script = (
        "pkgs=$(rpm -qa | grep -E '^gpfs\\.nfs-ganesha' || true); "
        'if [ -n "$pkgs" ]; then rpm -e --nodeps $pkgs; fi'
    )
    _run(node, f"bash -lc {shlex.quote(remove_script)}", timeout=timeout)

    # Install from cmake build dir; skip debuginfo/debugsource noise.
    install_script = (
        f"cd {clone_dir}/build && "
        "test -d x86_64 && "
        "rpms=$(find ./x86_64 ./noarch -type f -name '*.rpm' "
        "! -name '*debuginfo*' ! -name '*debugsource*' 2>/dev/null | tr '\\n' ' '); "
        'echo "Installing: $rpms"; '
        'test -n "$rpms" && dnf -y install $rpms'
    )
    _run(node, f"bash -lc {shlex.quote(install_script)}", timeout=timeout)

    _run(
        node,
        "sed -i.bak -e 's/^StateDirectory/#&/' "
        "/usr/lib/systemd/system/nfs-ganesha.service || true",
        timeout=60,
        check=False,
    )
    _run(node, "systemctl daemon-reload", timeout=60)
    # ulimit in this shell does not affect the systemd unit; start the service only.
    _run(node, "systemctl start nfs-ganesha", timeout=timeout)

    out, _ = node.exec_command(
        cmd="systemctl is-active nfs-ganesha", sudo=True, check_ec=False
    )
    if (out or "").strip() != "active":
        _run(
            node,
            "systemctl status nfs-ganesha --no-pager || true",
            check=False,
        )
        raise OperationFailedError(f"nfs-ganesha not active on {node.hostname}")

    log.info("NFS-Ganesha source build/install completed on %s", node.hostname)
    return {
        "ganesha_node": node,
        "mode": "source",
        "git_url": git_url,
        "refspec": refspec,
        "git_repo": git_repo,
        "clone_dir": clone_dir,
    }


def create_nfs_export(ceph_cluster, config=None):
    """
    Enable CES NFS, add Scale export, apply Ganesha/mmnfs settings, restart service.

    Config keys:
        scale_fs: filesystem name (default scale_volume)
        nfs_export: export path (default /ibm/<scale_fs>)
        deploy_timeout: timeout seconds
    """
    conf = config or {}
    timeout = int(conf.get("deploy_timeout", 7200))
    scale_fs = conf.get("scale_fs", DEFAULT_SCALE_FS)
    nfs_export = conf.get("nfs_export") or f"/ibm/{scale_fs}"
    node = resolve_ganesha_node(ceph_cluster)

    log.info("create_nfs_export on %s export=%s", node.hostname, nfs_export)

    _run(node, f"{MMFS_BIN}/mmces service enable nfs", timeout=timeout)
    ganesha_conf_snippet = (
        'grep -q "Graceless" /etc/ganesha/ganesha.conf 2>/dev/null || '
        'echo "NFSv4 { Graceless = true; Enforce_utf8_validation = True; }" '
        ">> /etc/ganesha/ganesha.conf"
    )
    _run(
        node,
        f"bash -lc {shlex.quote(ganesha_conf_snippet)}",
        timeout=60,
        check=False,
    )
    _run(
        node,
        f"{MMFS_BIN}/mmuserauth service create "
        "--data-access-method file --type userdefined || true",
        timeout=timeout,
        check=False,
    )
    _run(
        node,
        f"{MMFS_BIN}/mmnfs export add {nfs_export} "
        '-c "*(Access_Type=RW,Squash=none)"',
        timeout=timeout,
    )

    _run(node, "systemctl stop nfs-ganesha || true", timeout=timeout, check=False)
    _run(
        node,
        f"{MMFS_BIN}/mmnfs config change MINOR_VERSIONS=0,1,2",
        timeout=timeout,
    )
    _run(
        node,
        f"{MMFS_BIN}/mmnfs config change ENFORCE_UTF8_VALIDATION=true",
        timeout=timeout,
    )
    # Device/FS name for mmchfs is the Scale FS name, not the export path.
    _run(
        node,
        f"{MMFS_BIN}/mmchfs {scale_fs} -k nfs4 || "
        f"{MMFS_BIN}/mmchfs /ibm/{scale_fs} -k nfs4 || true",
        timeout=timeout,
        check=False,
    )

    # Drop known duplicate line that breaks ganesha restart (ci-tests workaround).
    fixup = (
        "sleep 30; "
        'sed -i.bak -e "41d" /var/mmfs/ces/nfs-config/gpfs.ganesha.main.conf '
        "2>/dev/null || true; "
        "sleep 15; systemctl daemon-reload"
    )
    _run(node, f"bash -lc {shlex.quote(fixup)}", timeout=timeout, check=False)
    _run(node, "systemctl start nfs-ganesha", timeout=timeout)

    out, _ = node.exec_command(
        cmd="systemctl is-active nfs-ganesha", sudo=True, check_ec=False
    )
    if (out or "").strip() != "active":
        _run(
            node,
            "systemctl status nfs-ganesha --no-pager || "
            "journalctl -u nfs-ganesha --no-pager | tail -80",
            check=False,
        )
        raise OperationFailedError(
            f"nfs-ganesha failed to start after export on {node.hostname}"
        )

    log.info("NFS export ready: %s on %s", nfs_export, node.hostname)
    return {
        "ganesha_node": node,
        "nfs_export": nfs_export,
        "scale_fs": scale_fs,
    }


def deploy_ganesha_stack(ceph_cluster, config=None):
    """Build/install Ganesha then create the Scale-backed export (full NFS stage)."""
    conf = config or {}
    result = {}
    if not conf.get("skip_ganesha"):
        result.update(build_install_ganesha(ceph_cluster, conf))
    else:
        log.info("skip_ganesha set — skipping build/install")
    if not conf.get("skip_export"):
        result.update(create_nfs_export(ceph_cluster, conf))
    else:
        log.info("skip_export set — skipping export create")
    return result
