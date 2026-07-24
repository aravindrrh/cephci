"""
IBM Spectrum Scale (GPFS) download and cluster deploy for CephCI.

Role model (static Onecloud confs):
  - installer / _admin: runs spectrumscale control plane
  - mgr: Scale cluster / CES-capable nodes
  - nfs: Ganesha node (not used here; see nfs_ganesha_deploy.py)
  - client: NFS test clients (not Scale peers)

Orchestration is Python; remote work uses exec_command for CLI tools.
"""

import re
import shlex

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

DEFAULT_SCALE_FS = "scale_volume"
# Absolute path — CephCI sudo cwd is not reliable across nodes/users.
DEFAULT_DOWNLOAD_DIR = "/root/DOWNLOAD_STORAGE_SCALE"
DEFAULT_S3_BUCKET = "centos-ci"
DEFAULT_VERSION_KEY = "version_to_use.txt"
DEFAULT_NSD_FILE = "/home/nsd1_scale_filedisk"
DEFAULT_NSD_SIZE_MB = 8192
# Written when deploy reuses an existing Scale cluster (same VERSION_TO_USE).
SCALE_REUSE_MARKER = "/root/.cephci_scale_reuse"

SCALE_PREREQ_PACKAGES = (
    "unzip kernel-devel-$(uname -r) kernel-headers-$(uname -r) "
    "cpp gcc gcc-c++ binutils numactl jre make elfutils elfutils-devel "
    "rpcbind sssd-tools openldap-clients bind-utils net-tools "
    "krb5-workstation python3.12 python3-pip"
)

# Product version like 5.2.3 or 5.2.3.1 inside zip names / RPM versions.
_VERSION_RE = re.compile(r"(\d+\.\d+(?:\.\d+){0,3})")


def _run(node, cmd, timeout=7200, check=True):
    """Run a remote command; raise on non-zero when check=True."""
    log.info("[%s] %s", node.hostname, cmd)
    rc = node.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=timeout)
    # long_running returns exit code (int); treat None as failure when checking.
    if check and rc != 0:
        raise OperationFailedError(
            f"Command failed on {node.hostname} (exit {rc}): {cmd}"
        )
    return rc


def _run_out(node, cmd, timeout=600):
    """Run a short command and return stdout stripped."""
    out, _err = node.exec_command(cmd=cmd, sudo=True, timeout=timeout)
    return (out or "").strip()


def resolve_scale_roles(ceph_cluster):
    """
    Resolve installer, Scale (mgr) nodes, and NFS clients from the cluster.

    Installer is always included in scale_nodes even if it lacks an explicit
    ``mgr`` label (Scale admin must be a cluster member).

    Returns:
        dict with installer, scale_nodes, nfs_clients
    """
    installers = ceph_cluster.get_nodes("installer")
    if not installers:
        raise ConfigError("Scale deploy requires a node with role 'installer'")
    installer = installers[0]

    scale_nodes = list(ceph_cluster.get_nodes("mgr") or [])
    if installer not in scale_nodes:
        log.warning(
            "Installer %s is not labeled 'mgr'; adding it to Scale membership",
            installer.hostname,
        )
        scale_nodes.insert(0, installer)
    if not scale_nodes:
        raise ConfigError(
            "Scale deploy requires at least one node with role 'mgr' "
            "(Scale cluster members)"
        )

    nfs_clients = ceph_cluster.get_nodes("client")
    return {
        "installer": installer,
        "scale_nodes": scale_nodes,
        "nfs_clients": nfs_clients,
    }


def normalize_scale_version(value):
    """
    Extract a comparable Scale product version from a zip name or RPM version.

    Examples:
      Storage_Scale_...-5.2.3.1-...zip -> 5.2.3.1
      gpfs.base-5.2.3-x -> 5.2.3
    """
    if not value:
        return ""
    match = _VERSION_RE.search(str(value).strip())
    return match.group(1) if match else ""


def scale_versions_equal(left, right):
    """
    True when two Scale version strings denote the same product version.

    Trailing .0 components are ignored so zip ``6.0.1.0`` matches RPM ``6.0.1``.
    """
    a = normalize_scale_version(left)
    b = normalize_scale_version(right)
    if not a or not b:
        return False
    pa = [int(x) for x in a.split(".")]
    pb = [int(x) for x in b.split(".")]
    n = max(len(pa), len(pb))
    pa.extend([0] * (n - len(pa)))
    pb.extend([0] * (n - len(pb)))
    return pa == pb


def _ensure_aws_cli(installer, timeout):
    """Install AWS CLI v2 if missing; raise if still unavailable."""
    _run(
        installer,
        "bash -lc 'command -v aws >/dev/null || ("
        "cd /tmp && curl -sS "
        '"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" '
        "-o awscliv2.zip && unzip -qq -o awscliv2.zip && ./aws/install)'",
        timeout=timeout,
        check=False,
    )
    aws_path = _run_out(installer, "bash -lc 'command -v aws || true'", timeout=60)
    if not aws_path:
        raise OperationFailedError(
            f"aws CLI not available on {installer.hostname} after install attempt"
        )


def fetch_version_to_use(installer, config=None):
    """
    Fetch S3 version_to_use.txt and return VERSION_TO_USE (installer zip name).

    Does not download the Scale installer zip itself.
    """
    conf = config or {}
    timeout = int(conf.get("deploy_timeout", 7200))
    work_dir = conf.get("scale_download_dir", DEFAULT_DOWNLOAD_DIR)
    bucket = conf.get("scale_s3_bucket", DEFAULT_S3_BUCKET)
    version_key = conf.get("scale_s3_version_key", DEFAULT_VERSION_KEY)
    source = str(conf.get("scale_installer_source", "s3")).lower()

    if source == "nfs_share":
        share_path = conf.get("scale_nfs_share_path")
        if not share_path:
            raise ConfigError(
                "scale_installer_source=nfs_share requires scale_nfs_share_path"
            )
        # Zip basename is the desired version artifact name.
        return _run_out(
            installer,
            f"bash -lc {shlex.quote(f'basename {share_path}')}",
            timeout=60,
        )

    if source != "s3":
        raise ConfigError(f"Unsupported scale_installer_source: {source}")

    _run(installer, f"mkdir -p {work_dir}", timeout=timeout)
    _ensure_aws_cli(installer, timeout)
    script = (
        f"cd {work_dir} && "
        f'aws s3api get-object --bucket {bucket} --key "{version_key}" '
        f"version_to_use.txt >/dev/null && "
        "cat version_to_use.txt"
    )
    version = _run_out(installer, f"bash -lc {shlex.quote(script)}", timeout=timeout)
    if not version:
        raise OperationFailedError(
            f"Empty VERSION_TO_USE from s3://{bucket}/{version_key}"
        )
    log.info("VERSION_TO_USE=%s", version)
    return version


def get_installed_scale_version(installer):
    """Return installed Scale version string from gpfs.base RPM, or empty."""
    out = _run_out(
        installer,
        "bash -lc \"rpm -q --qf '%{VERSION}' gpfs.base 2>/dev/null || true\"",
        timeout=60,
    )
    if out and "not installed" not in out.lower():
        return out
    # Fallback: versioned tree under /usr/lpp/mmfs/<ver>
    out = _run_out(
        installer,
        "bash -lc \"ls -1 /usr/lpp/mmfs 2>/dev/null | "
        "grep -E '^[0-9]+\\.[0-9]+' | head -1 || true\"",
        timeout=60,
    )
    return out or ""


def scale_cluster_healthy(installer):
    """True when GPFS core binaries exist and mmlscluster succeeds."""
    script = (
        "export PATH=\"$PATH:/usr/lpp/mmfs/bin\"; "
        "if ! test -x /usr/lpp/mmfs/bin/mmlscluster; then echo no; exit 0; fi; "
        "if mmlscluster >/dev/null 2>&1; then echo yes; else echo no; fi"
    )
    out = _run_out(installer, f"bash -lc {shlex.quote(script)}", timeout=120)
    return out.strip() == "yes"


def scale_residue_present(installer):
    """True if Scale RPMs, /var/mmfs, or toolkit bits look present."""
    checks = (
        "rpm -q gpfs.base >/dev/null 2>&1 && echo yes",
        "test -d /var/mmfs && echo yes",
        "test -d /usr/lpp/mmfs && echo yes",
        "ls /usr/lpp/mmfs/*/ansible-toolkit >/dev/null 2>&1 && echo yes",
    )
    for check in checks:
        out = _run_out(installer, f"bash -lc {shlex.quote(check + ' || true')}", timeout=60)
        if out.strip() == "yes":
            return True
    return False


def should_reuse_existing_scale(ceph_cluster, config=None):
    """
    Decide whether to skip Scale install and reuse the existing cluster.

    Returns:
        (reuse: bool, version: str) — version is the normalized product version
        when reusable, else "".

    Config:
      force_scale_redeploy: if true, never reuse
    """
    conf = config or {}
    if conf.get("force_scale_redeploy"):
        log.info("force_scale_redeploy set — will not reuse existing Scale")
        return False, ""
    if conf.get("skip_scale"):
        return False, ""

    roles = resolve_scale_roles(ceph_cluster)
    installer = roles["installer"]

    desired_raw = fetch_version_to_use(installer, conf)
    installed_raw = get_installed_scale_version(installer)
    desired = normalize_scale_version(desired_raw)
    installed = normalize_scale_version(installed_raw)

    log.info(
        "Scale version check: VERSION_TO_USE=%s (%s) installed=%s (%s)",
        desired_raw,
        desired or "?",
        installed_raw or "(none)",
        installed or "?",
    )

    if not desired or not installed or not scale_versions_equal(desired, installed):
        return False, desired
    if not scale_cluster_healthy(installer):
        log.warning(
            "Versions match (%s ~ %s) but Scale cluster is not healthy — will redeploy",
            desired,
            installed,
        )
        return False, desired
    log.info(
        "Reusing existing Scale cluster (VERSION_TO_USE=%s ~ installed=%s)",
        desired,
        installed,
    )
    return True, installed


def write_scale_reuse_marker(installer, version):
    """Persist reuse decision for suite cleanup."""
    body = f"echo {shlex.quote(str(version))} > {SCALE_REUSE_MARKER}"
    _run(installer, f"bash -lc {shlex.quote(body)}", timeout=60)


def clear_scale_reuse_marker(installer):
    """Remove reuse marker (full Scale install path)."""
    _run(
        installer,
        f"bash -lc {shlex.quote(f'rm -f {SCALE_REUSE_MARKER}')}",
        timeout=60,
        check=False,
    )


def scale_reuse_marker_present(installer):
    """True if deploy left a reuse marker on the installer."""
    out = _run_out(
        installer,
        f"bash -lc {shlex.quote(f'test -f {SCALE_REUSE_MARKER} && echo yes || true')}",
        timeout=60,
    )
    return out.strip() == "yes"


def download_scale(installer, config=None):
    """
    Download the Scale installer artifact onto the installer node.

    Config keys:
        scale_download_dir: work dir (default /root/DOWNLOAD_STORAGE_SCALE)
        scale_installer_source: ``s3`` (default) or ``nfs_share``
        scale_s3_bucket: S3 bucket (default centos-ci)
        scale_s3_version_key: key for version file (default version_to_use.txt)
        scale_nfs_share_path: path when source is nfs_share
        deploy_timeout: command timeout seconds

    Assumes AWS_ACCESS_KEY / AWS_SECRET_KEY are already set on the target for S3.
    """
    conf = config or {}
    timeout = int(conf.get("deploy_timeout", 7200))
    work_dir = conf.get("scale_download_dir", DEFAULT_DOWNLOAD_DIR)
    source = str(conf.get("scale_installer_source", "s3")).lower()
    bucket = conf.get("scale_s3_bucket", DEFAULT_S3_BUCKET)
    version_key = conf.get("scale_s3_version_key", DEFAULT_VERSION_KEY)

    log.info(
        "download_scale on %s (source=%s dir=%s)",
        installer.hostname,
        source,
        work_dir,
    )
    _run(installer, f"mkdir -p {work_dir}", timeout=timeout)
    _run(
        installer,
        "yum install -y unzip curl || dnf install -y unzip curl",
        timeout=timeout,
    )

    if source == "nfs_share":
        share_path = conf.get("scale_nfs_share_path")
        if not share_path:
            raise ConfigError(
                "scale_installer_source=nfs_share requires scale_nfs_share_path"
            )
        copy_script = (
            f"cp -f {share_path} {work_dir}/ && "
            f"cd {work_dir} && rm -rf INSTALLER_PATH && mkdir -p INSTALLER_PATH && "
            f"unzip -o $(basename {share_path}) -d INSTALLER_PATH/"
        )
        _run(
            installer,
            f"bash -lc {shlex.quote(copy_script)}",
            timeout=timeout,
        )
        return {"work_dir": work_dir, "source": source}

    if source != "s3":
        raise ConfigError(f"Unsupported scale_installer_source: {source}")

    # Install AWS CLI v2 only when missing (creds already exported on target).
    _ensure_aws_cli(installer, timeout)

    s3_script = (
        f"cd {work_dir} && "
        f'aws s3api get-object --bucket {bucket} --key "{version_key}" '
        f"version_to_use.txt && "
        "VERSION_TO_USE=$(cat version_to_use.txt) && "
        'echo "VERSION_TO_USE=$VERSION_TO_USE" && '
        f'aws s3api get-object --bucket {bucket} --key "$VERSION_TO_USE" '
        '"$VERSION_TO_USE" && '
        "rm -rf INSTALLER_PATH && mkdir -p INSTALLER_PATH && "
        'unzip -o "$VERSION_TO_USE" -d INSTALLER_PATH/'
    )
    _run(installer, f"bash -lc {shlex.quote(s3_script)}", timeout=timeout)
    return {"work_dir": work_dir, "source": source, "bucket": bucket}


def _allocate_ces_ip(installer, config):
    """
    Return CES IP: from config ``ces_ip`` or by scanning free IPs on eth0.

    Also appends ``<ip> cesip1`` to /etc/hosts on the installer when not already present.
    """
    conf = config or {}
    ces_ip = conf.get("ces_ip")
    if ces_ip:
        log.info("Using configured ces_ip=%s", ces_ip)
    else:
        # Same approach as basic-storage-scale-multi-node.sh (first free IP in subnet).
        discover_script = (
            "ip_address=$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1)\n"
            "USABLE_IP=\n"
            "for new_ip in $(echo \"$ip_address\" | awk -F. "
            "'{for(i=$4+1;i<=255;i++){print $1\".\"$2\".\"$3\".\"i}}'); do\n"
            "  if ! ping -c 1 -W 1 \"$new_ip\" >/dev/null 2>&1; then\n"
            "    USABLE_IP=$new_ip\n"
            "    break\n"
            "  fi\n"
            "done\n"
            "echo \"$USABLE_IP\"\n"
        )
        ces_ip = _run_out(
            installer, f"bash -lc {shlex.quote(discover_script)}", timeout=3600
        )
        if not ces_ip:
            raise OperationFailedError(
                f"Failed to find a free CES IP on {installer.hostname}"
            )
        log.info("Discovered CES IP (cesip1)=%s", ces_ip)

    hosts_cmd = (
        f"grep -qE '[[:space:]]cesip1([[:space:]]|$)' /etc/hosts || "
        f"echo '{ces_ip}    cesip1' >> /etc/hosts"
    )
    _run(installer, f"bash -lc {shlex.quote(hosts_cmd)}", timeout=60)
    return ces_ip


def _run_scale_installer_binary(installer, work_dir, timeout):
    """Execute the silent Scale installer from the download directory."""
    install_script = (
        f"cd {work_dir} && "
        'INSTALLER_VERSION=$(ls INSTALLER_PATH/ --ignore="*.md5" '
        '--ignore="*.README" --ignore="*.pgp" | head -1) && '
        'test -n "$INSTALLER_VERSION" && '
        'INSTALLER=$(readlink -f INSTALLER_PATH/${INSTALLER_VERSION}) && '
        'chmod +x "$INSTALLER" && "$INSTALLER" --silent'
    )
    _run(installer, f"bash -lc {shlex.quote(install_script)}", timeout=timeout)


def _spectrumscale_path_export():
    """Shell snippet to put spectrumscale ansible-toolkit on PATH."""
    return (
        'export PATH="$PATH:$(readlink -f /usr/lpp/mmfs/*/ansible-toolkit/ '
        '2>/dev/null | head -1)"'
    )


def deploy_spectrum_scale(ceph_cluster, config=None):
    """
    Download (unless skipped) and deploy a multi-node Spectrum Scale cluster.

    Config keys:
        skip_download: if true, assume installer already under scale_download_dir
        scale_fs: filesystem name (default scale_volume)
        ces_ip: optional fixed CES IP (else auto-discover)
        nsd_file / nsd_size_mb: file-backed NSD path and size
        deploy_timeout: per-command timeout
        scale_installer_source / scale_s3_* / scale_nfs_share_path: see download_scale
    """
    conf = config or {}
    timeout = int(conf.get("deploy_timeout", 7200))
    scale_fs = conf.get("scale_fs", DEFAULT_SCALE_FS)
    work_dir = conf.get("scale_download_dir", DEFAULT_DOWNLOAD_DIR)
    nsd_file = conf.get("nsd_file", DEFAULT_NSD_FILE)
    nsd_size = int(conf.get("nsd_size_mb", DEFAULT_NSD_SIZE_MB))

    roles = resolve_scale_roles(ceph_cluster)
    installer = roles["installer"]
    scale_nodes = roles["scale_nodes"]

    log.info(
        "deploy_spectrum_scale: installer=%s scale_nodes=%s fs=%s",
        installer.hostname,
        [n.hostname for n in scale_nodes],
        scale_fs,
    )

    # Prereqs on all Scale members (idempotent yum/dnf).
    for node in scale_nodes:
        _run(
            node,
            f"yum install -y {SCALE_PREREQ_PACKAGES} --skip-broken || "
            f"dnf install -y {SCALE_PREREQ_PACKAGES} --skip-broken",
            timeout=timeout,
            check=False,
        )

    if not conf.get("skip_download"):
        download_scale(installer, conf)
    else:
        log.info("skip_download set — using existing installer under %s", work_dir)

    ces_ip = _allocate_ces_ip(installer, conf)
    _run_scale_installer_binary(installer, work_dir, timeout)

    path_export = _spectrumscale_path_export()
    installer_ip = installer.ip_address
    cluster_name = f"{installer.hostname.split('.')[0]}_cluster"

    def ss(cmd):
        full = f"{path_export}; {cmd}"
        _run(installer, f"bash -lc {shlex.quote(full)}", timeout=timeout)

    # Order aligned with basic-storage-scale-multi-node.sh
    ss(f"spectrumscale setup -s {installer_ip} --storesecret")
    for node in scale_nodes:
        ss(f"spectrumscale node add {node.hostname} -n")
    for node in scale_nodes:
        ss(f"spectrumscale node add {node.hostname} -p")
    ss(f"spectrumscale config protocols -e {ces_ip}")
    ss(f"spectrumscale node add -a {installer.hostname}")
    ss(f"spectrumscale config gpfs -c {cluster_name}")

    # File-backed NSD on installer (recreate so redeploy is safe).
    _run(installer, f"rm -f {nsd_file}", timeout=60, check=False)
    _run(
        installer,
        f"dd if=/dev/zero of={nsd_file} bs=1M count={nsd_size}",
        timeout=timeout,
    )
    ss(
        f"spectrumscale nsd add -p {installer.hostname} -u dataAndMetadata "
        f"-fs {scale_fs} -fg 1 {nsd_file}"
    )
    ss(f"spectrumscale config protocols -f {scale_fs} -m /ibm/{scale_fs}")
    ss("spectrumscale enable nfs")
    ss("spectrumscale enable smb")
    ss("spectrumscale callhome disable")
    ss("spectrumscale config perfmon -r off")
    ss("spectrumscale node list")

    ss("spectrumscale install --precheck")
    ss("spectrumscale install")
    ss("spectrumscale deploy --precheck")
    ss("spectrumscale deploy")
    ss("spectrumscale nsd list")
    ss("spectrumscale filesystem list")

    log.info("Spectrum Scale deploy completed (fs=/ibm/%s ces_ip=%s)", scale_fs, ces_ip)
    return {
        "installer": installer,
        "scale_nodes": scale_nodes,
        "nfs_clients": roles["nfs_clients"],
        "scale_fs": scale_fs,
        "ces_ip": ces_ip,
        "work_dir": work_dir,
    }
