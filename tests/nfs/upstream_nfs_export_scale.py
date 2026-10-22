"""
Scale NFS export stress: create/load 10k GPFS exports, mount, sample IO, cleanup.

Expects Spectrum Scale + nfs-ganesha already deployed (combined suite /
``upstream_nfs_deploy``). Does not redeploy Scale.

Steps:
  1. Backup CES gpfs.ganesha.exports.conf (single-export baseline)
  2. Create N directories under /ibm/<fs>/exp_<i>
  3. Generate export conf with N EXPORT blocks (FSAL=gpfs)
  4. mmnfs export load <conf>
  5. systemctl restart nfs-ganesha; verify active
  6. Mount all exports on a client (NFSv3 by default)
  7. dd IO on a random sample of mounts; verify success
  8-10. Unmount, remove mount dirs / exp_*, restore exports.conf, restart ganesha
"""

import random
import shlex
from time import sleep

from cli.exceptions import ConfigError, OperationFailedError
from tests.nfs.lib.nfs_ganesha_deploy import MMFS_BIN, resolve_ganesha_node
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    run_suite_cleanup,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)

DEFAULT_SCALE_FS = "scale_volume"
DEFAULT_NUM_EXPORTS = 10000
DEFAULT_IO_SAMPLE = 100
DEFAULT_DD_COUNT_MB = 10
DEFAULT_NFS_VERSION = "3"
EXPORT_CONF_NAME = "export10k.conf"
CES_EXPORTS_CONF = "/var/mmfs/ces/nfs-config/gpfs.ganesha.exports.conf"
CES_EXPORTS_CONF_BACKUP = (
    "/var/mmfs/ces/nfs-config/gpfs.ganesha.exports.conf.pre_export_scale"
)


def _scale_base(scale_fs):
    return f"/ibm/{scale_fs}"


def _run(node, cmd, timeout=7200, check=True, long_running=True):
    log.info("[%s] %s", node.hostname, cmd)
    rc = node.exec_command(
        cmd=cmd, sudo=True, long_running=long_running, timeout=timeout
    )
    if check and rc != 0:
        raise OperationFailedError(
            f"Command failed on {node.hostname} (exit {rc}): {cmd}"
        )
    return rc


def _create_export_dirs(server, scale_fs, num_exports, timeout):
    base = _scale_base(scale_fs)
    log.info("Creating %s export directories under %s", num_exports, base)
    _run(server, f"mkdir -p {base}", timeout=timeout)
    # Single remote loop avoids 10k SSH round-trips.
    _run(
        server,
        f"for i in $(seq 1 {num_exports}); do "
        f"mkdir -p {base}/exp_$i; done",
        timeout=timeout,
    )


def _write_export_conf(server, scale_fs, num_exports, export_id_start, conf_path, timeout):
    """Generate export conf on the server (avoids transferring a multi-MB file)."""
    log.info(
        "Writing %s with %s EXPORT blocks (id start=%s)",
        conf_path,
        num_exports,
        export_id_start,
    )
    base = _scale_base(scale_fs)
    # Path, Pseudo, and Tag all use the same export dir: <base>/exp_<i>.
    # Export_id is unique per stanza (required by Ganesha/mmnfs).
    generator = f"""
base = {base!r}
n = {num_exports}
id0 = {export_id_start}
out = {conf_path!r}
hdr = (
    "EXPORT\\n"
    "{{\\n"
    "        Attr_Expiration_Time=60;\\n"
    "        Delegations=none;\\n"
)
mid = (
    "        Filesystem_id=666.666;\\n"
    "        MaxOffsetRead=18446744073709551615;\\n"
    "        MaxOffsetWrite=18446744073709551615;\\n"
    "        MaxRead=1048576;\\n"
    "        MaxWrite=1048576;\\n"
)
fsal = (
    "        UseCookieVerifier=false;\\n"
    "        FSAL\\n"
    "        {{\\n"
    "                Name=gpfs;\\n"
    "        }}\\n"
    "        CLIENT\\n"
    "        {{\\n"
    "                Access_Type=rw;\\n"
    "                Clients=*;\\n"
    "                Squash=none;\\n"
    "        }}\\n"
    "}}\\n"
)
with open(out, "w") as fh:
    for i in range(1, n + 1):
        # Path / Pseudo / Tag must all reference the same exp_<i> directory.
        exp = f"{{base}}/exp_{{i}}"
        fh.write(hdr)
        fh.write(f"        Export_id={{id0 + i - 1}};\\n")
        fh.write(mid)
        fh.write(f'        Path="{{exp}}";\\n')
        fh.write("        PrefRead=1048576;\\n")
        fh.write("        PrefReaddir=1048576;\\n")
        fh.write("        PrefWrite=1048576;\\n")
        fh.write(f'        Pseudo="{{exp}}";\\n')
        fh.write(f'        Tag="{{exp}}";\\n')
        fh.write(fsal)
        fh.write("\\n")
print(out, n)
"""
    _run(server, f"python3 - <<'PY'\n{generator}\nPY", timeout=timeout)
    _run(server, f"test -s {conf_path} && wc -l {conf_path}", timeout=300)
    # Spot-check first and last stanzas: Path == Pseudo == Tag == .../exp_<i>
    _run(
        server,
        f"python3 - <<'PY'\n"
        f"import re, sys\n"
        f"text = open({conf_path!r}).read()\n"
        f"blocks = re.findall(r'EXPORT\\s*\\{{.*?\\}}', text, re.S)\n"
        f"assert len(blocks) == {num_exports}, (len(blocks), {num_exports})\n"
        f"for idx in (0, len(blocks) - 1):\n"
        f"    b = blocks[idx]\n"
        f"    i = idx + 1 if idx == 0 else {num_exports}\n"
        f"    expect = f'{base}/exp_{{i}}'\n"
        f"    for key in ('Path', 'Pseudo', 'Tag'):\n"
        f"        m = re.search(rf'{{key}}=\"([^\"]+)\"', b)\n"
        f"        assert m and m.group(1) == expect, (key, m and m.group(1), expect)\n"
        f"print('verified Path/Pseudo/Tag for exp_1 and exp_{num_exports}')\n"
        f"PY",
        timeout=300,
    )


def _backup_ces_exports_conf(server, exports_conf, backup_path, timeout):
    """Save the pre-scale (typically single-export) CES exports conf."""
    log.info("Backing up %s -> %s", exports_conf, backup_path)
    _run(
        server,
        f"test -f {shlex.quote(exports_conf)} && "
        f"cp -a {shlex.quote(exports_conf)} {shlex.quote(backup_path)}",
        timeout=timeout,
    )
    _run(
        server,
        f"wc -l {shlex.quote(backup_path)} && "
        f"ls -l {shlex.quote(exports_conf)} {shlex.quote(backup_path)}",
        timeout=120,
    )


def _restore_ces_exports_conf(server, exports_conf, backup_path, timeout):
    """Restore the original CES exports conf (single export) from backup."""
    log.info("Restoring %s from %s", exports_conf, backup_path)
    _run(
        server,
        f"test -f {shlex.quote(backup_path)} && "
        f"cp -a {shlex.quote(backup_path)} {shlex.quote(exports_conf)}",
        timeout=timeout,
    )
    _run(
        server,
        f"wc -l {shlex.quote(exports_conf)} && "
        f"rm -f {shlex.quote(backup_path)}",
        timeout=120,
        check=False,
    )


def _load_exports(server, conf_path, timeout):
    log.info("Loading exports via mmnfs export load %s", conf_path)
    _run(
        server,
        f"{MMFS_BIN}/mmnfs export load {conf_path}",
        timeout=timeout,
    )


def _restart_ganesha(server, timeout):
    log.info("Restarting nfs-ganesha on %s", server.hostname)
    _run(server, "systemctl restart nfs-ganesha", timeout=timeout)
    sleep(5)
    out, _ = server.exec_command(
        cmd="systemctl is-active nfs-ganesha", sudo=True, check_ec=False
    )
    state = (out or "").strip()
    if state != "active":
        _run(
            server,
            "systemctl status nfs-ganesha --no-pager || "
            "journalctl -u nfs-ganesha --no-pager | tail -100",
            check=False,
            timeout=120,
        )
        raise OperationFailedError(
            f"nfs-ganesha not active after restart on {server.hostname}: {state!r}"
        )
    log.info("nfs-ganesha is active")


def _mount_all_exports(client, nfs_server_ip, scale_fs, num_exports, version, timeout):
    log.info(
        "Mounting %s exports on %s (server=%s vers=%s)",
        num_exports,
        client.hostname,
        nfs_server_ip,
        version,
    )
    # Path (exp_N) for NFSv3; Pseudo (exportN) is for NFSv4 namespace.
    _run(
        client,
        f"for i in $(seq 1 {num_exports}); do "
        f"mkdir -p /mnt/mount$i && "
        f"mount -t nfs -o vers={version} "
        f"{nfs_server_ip}:{_scale_base(scale_fs)}/exp_$i /mnt/mount$i; "
        f"done",
        timeout=timeout,
    )


def _run_sample_io(client, num_exports, sample_count, dd_count_mb, timeout):
    sample = min(sample_count, num_exports)
    indices = sorted(random.sample(range(1, num_exports + 1), sample))
    log.info("Running dd IO on %s random mounts: %s", sample, indices)
    idx_list = " ".join(str(i) for i in indices)
    script = f"""
set -e
failed=0
for i in {idx_list}; do
  mnt=/mnt/mount$i
  if ! dd if=/dev/urandom of=$mnt/testfile bs=1M count={dd_count_mb} status=none; then
    echo "IO failed on $mnt" >&2
    failed=1
  else
    sz=$(stat -c %s $mnt/testfile 2>/dev/null || echo 0)
    expect=$(( {dd_count_mb} * 1024 * 1024 ))
    if [ "$sz" != "$expect" ]; then
      echo "Size mismatch on $mnt: got $sz expect $expect" >&2
      failed=1
    fi
  fi
done
exit $failed
"""
    _run(client, f"bash -lc {shlex.quote(script)}", timeout=timeout)


def _unmount_and_cleanup_mounts(client, num_exports, timeout):
    log.info("Unmounting and removing /mnt/mount* on %s", client.hostname)
    _run(
        client,
        f"for i in $(seq 1 {num_exports}); do "
        f"umount -f /mnt/mount$i 2>/dev/null || "
        f"umount -l /mnt/mount$i 2>/dev/null || true; "
        f"rm -rf /mnt/mount$i; "
        f"done",
        timeout=timeout,
        check=False,
    )


def _cleanup_export_dirs(server, scale_fs, conf_path, timeout):
    base = _scale_base(scale_fs)
    log.info("Removing %s/exp_* and %s", base, conf_path)
    _run(
        server,
        f"rm -rf {base}/exp_* {conf_path}",
        timeout=timeout,
        check=False,
    )


def run(ceph_cluster, **kw):
    """
    Create/load N Scale NFS exports, mount, sample IO, cleanup.

    Config keys:
        num_exports: number of exports (default 10000)
        io_sample_count: random mounts for dd (default 100)
        dd_count_mb: dd count in MiB (default 10)
        nfs_version: mount vers (default 3)
        scale_fs: GPFS fs name (default scale_volume)
        export_id_start: first Export_id (default 10001 to avoid clash
            with existing mmnfs export1)
        clients: number of clients required (default 1)
        skip_deployment: assume Scale/Ganesha already up
        cmd_timeout / deploy_timeout: remote command timeout seconds
        ces_exports_conf: path to CES exports conf (default
            /var/mmfs/ces/nfs-config/gpfs.ganesha.exports.conf)
        ces_exports_backup: backup path for pre-scale exports conf
        suite_cleanup: run shared mount cleanup in finally (default True)
    """
    config = dict(kw.get("config") or {})
    num_exports = int(config.get("num_exports", DEFAULT_NUM_EXPORTS))
    io_sample = int(config.get("io_sample_count", DEFAULT_IO_SAMPLE))
    dd_count_mb = int(config.get("dd_count_mb", DEFAULT_DD_COUNT_MB))
    version = str(config.get("nfs_version", DEFAULT_NFS_VERSION))
    scale_fs = config.get("scale_fs", DEFAULT_SCALE_FS)
    # Default high so we do not collide with the durable export1 from deploy.
    export_id_start = int(config.get("export_id_start", 10001))
    no_clients = int(config.get("clients", 1))
    timeout = int(config.get("cmd_timeout") or config.get("deploy_timeout") or 14400)
    conf_name = config.get("export_conf", EXPORT_CONF_NAME)
    ces_exports_conf = config.get("ces_exports_conf", CES_EXPORTS_CONF)
    ces_exports_backup = config.get("ces_exports_backup", CES_EXPORTS_CONF_BACKUP)

    if should_skip_deployment(config):
        log.info("skip_deployment set — assuming Scale/Ganesha already up")

    clients = ceph_cluster.get_nodes("client")
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")
    client = clients[0]
    server = resolve_ganesha_node(ceph_cluster)
    nfs_server_ip = server.ip_address
    conf_path = f"/root/{conf_name}"
    backup_taken = False

    log.info(
        "\n"
        + "=" * 70
        + "\n"
        + "  UPSTREAM NFS — export scale (%s exports)\n"
        + "  server=%s (%s)  client=%s  vers=%s\n"
        + "=" * 70,
        num_exports,
        server.hostname,
        nfs_server_ip,
        client.hostname,
        version,
    )

    try:
        _backup_ces_exports_conf(
            server, ces_exports_conf, ces_exports_backup, timeout
        )
        backup_taken = True
        _create_export_dirs(server, scale_fs, num_exports, timeout)
        _write_export_conf(
            server, scale_fs, num_exports, export_id_start, conf_path, timeout
        )
        _load_exports(server, conf_path, timeout)
        _restart_ganesha(server, timeout)
        _mount_all_exports(
            client, nfs_server_ip, scale_fs, num_exports, version, timeout
        )
        _run_sample_io(client, num_exports, io_sample, dd_count_mb, timeout)
        log.info("Export scale test completed successfully")
    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Export scale test failed: %s", e)
        raise OperationFailedError(f"Export scale test failed: {e}") from e
    finally:
        log.info("Cleaning up export-scale artifacts")
        try:
            _unmount_and_cleanup_mounts(client, num_exports, timeout)
        except Exception as e:
            log.warning("Client mount cleanup issue: %s", e)
        try:
            _cleanup_export_dirs(server, scale_fs, conf_path, timeout)
        except Exception as e:
            log.warning("exp_* cleanup issue: %s", e)
        if backup_taken:
            try:
                _restore_ces_exports_conf(
                    server, ces_exports_conf, ces_exports_backup, timeout
                )
            except Exception as e:
                log.warning("CES exports.conf restore issue: %s", e)
        try:
            _restart_ganesha(server, timeout)
        except Exception as e:
            log.warning("nfs-ganesha restart after cleanup: %s", e)
        run_suite_cleanup(ceph_cluster, config)

    return 0
