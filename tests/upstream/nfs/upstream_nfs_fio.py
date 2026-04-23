"""
FIO workload against CephFS-backed NFS-Ganesha (upstream manual Ganesha layout).

Uses the same cluster setup semantics as upstream_nfs_cthon.py / upstream_nfs_ltp.py
(setup_nfs_cluster from upstream_nfs_operations).
"""

from cli.exceptions import OperationFailedError
from upstream_nfs_operations import setup_nfs_cluster

from utility.log import Log

log = Log(__name__)

_FIO_JOB = """[global]
ioengine=libaio
direct=1
runtime={runtime}
time_based
size={size}
directory={directory}
group_reporting

[seqwrite]
rw=write
bs=1M

[seqread]
rw=read
bs=1M

[randrw]
rw=randrw
rwmixread=70
bs=4k
numjobs=4"""


def _nfs_v4_version_string(version):
    """Normalize suite nfs_version (scalar or multiclient list) for mount -o vers=."""
    if isinstance(version, list) and version:
        entry = version[0]
        if isinstance(entry, dict):
            return str(list(entry.keys())[0])
    return str(version)


def _umount_lazy(client, path):
    client.exec_command(sudo=True, cmd=f"umount -l {path}", check_ec=False)


def _run_fio(client, directory, runtime, size):
    job = _FIO_JOB.format(runtime=runtime, size=size, directory=directory)
    fio_path = f"{directory}/ganesha_test.fio"
    cmd = f"cat <<'EOF' > {fio_path}\n{job}\nEOF"
    client.exec_command(cmd=cmd, sudo=True)
    output = f"{directory}/fio_results.json"
    cmd = f"fio {fio_path} --output={output} --output-format=json"
    client.exec_command(
        cmd=cmd,
        sudo=True,
        long_running=True,
        timeout=10400,
        check_ec=True,
    )
    out, _ = client.exec_command(cmd=f"cat {output}", sudo=True)
    snippet = (out or "")[:4000]
    log.info("FIO JSON output (truncated): %s", snippet)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    nfs_mount = config.get("mount_point", "/mnt/nfs")
    clients = ceph_cluster.get_nodes("client")
    port = str(config.get("port", "2049"))
    version = config.get("nfs_version", "4.2")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    fio_runtime = int(config.get("fio_runtime", 60))
    fio_size = config.get("fio_size", "1G")
    export_pseudo = config.get("fio_export_pseudo", "/export_0")
    mount_v3 = config.get("mount_point_v3", "/mnt/nfs_fio_v3")
    mount_v4 = config.get("mount_point_v4", "/mnt/nfs_fio_v4")
    run_v3 = bool(config.get("run_fio_nfsv3", True))

    if not clients:
        raise OperationFailedError("upstream_nfs_fio requires at least one client node")

    client0 = clients[0]
    server_ip = nfs_node.ip_address
    v4_vers = _nfs_v4_version_string(version)
    mounts_to_clean = []

    try:
        log.info("Setup nfs cluster (upstream manual Ganesha + CephFS)")
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

        client0.exec_command(cmd="dnf install -y fio libaio", sudo=True)

        if run_v3:
            client0.exec_command(cmd=f"mkdir -p {mount_v3}", sudo=True)
            cmd_v3 = (
                f"mount -t nfs -o vers=3,port={port},addr={server_ip} "
                f"{server_ip}:{export_pseudo} {mount_v3}"
            )
            client0.exec_command(cmd=cmd_v3, sudo=True)
            mounts_to_clean.append(mount_v3)
            log.info("Running FIO on NFSv3 mount %s", mount_v3)
            _run_fio(client0, mount_v3, fio_runtime, fio_size)

        client0.exec_command(cmd=f"mkdir -p {mount_v4}", sudo=True)
        cmd_v4 = (
            f"mount -t nfs -o vers={v4_vers},port={port},addr={server_ip} "
            f"{server_ip}:{export_pseudo} {mount_v4}"
        )
        client0.exec_command(cmd=cmd_v4, sudo=True)
        mounts_to_clean.append(mount_v4)
        log.info("Running FIO on NFSv%s mount %s", v4_vers, mount_v4)
        _run_fio(client0, mount_v4, fio_runtime, fio_size)

    except Exception as e:
        log.error("FIO run failed: %s", e)
        return 1
    finally:
        for m in reversed(mounts_to_clean):
            _umount_lazy(client0, m)

    return 0
