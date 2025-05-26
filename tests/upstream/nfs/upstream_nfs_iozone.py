from upstream_nfs_operations import cleanup_cluster, setup_nfs_cluster

from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    config = kw.get("config")
    nfs_mount = config.get("mount_point", "/mnt/nfs")
    clients = ceph_cluster.get_nodes("client")
    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    nfs_nodes = ceph_cluster.get_nodes("installer")
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname

    log.info("Setup nfs cluster")
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
            fs,
            ceph_cluster=ceph_cluster,
        )
        # Install pre-req

        cmd = "sudo dnf install -y wget git gcc gcc-c++ time make automake autoconf " \
              "pkgconf pkgconf-pkg-config libtool bison flex " \
              "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
        clients[0].exec_command(cmd=cmd, sudo=True)

        # Perform mount on client
        cmd = "wget http://www.iozone.org/src/current/iozone3_506.tar;" \
              "tar xvf iozone3_506.tar;cd iozone3_506/src/current/;make;make linux"
        clients[0].exec_command(cmd=cmd, sudo=True)

        io_params = {
            "file_name": None,  # default None, else list of filenames required to test ['file1','file2']
            "io_type": ["all"],
            "file_size": "auto",  # mention alternate size in as 64k/64m/64g for KB/MB/GB
            "max_file_size": "100m",
            "reclen": "auto",  # mention alternate size as 64k/64m/64g for KB/MB/GB
            "cpu_use_report": True,  # to report cpu use by each test
            "spreadsheet": True,  # to copy stats to spreadsheet
            "throughput_test": False,
            "threads": 2,  # to be used when throughput_test is True
            "ext_opts": None,  # other options as "-C -e -K",default is 'None'
        }
        iozone_path = "/root/iozone3_506/src/current/iozone"
        iozone_cmd = f"{iozone_path} -a"
        if io_params["throughput_test"]:
            iozone_cmd = f"{iozone_path} -t {io_params['threads']}"
        if io_params["cpu_use_report"]:
            iozone_cmd += " -+u"
        if io_params["spreadsheet"]:
            iozone_cmd += f" -b {nfs_mount}/iozone_report.xls"
        if "all" not in io_params["io_type"]:
            for io_type in io_params["io_type"]:
                iozone_cmd += f" -i {io_type}"
        if "auto" not in io_params["file_size"]:
            iozone_cmd += f" -s {io_params['file_size']}"
        elif io_params["file_name"] is None:
            iozone_cmd += f" -g {io_params['max_file_size']}"
        if "auto" not in io_params["reclen"]:
            iozone_cmd += f" -r {io_params['reclen']}"
        if io_params.get("ext_opts"):
            iozone_cmd += f" {io_params['ext_opts']}"
        if io_params.get("file_names"):
            thread_len = len(io_params["file_names"])
            iozone_cmd += f" -t {thread_len} -F "
            for file_name in io_params["file_names"]:
                io_path = f"{nfs_mount}/{file_name}"
                iozone_cmd += f" {io_path}"

        cmd = f"cd {nfs_mount};{iozone_cmd}"
        clients[0].exec_command(
        sudo=True,
        cmd=cmd,
        long_running=True,
        timeout=7200
        )
    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        # sleep(30)
        pass
    return 0
