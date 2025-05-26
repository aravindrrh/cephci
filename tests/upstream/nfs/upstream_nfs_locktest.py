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
        # Perform mount on client
        cmds = ["dnf -y install git wget gcc nfs-utils time make",
                "mkdir -p /mnt/nfsv4",
                f"mount -t nfs -o vers=4 {nfs_node.ip_address}:export_1 /mnt/nfsv4"
                ]

        for cmd in cmds:
            clients[0].exec_command(cmd=cmd, sudo=True)
            clients[1].exec_command(cmd=cmd, sudo=True)

        client = clients[0]  # Run setup from the first client

        nfstest_repo = "git://git.linux-nfs.org/projects/mora/nfstest.git"
        nfstest_dir = "/root/nfstest"
        nfstest_lock = f"{nfstest_dir}/test/nfstest_lock"
        export = config.get("nfs_export", "/ibm/scale_volume")
        version = config.get("nfs_version", 3)

        try:
            log.info(">>> Installing required packages...")
            install_cmd = (
                "dnf install -y git python3 python3-devel tcpdump "
                "wireshark sshpass firewalld"
            )
            client.exec_command(cmd=install_cmd, sudo=True)

            log.info(">>> Enabling and configuring firewalld...")
            client.exec_command(cmd="systemctl enable firewalld --now", sudo=True)
            client.exec_command(
                cmd="firewall-cmd --zone=public --add-port=9900-9920/tcp --permanent", sudo=True
            )
            client.exec_command(cmd="firewall-cmd --reload", sudo=True)
            client.exec_command(cmd="firewall-cmd --zone=public --list-ports", sudo=True)

            log.info(">>> Cloning nfstest repo...")
            client.exec_command(cmd=f"git clone {nfstest_repo} {nfstest_dir}", sudo=True)

            log.info(">>> Configuring PYTHONPATH...")
            bashrc_path = "~/.bashrc"
            export_line = f"export PYTHONPATH={nfstest_dir}"
            grep_cmd = f"grep -qxF '{export_line}' {bashrc_path} || echo '{export_line}' >> {bashrc_path}"
            client.exec_command(cmd=grep_cmd, sudo=True)
            client.exec_command(cmd=f"export PYTHONPATH={nfstest_dir}", sudo=True)

            log.info(">>> Verifying nfstest_lock exists...")
            client.exec_command(cmd=f"ls {nfstest_lock}", sudo=True)

            for version in ['4', '4.1']:
                log.info(f">>> Running  nfstest_lock sanity test for V{version}")
                test_cmd = (
                    f"{nfstest_lock} --server {nfs_node .ip_address} --export export_1 "
                    f"--nfsversion {version} --createlog"
                )
                out, err = client.exec_command(cmd=test_cmd, sudo=True, timeout=7200)
                log.info(out)
                log.info(err)
            log.info("NFS locking test completed successfully.")
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            return 1
        return 0

    except Exception as e:
        log.error(f"Error : {e}")
        return 1
    finally:
        # sleep(30)
        pass
