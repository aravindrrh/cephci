from os import environ
from re import T

from cli.exceptions import OperationFailedError
from utility.log import Log

log = Log(__name__)


def setup_passwordless_ssh(nodes):
    # Setup passwordless SSH between all nodes
    log.info("Setting up passwordless SSH between all nodes")

    # Generate SSH keys on all nodes if they don't exist
    for node in nodes:
        log.info(f"Generating SSH key on {node.hostname}")
        node.exec_command(
            cmd="[ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa",
            sudo=True,
        )

    # Collect all public keys
    public_keys = {}
    for node in nodes:
        log.info(f"Collecting public key from {node.hostname}")
        out, _ = node.exec_command(cmd="cat ~/.ssh/id_rsa.pub", sudo=True)
        public_keys[node.hostname] = out.strip()

    # Distribute all public keys to all nodes
    for node in nodes:
        log.info(f"Distributing public keys to {node.hostname}")
        # Ensure .ssh directory and authorized_keys exist with correct permissions
        node.exec_command(cmd="mkdir -p ~/.ssh && chmod 700 ~/.ssh", sudo=True)
        node.exec_command(
            cmd="touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys",
            sudo=True,
        )

        # Add all public keys to authorized_keys (avoiding duplicates)
        for hostname, pub_key in public_keys.items():
            # Check if key already exists, if not add it
            check_cmd = f"grep -q '{pub_key}' ~/.ssh/authorized_keys || echo '{pub_key}' >> ~/.ssh/authorized_keys"
            node.exec_command(cmd=check_cmd, sudo=True)

        # Disable strict host key checking for seamless SSH
        ssh_config = """Host *
StrictHostKeyChecking no
UserKnownHostsFile=/dev/null"""
        node.exec_command(
            cmd=f"echo '{ssh_config}' > ~/.ssh/config && chmod 600 ~/.ssh/config",
            sudo=True,
        )

    log.info("Passwordless SSH setup completed successfully")


def run(ceph_cluster, **kw):
    clients = ceph_cluster.get_nodes("client")
    log.info("Setup nfs cluster")
    # export_name = environ['EXPORT_NAME']
    export_name = "/ibm/scale_volume"

    try:
        server = ceph_cluster.get_nodes("installer")[0]
        client = ceph_cluster.get_nodes("client")[0]
        node2 = client.ip_address
        node3 =  ceph_cluster.get_nodes("client")[1].ip_address

        # Setup Passwrodless SSH between Nodes
        setup_passwordless_ssh(ceph_cluster.get_nodes())

        cmds = [
            "rm -rf ci-tests/",
            "yum install -y git wget",
            f'echo "export node2=\"{node2}\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export node3=\"{node3}\"" >> ~/.bashrc && source ~/.bashrc',
            "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
            "sh ci-tests/build_scripts/common/basic-storage-scale-multi-node.sh",
        ]  # Copy Gerontion folder to all the clients]

        for cmd in cmds:
            exit_code = server.exec_command(
                cmd=cmd, sudo=True, long_running=True, timeout=3600
            )
            if exit_code != 0:
                raise OperationFailedError(
                    f"LTP server command failed (exit {exit_code}): {cmd}"
                )

        for client in clients:
            # for cmd in ["mkdir /mnt/gpfstests", "curl -O http://10.0.210.156/gpfstests/ /mnt/gpfstests/gerontion"]:
            for cmd in [
                "yum install -y wget",
                "rm -rf /u/gpfstesti/stress/",
                "mkdir -p /u/gpfstest/stress",
                "cd /u/gpfstest/stress && wget -r -np -nH -R 'index.htm*' --cut-dirs=1 -e robots=off http://10.0.210.156/gpfstests/gerontion",
                "chmod +x /u/gpfstest/stress/gerontion/gerontion",
            ]:
                client.exec_command(cmd=cmd, sudo=True)

        # Install pre-req
        for client in clients:
            cmd = (
                "sudo dnf install -y wget git gcc gcc-c++ time make automake autoconf "
                "pkgconf pkgconf-pkg-config libtool bison flex "
                "perl perl-Time-HiRes python3 wget tar libaio-devel net-tools nfs-utils"
            )
            client.exec_command(cmd=cmd, sudo=True)

        mounts = ["/mnt/nfsv3", "/mnt/nfsv4_1"]  # , "/mnt/nfsv4_2"]
        # Perform mount on all client with different mount versions
        for nfs_mount, ver in {
            "/mnt/nfsv3": "3",
            "/mnt/nfsv4_1": "4.1",
        }.items():  # , '/mnt/nfsv4_2':'4.2'}.items():
            cmds = [
                f"mkdir -p {nfs_mount}",
                f"mount -t nfs -o vers={ver} {server.ip_address}:{export_name} {nfs_mount}",
                f"export TESTDIR={nfs_mount}",
            ]
            for client in clients:
                for cmd in cmds:
                    client.exec_command(cmd=cmd, sudo=True)

        # Run gerontion tests

        # Run Racer
        for mount in mounts:
            cmd = f"/u/gpfstest/stress/gerontion/gerontion -N {clients[0].ip_address} -F {mount} racer"
            out = clients[0].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: Racer , Mount : {mount}, result : {out}")

        # Run cdata
        for mount in mounts:
            cmd = f"/u/gpfstest/stress/gerontion/gerontion -N {clients[1].ip_address} -F {mount} cdata"
            clients[1].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: cdata, Mount : {mount}, result : {out}")

        # Run locktest
        for mount in mounts:
            cmd = f"/mnt/gpfstests/gerontion/gerontion -N {clients[1].ip_address} -F {mount} locktest"
            clients[2].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: Locktest, Mount : {mount}, result : {out}")

        # Run blast
        for mount in mounts:
            cmd = f"/u/gpfstest/stress/gerontion/gerontion -N {clients[1].ip_address} -F {mount} blast"
            clients[3].exec_command(cmd=cmd, sudo=True)
            log.info(f"Test: Blast, Mount : {mount}, result : {out}")

    except OperationFailedError:
        raise
    except Exception as e:
        log.error("Gerontion setup/run failed: %s", e)
        raise OperationFailedError(f"Gerontion setup/run failed: {e}") from e
    finally:
        pass
    return 0
