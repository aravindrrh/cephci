from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]

    cmds = ["rm -rf ci-tests/",
            "yum install -y git wget",
            "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
            #"sh ci-tests/build_scripts/common/basic-storage-scale.sh"]
            "sh ci-tests/build_scripts/common/basic-storage-scale-multi-node.sh"]
    for cmd in cmds:
        server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=5400)


    cmds = [f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export EXPORT=\"/ibm/scale_volume\"" >> ~/.bashrc && source ~/.bashrc',
            "rm -rf ci-tests/",
            "yum install -y git wget",
            "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
            "sh ci-tests/build_scripts/storage-scale/client.sh"]

    for cmd in cmds:
        client.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=5400)

    return 0
