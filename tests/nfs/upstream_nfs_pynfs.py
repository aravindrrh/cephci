from utility.log import Log
from os import environ
log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]
    skip_deployment = environ['SKIP_DEPLOYMENT']
    export_name = environ['EXPORT_NAME']

    if skip_deployment == "true":
        log.info("Skipping installation and deployment")
    else:
        cmds = ["rm -rf ci-tests/",
                "yum install -y git wget",
                "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
                # "sh ci-tests/build_scripts/common/basic-storage-scale.sh"]
                "sh ci-tests/build_scripts/common/basic-storage-scale-multi-node.sh"]

        for cmd in cmds:
            server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=7200)

    cmds = [f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export EXPORT=\"{export_name}\"" >> ~/.bashrc && source ~/.bashrc',
            "rm -rf ci-tests/",
            "yum install -y git wget",
            "git clone https://github.com/aravindrrh/ci-tests; cd ci-tests; git checkout scale_downstream",
            "sh ci-tests/build_scripts/pynfs/client.sh"]

    for cmd in cmds:
        client.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=7200)

    return 0
