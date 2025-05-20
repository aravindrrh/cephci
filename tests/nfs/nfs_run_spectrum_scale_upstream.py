from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify file lock operation
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    server = ceph_cluster.get_nodes("installer")[0]

    cmds = ["rm -rf ci-tests/",
            "yum install -y git wget",
            "git clone https://github.com/pranavprakash20/ci-tests.git; cd ci-tests; git checkout scale_downstream",
            "sh ci-tests/build_scripts/common/basic-storage-scale.sh"]

    for cmd in cmds:
        server.exec_command(cmd=cmd, sudo=True, long_running=True, timeout="notimeout")

    log.info("Completed the scale build and installation check")

    return 0
