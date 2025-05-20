from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify scale upstream with pynfs
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]

    cmds = ["rm -rf ci-tests/",
            "yum install -y git wget",
            "git clone https://github.com/pranavprakash20/ci-tests.git; cd ci-tests; git checkout scale_downstream",
            "sh ci-tests/build_scripts/common/basic-storage-scale-custom-repo.sh",
            f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export EXPORT=\"/ibm/scale_volume\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export YUM_REPO=\"http://magna002.ceph.redhat.com/ceph-qe-logs/prprakas/scale-nfs-ganesha-v7.repo\" >> ~/.bashrc && source ~/.bashrc',
            "sh ci-tests/ceph/pynfs-client.sh"]
            # "sh $WORKSPACE/ci-tests/build_scripts/common/basic-storage-scale.sh"]
    # if config.get("install_as_non_root_user", False):
    node = ceph_cluster.get_nodes(role="installer")[0]

    # Add *umask 027* to user's ~/.bashrc file
    cmd = "echo *umask 027* >> ~/.bashrc"
    node.exec_command(cmd=cmd, sudo=True)

    # Verify the bashrc file is updated
    cmd = "cat ~/.bashrc"
    out, _ = node.exec_command(cmd=cmd, sudo=True)
    if "*umask 027*" not in out:
        log.error(
            "Failed to update user's bashrc file. Install via non-root user failed"
        )
        return 1
    for cmd in cmds:
        out = server.exec_command(cmd=cmd, sudo=True, long_running=True)
        log.info(out)

    # client_cmd = "sh $WORKSPACE/ci-tests/build_scripts/storage-scale/client.sh"
    # client_cmds = ["subscription-manager register --username=qa@redhat.com --password=MTQj5t3n5K86p3gH",
    #                "subscription-manager auto-attach",
    #                "rm -rf ci-tests/",
    #                "yum install -y git wget",
    #                f'echo "export SERVER=\"{client.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
    #                "git clone https://github.com/pranavprakash20/ci-tests.git; cd ci-tests; git checkout scale_downstream",
    #               "sh ci-tests/build_scripts/storage-scale/client.sh"]
    # for cmd in client_cmds:
    #     client.exec_command(cmd=cmd, sudo=True, long_running=True)

    return 0