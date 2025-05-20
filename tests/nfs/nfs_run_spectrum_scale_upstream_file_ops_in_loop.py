from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Verify scale upstream with custom test scenarios
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    server = ceph_cluster.get_nodes("installer")[0]
    client = ceph_cluster.get_nodes("client")[0]

    cmds = ["rm -rf ci-tests/",
            "yum install -y git wget",
            "git clone https://github.com/pranavprakash20/ci-tests.git; cd ci-tests; git checkout scale_downstream",
            "sh ci-tests/build_scripts/common/basic-storage-scale-custom-repo.sh",
            f'echo "export SERVER=\"{server.ip_address}\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export EXPORT=\"/ibm/scale_volume\"" >> ~/.bashrc && source ~/.bashrc',
            f'echo "export YUM_REPO=\"http://magna002.ceph.redhat.com/ceph-qe-logs/manim/repo/nfs-ganesha-v7.repo\"" >> ~/.bashrc && source ~/.bashrc']
    for cmd in cmds:
        out = server.exec_command(cmd=cmd, sudo=True, long_running=True)
        log.info(out)

    mount_path = "/mnt/nfsv4"
    EXPORT = "/ibm/scale_volume"
    cmds = ["dnf -y install git gcc nfs-utils time make",
            "subscription-manager repos --enable codeready-builder-for-rhel-$(rpm -E %rhel)-$(uname -m)-rpms",
            "dnf -y install epel-release libtirpc-devel --skip-broken",
            'echo "/tmp/cores/core.%e.%p.%h.%t" > /proc/sys/kernel/core_pattern',
            'mkdir -p /tmp/cores',
            "mkdir - p /mnt/nfsv4",
            f"mount -t nfs -o vers=4 {server.ip_address}:{EXPORT} {mount_path}"
            ]
    for cmd in cmds:
        out = server.exec_command(cmd=cmd, sudo=True, long_running=True)
        log.info(out)

    # Test Scenario : 1
    test_file = """#!/usr/bin/sh

dir="$1"

if [ -z "$dir" ]; then
 echo "Usage: $0 <directory>"
 exit 1
fi

while true; do
 dd if=/dev/random of="$dir/testfile.txt" bs=1k count=1
 echo "Created file testfile.txt"

 # Listing contents quietly
 ls -lrt "$dir" > /dev/null

 rm -f "$dir/testfile.txt"
 echo "Deleted file testfile.txt"

done
"""
    cmd = f"touch {mount_path}/cr_rm_loop.sh"
    server.exec_command(cmd=cmd, sudo=True, long_running=True)
    with server.remote_file(sudo=True, file_name=f"{mount_path}/cr_rm_loop.sh", file_mode="w") as _f:
        _f.write(test_file)
    out = server.exec_command(cmd=f"sh {mount_path}/cr_rm_loop.sh {mount_path}", sudo=True, long_running=True)
    log.info(out)

    return 0
