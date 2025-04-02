from cli.cephadm.cephadm import CephAdm
from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Mount, Unmount
from utility.log import Log

log = Log(__name__)
SSH = "~/.ssh"
SSH_KEYGEN = f"ssh-keygen -b 2048 -f {SSH}/id_rsa -t rsa -q -N ''"
SSH_COPYID = "ssh-copy-id -f -i {} {}@{}"
CEPH_PUB_KEY = "/etc/ceph/ceph.pub"


def run(ceph_cluster, **kw):
    """Verify readdir ops
    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """

    config = kw.get("config")
    installer = ceph_cluster.get_nodes("installer")[0]

    clients = ceph_cluster.get_nodes("client")
    servers = set(ceph_cluster.get_nodes("")) - set(clients)

    # Setup Passwordless SSH to other nodes
    # installer.exec_command(cmd=SSH_KEYGEN)

    # for node in ceph_cluster.get_nodes(""):
    #     if node != installer:
    #         installer.exec_command(sudo=True, cmd=SSH_COPYID.format(CEPH_PUB_KEY, "root", node.ip_address))

    cmds = ['dnf -y install centos-release-ceph epel-release',
            'yum install -y git bison cmake dbus-devel flex gcc-c++ krb5-devel libacl-devel libblkid-devel '
            'libcap-devel redhat-rpm-config rpm-build xfsprogs-devel',
            'yum install --enablerepo=crb -y libnsl2-devel libnfsidmap-devel libwbclient-devel userspace-rcu-devel',
            'yum install -y libcephfs-devel',
            'rm -rf nfs-ganesha',
            'git clone --depth=1 https://review.gerrithub.io/ffilz/nfs-ganesha',
            'cd $(basename "ffilz/nfs-ganesha"); git fetch origin refs/heads/next && git checkout FETCH_HEAD; git '
            'submodule update --recursive --init || git submodule sync ; [ -d build ] && rm -rf build ; mkdir build ; '
            'cd build ; ( cmake ../src -DCMAKE_BUILD_TYPE=Maintainer -DUSE_FSAL_GLUSTER=OFF -DUSE_FSAL_CEPH=ON '
            '-DUSE_FSAL_RGW=OFF -DUSE_DBUS=ON -DUSE_ADMIN_TOOLS=ON && make) || touch FAILED ; make install',
            'dnf install -y cephadm',
            'cephadm add-repo --release squid',
            'dnf install -y ceph',
            f'cephadm bootstrap --mon-ip {installer.ip_address}',
            'ceph orch apply osd --all-available-devices',
            'sleep 30',
            'ceph fs volume create cephfs',
            'touch /etc/ganesha/ganesha.conf'
    ]
    for cmd in cmds:
        _ = installer.exec_command(cmd=cmd, sudo=True)

    for node in clients:
        node.exec_command(cmd="yum install nfs-utils", sudo=True)

    return 0
