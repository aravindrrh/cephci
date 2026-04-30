from time import sleep

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Run a dummy test"""
    sleep(30)
    log.info("Dummy test passed")
    return 0