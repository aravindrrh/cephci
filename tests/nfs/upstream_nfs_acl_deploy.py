"""
Spectrum Scale / NFS deployment stage for the GPFS upstream ACL suite.

Delegates to the shared multi-node deploy in upstream_gpfs_nfs_setup.
Later ACL modules should set ``skip_deployment: true`` in config.
"""

from tests.nfs.upstream_nfs_deploy import run
