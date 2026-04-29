import shlex

from nfs_operations import cleanup_cluster, setup_nfs_cluster

from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)


def run(ceph_cluster, **kw):
    """Run pynfs testserver against the NFS export and check results.

    Args:
        **kw: Key/value pairs of configuration information to be used in the test.
    """
    config = kw.get("config")
    nfs_nodes = ceph_cluster.get_nodes("nfs")
    clients = ceph_cluster.get_nodes("client")

    port = config.get("port", "2049")
    version = config.get("nfs_version", "4.0")
    no_clients = int(config.get("clients", "2"))

    # If the setup doesn't have required number of clients, exit.
    if no_clients > len(clients):
        raise ConfigError("The test requires more clients than available")

    clients = clients[:no_clients]  # Select only the required number of clients
    nfs_node = nfs_nodes[0]
    fs_name = "cephfs"
    nfs_name = "cephfs-nfs"
    nfs_export = "/export"
    nfs_mount = "/mnt/nfs"
    fs = "cephfs"
    nfs_server_name = nfs_node.hostname
    pynfs_workdir = None

    try:
        # Setup nfs cluster
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

        out_mk, _ = clients[0].exec_command(
            cmd="mktemp -d /tmp/cephci-pynfs.XXXXXX",
            sudo=True,
        )
        pynfs_workdir = out_mk.strip().splitlines()[-1].strip()
        if not pynfs_workdir.startswith("/tmp/cephci-pynfs."):
            raise OperationFailedError(
                f"Refusing to use unexpected mktemp path: {pynfs_workdir!r}"
            )

        repo = f"{pynfs_workdir}/pynfs"
        qrepo = shlex.quote(repo)
        # Isolate clone/build under /tmp (unique per run); testserver targets the NFS export over the network.
        cmd = (
            "dnf install -y python3-ply && "
            f"git clone git://git.linux-nfs.org/projects/cdmackay/pynfs.git {qrepo} && "
            f"cd {qrepo} && "
            "yes | python setup.py build && "
            f"cd {qrepo}/nfs{version} && "
            f"./testserver.py {nfs_server_name}:{nfs_export}_0 -v --outfile "
            f"~/pynfs.run --maketree --showomit --rundep all"
        )

        out, _ = clients[0].exec_command(cmd=cmd, sudo=True, timeout=600)
        log.info(f"Pynfs Output:\n {out}")

        if "FailureException" in out:
            raise OperationFailedError(f"Failed to run {cmd} on {clients[0].hostname}")

        # Parse test output to detect failures
        failed_tests = []
        allowed_failures = {"EID9", "SEQ6"}

        for line in out.splitlines():
            line = line.strip()
            if line.endswith(": FAILURE"):
                test_id = line.split()[0]
                if test_id not in allowed_failures:
                    failed_tests.append(test_id)

        if failed_tests:
            log.error(f"Unexpected pynfs test failures: {failed_tests}")
            return 1

        log.info("================================================")
        log.info("Pynfs test completed successfully")
        log.info("================================================")
        return 0

    except Exception as e:
        log.error(f"Failed to run pynfs on {clients[0].hostname}, Error: {e}")
        return 1

    finally:
        if pynfs_workdir:
            wd = shlex.quote(pynfs_workdir)
            try:
                clients[0].exec_command(
                    cmd=f"rm -rf -- {wd}",
                    sudo=True,
                    check_ec=False,
                )
            except Exception as exc:
                log.warning("Could not remove pynfs workdir %s: %s", pynfs_workdir, exc)
        cleanup_cluster(clients, nfs_mount, nfs_name, nfs_export, nfs_nodes=nfs_node)
        log.info("Cleaning up successful")
