"""SPECstorage benchmark on Spectrum Scale / GPFS NFS (upstream)."""

from cli.exceptions import ConfigError, OperationFailedError
from cli.io.spec_storage import SpecStorage
from tests.nfs.lib.upstream_gpfs_nfs_setup import (
    deploy_gpfs_scale,
    run_suite_cleanup,
    setup_gpfs_nfs,
    should_skip_deployment,
)
from utility.log import Log

log = Log(__name__)

# SPECstorage 2020 workloads used in this suite (see cli/io/spec_storage.py):
#   SWBUILD  — software build (many small files)
#   VDA      — virtual desktop (VDI-like)
#   AI_IMAGE — AI/deep-learning image training
DEFAULT_BENCHMARKS = ("SWBUILD", "VDA", "AI_IMAGE")

# Defaults from nfs_downstream_regression tier-2_nfs_ganesha_spec_storage.yaml
DEFAULT_BENCHMARK_DEFINATION = {
    "Warmup_time": 30,
    "Dir_count": 15,
    "Files_per_dir": 30,
    "File_size": "3k",
    "Instances": 4,
}


def _resolve_benchmarks(config):
    """Return workload list from config ``benchmark`` (str or list)."""
    benchmark = config.get("benchmark")
    if benchmark is None:
        # backward compat
        legacy = config.get("benchmarks")
        if legacy:
            return list(legacy) if isinstance(legacy, (list, tuple)) else [legacy]
        return list(DEFAULT_BENCHMARKS)
    if isinstance(benchmark, (list, tuple)):
        return list(benchmark)
    return [benchmark]


def _benchmark_defination_for(config, benchmark):
    """Per-workload overrides, else suite-wide benchmark_defination, else defaults."""
    per_benchmark = (config.get("benchmark_definations") or {}).get(benchmark)
    if per_benchmark:
        return per_benchmark
    return config.get("benchmark_defination") or DEFAULT_BENCHMARK_DEFINATION


def _setup_spec_storage_ssh(primary_client, clients):
    """Ensure sshpass and root SSH access to SPECstorage client nodes."""
    primary_client.exec_command(cmd="dnf install -y sshpass", sudo=True)
    for client in clients:
        for target in (client.hostname, client.ip_address):
            cmd = (
                "sshpass -p passwd ssh-copy-id -o StrictHostKeyChecking=no "
                f"-f -i ~/.ssh/id_rsa.pub root@{target}"
            )
            primary_client.exec_command(cmd=cmd, sudo=True, check_ec=False)


def run(ceph_cluster, **kw):
    config = kw.get("config") or {}
    clients_all = ceph_cluster.get_nodes("client")
    no_clients = int(config.get("clients", "1"))
    if no_clients > len(clients_all):
        raise ConfigError("The test requires more clients than available")
    clients = clients_all[:no_clients]
    primary_client = clients[0]

    benchmarks = _resolve_benchmarks(config)
    load = config.get("load", "1")
    incr_load = config.get("incr_load", "1")
    num_runs = config.get("num_runs", "1")

    try:
        if not should_skip_deployment(config):
            deploy_gpfs_scale(ceph_cluster, config)

        mount_config = dict(config)
        mount_config["skip_deployment"] = True
        gpfs = setup_gpfs_nfs(ceph_cluster, mount_config)
        nfs_mount = gpfs["nfs_mount"]

        _setup_spec_storage_ssh(primary_client, clients)

        log.info(
            "SPECstorage workloads (single mount, no remount between runs): %s",
            benchmarks,
        )
        spec_storage = SpecStorage(primary_client)
        for benchmark in benchmarks:
            benchmark_defination = _benchmark_defination_for(config, benchmark)
            log.info(
                "Run SPECstorage with %s benchmark on %s (clients=%d)",
                benchmark,
                nfs_mount,
                len(clients),
            )
            spec_storage.run_spec_storage(
                benchmark,
                load,
                incr_load,
                num_runs,
                clients,
                nfs_mount,
                benchmark_defination,
            )
            log.info("SPECstorage %s run completed", benchmark)
        return 0
    except OperationFailedError:
        raise
    except Exception as e:
        log.error("SPECstorage failed: %s", e)
        raise OperationFailedError(f"SPECstorage failed: {e}") from e
    finally:
        run_suite_cleanup(ceph_cluster, config)
