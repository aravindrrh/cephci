import csv
import os
import xml.etree.ElementTree as ET

from cli import Cli


class SpecStorageError(Exception):
    pass


class SpecStorage(Cli):
    INSTALL_PREREQS = "dnf install -y wget tar gzip sshpass"

    def __init__(self, primary_client):
        super(SpecStorage, self).__init__(primary_client)
        self.primary_client = primary_client
        self.config = "sfs_rc"
        self.install_dest = "/root/specStorage"
        self.sm2020 = f"{self.install_dest}/SPECstorage2020/SM2020"
        self.base_cmd = f"python3 {self.sm2020}"
        self.outputlog = "result"
        self.benchmark_file = "storage2020.yml"
        self.install_loc = "http://magna002.ceph.redhat.com/spec_storage/"

    def _execute_checked(self, cmd, timeout=3600, long_running=False):
        """Run a command and fail fast when exit code is non-zero."""
        # Cli.execute() does not forward verbose=; call the node directly.
        out, err, exit_code, _duration = self.primary_client.exec_command(
            sudo=True,
            cmd=cmd,
            verbose=True,
            timeout=timeout,
            long_running=long_running,
            check_ec=False,
        )
        if exit_code != 0:
            raise SpecStorageError(
                f"Command failed (exit {exit_code}): {cmd}\nstdout: {out}\nstderr: {err}"
            )
        return out, err

    def _verify_install_layout(self):
        """Confirm SPECstorage tarball was extracted before running SM2020."""
        try:
            self._execute_checked(
                f"test -x {self.sm2020} && "
                f"test -f {self.install_dest}/SPECstorage2020/{self.benchmark_file}"
            )
        except SpecStorageError as exc:
            listing, _, _, _ = self.primary_client.exec_command(
                sudo=True,
                cmd=f"ls -la {self.install_dest}",
                verbose=True,
                check_ec=False,
            )
            raise SpecStorageError(
                f"SPECstorage install incomplete: missing {self.sm2020}\n"
                f"{self.install_dest} contents:\n{listing}"
            ) from exc

    def install_spec_storage(self):
        """Install SPECstorage toolkit on the primary client."""
        tarball = f"{self.install_dest}/SPECstorage2020-2529.tgz"
        try:
            self._execute_checked(self.INSTALL_PREREQS)
            self._execute_checked(f"mkdir -p {self.install_dest}")
            self._execute_checked(
                f"wget {self.install_loc}SPECstorage2020-2529.tgz -O {tarball}"
            )
            self._execute_checked(
                f"wget {self.install_loc}SPECstorage_clients.sh "
                f"-O {self.install_dest}/SPECstorage_clients.sh"
            )
            self._execute_checked(f"tar zxvf {tarball} -C {self.install_dest}")
            self._execute_checked(f". {self.install_dest}/SPECstorage_clients.sh")
            self._verify_install_layout()
        except SpecStorageError:
            raise
        except Exception as exc:
            raise SpecStorageError(f"SPECstorage installation failed: {exc}") from exc

    def update_config(
        self,
        benchmark,
        load,
        incr_load,
        num_runs,
        clients,
        nfs_mount,
        benchmark_defination,
    ):
        """
        Update SPECstorage configuration file
        Args:
            benchmark (str): Benchmark example: SWBUILD, VDA, EDA, AI_IMAGE, GENOMICS
            load (str): Starting load value
            incr_load (str): Incremental increase value in load for successive data points in a run
            num_runs (str): The number of load points to run
            clients (str): All Clients
            nfs_mount (str): Clients mount points
            benchmark_defination (dir) : benchmark defination parameters with values
        """
        config_path = f"{self.install_dest}/{self.config}"
        benchmark_yml = f"{self.install_dest}/SPECstorage2020/{self.benchmark_file}"
        try:
            self._execute_checked(
                f"wget {self.install_loc}/{self.config} -O {config_path}"
            )
            self._execute_checked(
                f"sed -i '/EXEC_PATH=/d' {config_path} && "
                f"echo EXEC_PATH={self.install_dest}/SPECstorage2020/binaries/linux/x86_64/netmist >> "
                f"{config_path}"
            )

            client_mountpoints = "CLIENT_MOUNTPOINTS="
            for client in clients:
                client_mountpoints += f"{client.ip_address}:{nfs_mount} "
            self._execute_checked(
                f"echo {client_mountpoints.rstrip()} >> {config_path}"
            )
            self._execute_checked(f"echo BENCHMARK={benchmark} >> {config_path}")
            self._execute_checked(f"echo LOAD={load} >> {config_path}")
            self._execute_checked(f"echo INCR_LOAD={incr_load} >> {config_path}")
            self._execute_checked(f"echo NUM_RUNS={num_runs} >> {config_path}")

            if benchmark_defination:
                for parameter, value in benchmark_defination.items():
                    self._execute_checked(
                        f"sed -i '/Benchmark_name:/,/{parameter}:/ s/{parameter}:.*/{parameter}: {value}/'"
                        f" {benchmark_yml}"
                    )
        except SpecStorageError:
            raise
        except Exception as exc:
            raise SpecStorageError(f"SPECstorage configuration failed: {exc}") from exc

    def run_spec_storage(
        self,
        benchmark,
        load,
        incr_load,
        num_runs,
        clients,
        nfs_mount,
        benchmark_defination,
    ):
        """
        Run SPECstorage
        Args:
            benchmark (str): Benchmark example: SWBUILD, VDA, EDA, AI_IMAGE, GENOMICS
            load (str): Starting load value
            incr_load (str): Incremental increase value in load for successive data points in a run
            num_runs (str): The number of load points to run
            clients (str): All Clients
            nfs_mount (str): Clients mount points
        """
        # Install SPECstorage
        self.install_spec_storage()

        # Update SPECstorage configuration
        self.update_config(
            benchmark,
            load,
            incr_load,
            num_runs,
            clients,
            nfs_mount,
            benchmark_defination,
        )
        nfs_mount = nfs_mount.rstrip("/")
        last_path_component = os.path.basename(nfs_mount)
        benchmark_yml = f"{self.install_dest}/SPECstorage2020/{self.benchmark_file}"
        config_path = f"{self.install_dest}/{self.config}"
        cmd = (
            f"{self.base_cmd} -b {benchmark_yml} -r {config_path} "
            f"-s {benchmark}-{self.outputlog}-{last_path_component}"
        )
        _out, _err, exit_code, _duration = self.primary_client.exec_command(
            sudo=True,
            long_running=True,
            cmd=cmd,
            verbose=True,
            timeout=7200,
            check_ec=False,
        )
        if exit_code != 0:
            raise SpecStorageError(
                f"SPECstorage run failed (exit {exit_code}): {cmd}\nstdout: {_out}\nstderr: {_err}"
            )
        return 0

    def append_to_csv(self, output_file, metrics):
        fieldnames = list(metrics.keys())
        file_exists = os.path.isfile(output_file)

        with open(output_file, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics)

    def extract_metrics(self, remote_file, **kwargs):
        # Create an ElementTree object from the remote file
        tree = ET.parse(remote_file)
        root = tree.getroot()

        metrics = kwargs
        business_metric = root.find(".//business_metric").text
        metrics["business_metric"] = business_metric
        benchmark = root.find(".//benchmark").attrib["name"]
        metrics["benchmark"] = benchmark
        for metric in root.findall(".//metric"):
            name = metric.attrib["name"]
            value = metric.text
            metrics[name] = value
        return metrics

    def parse_spectorage_results(self, results_dir, output_file, **kwargs):
        dir_items = self.primary_client.get_dir_list(results_dir, sudo=True)
        for item in dir_items:
            if item.endswith(".xml") and "_parsed" not in item:
                remote_file_xml = self.primary_client.remote_file(
                    file_name=f"{results_dir}/{item}", file_mode="r", sudo=True
                )
                metrics = self.extract_metrics(remote_file_xml, **kwargs)
                self.append_to_csv(output_file, metrics)
                # Rename the processed XML file
                new_name = item.replace(".xml", "_parsed.xml")
                self.primary_client.exec_command(
                    cmd=f"mv {results_dir}/{item} {results_dir}/{new_name}", sudo=True
                )
