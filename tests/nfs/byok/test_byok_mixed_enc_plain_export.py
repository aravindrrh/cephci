"""
BYOK: encrypted export + unencrypted export on the same CephFS.

Regression for adding a second NFS export without ``enctag`` (``kmip_key_id``)
after a BYOK export is working; the original encrypted export must remain usable.
"""

import json
import traceback
from time import sleep

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from cli.utilities.filesys import Unmount
from tests.nfs.byok.byok_tools import (
    clean_up_gklm,
    create_nfs_instance_for_byok,
    ensure_fresh_gklm_kmip_client,
    get_enctag,
    load_gklm_config,
    perform_io_operations_and_validate_fuse,
    setup_gklm_infrastructure,
    wait_for_gklm_server_restart,
)
from tests.nfs.nfs_operations import (
    cleanup_custom_nfs_cluster_multi_export_client,
    mount_cleanup_retry,
    mount_retry,
    _get_client_specific_mount_versions,
)
from tests.nfs.test_nfs_multiple_operations_for_upgrade import create_file
from utility.gklm_client.gklm_client import build_gklm_client
from utility.log import Log
from utility.utils import get_cephci_config

log = Log(__name__)

subvolume_group = "ganeshagroup"
gkml_client_name = "automation"
gklm_cert_alias = "cert2"


def run(ceph_cluster, **kw):
    """
    1. Create BYOK NFS cluster and an encrypted export (``enctag`` / ``kmip_key_id``).
    2. Mount export 1, run I/O and FUSE encryption validation.
    3. Add a second export on the same CephFS without ``enctag``, mount it, light I/O.
    4. Re-run I/O and FUSE validation on export 1.
    """
    config = kw.get("config", {})
    custom_data = kw.get("test_data", {})
    cephci_data = get_cephci_config()

    nfs_nodes = ceph_cluster.get_nodes("nfs")
    nfs_node = nfs_nodes[0]
    installer = ceph_cluster.get_nodes(role="installer")
    clients = ceph_cluster.get_nodes("client")

    nfs_mount = config.get("nfs_mount", "/mnt/nfs_byok_mix")
    nfs_export = config.get("nfs_export", "/export_byok_mix")
    nfs_name = config.get("nfs_instance_name", "nfs_byok_mix")
    fs_name = config.get("fs_name", "cephfs")
    port = config.get("nfs_port", "2049")
    nfs_version = config.get("nfs_version", "4.2")
    no_clients = int(config.get("clients", 1))

    if no_clients > len(clients):
        raise ConfigError(
            f"Test requires {no_clients} clients but only {len(clients)} available"
        )
    clients = clients[:no_clients]
    client0 = clients[0]

    enc_export = f"{nfs_export}_0"
    enc_mount = f"{nfs_mount}_0"
    plain_export = f"{nfs_export}_plain"
    plain_mount = f"{nfs_mount}_plain"

    gklm_rest_client = None

    try:
        gklm_params = load_gklm_config(custom_data, config, cephci_data)
        gklm_ip = gklm_params["gklm_ip"]
        gklm_user = gklm_params["gklm_user"]
        gklm_hostname = gklm_params["gklm_hostname"]

        log.info("Setting up GKLM infrastructure")
        setup_gklm_infrastructure(
            nfs_nodes=nfs_nodes,
            gklm_ip=gklm_ip,
            gklm_hostname=gklm_hostname,
        )
        gklm_rest_client = build_gklm_client(gklm_params, verify=False)
        ensure_fresh_gklm_kmip_client(
            gklm_rest_client,
            gkml_client_name,
            legacy_cert_aliases=("cert2",),
        )

        sys_cert_details = None
        for entry in gklm_rest_client.certificates.list_system_certificates():
            a = (
                entry.get("alias")
                or entry.get("Alias")
                or entry.get("certAlias")
                or entry.get("name")
            )
            if a and str(a) == gklm_hostname:
                sys_cert_details = entry
                break
        legacy_cert_details = None
        if not sys_cert_details:
            for entry in gklm_rest_client.certificates.list_certificates():
                a = (
                    entry.get("alias")
                    or entry.get("Alias")
                    or entry.get("certAlias")
                    or entry.get("name")
                )
                if a and str(a) == gklm_hostname:
                    legacy_cert_details = entry
                    break

        needs_restart = False
        if not sys_cert_details and not legacy_cert_details:
            gklm_rest_client.certificates.create_system_certificate(
                {
                    "type": "Self-signed",
                    "alias": gklm_hostname,
                    "cn": gklm_hostname,
                    "validity": "3650",
                    "algorithm": "RSA",
                    "usageSubtype": "KEYSERVING_TLS",
                }
            )
            needs_restart = True
        else:
            cert_to_check = sys_cert_details or legacy_cert_details
            usage = str(cert_to_check.get("usage", "")) + str(
                cert_to_check.get("usageSubtype", "")
            )
            if "KEYSERVING" not in usage.upper():
                gklm_rest_client.certificates.update_system_certificate(
                    alias=gklm_hostname,
                    add_usage_subtype="KEYSERVING_TLS",
                )
                needs_restart = True

        if needs_restart:
            gklm_rest_client.server.restart_server()
            wait_for_gklm_server_restart(
                gklm_rest_client=gklm_rest_client,
                timeout=500,
                check_interval=10,
                initial_wait=10,
            )
        ca_cert = gklm_rest_client.certificates.get_system_certificate(
            cert_name=gklm_hostname
        )
        rsa_key, cert, _ = gklm_rest_client.certificates.get_certificates(
            subject={
                "common_name": nfs_node.hostname,
                "ip_address": nfs_node.ip_address,
            }
        )
        all_clients = gklm_rest_client.clients.list_clients()
        if gkml_client_name.upper() not in [x.get("clientName") for x in all_clients]:
            gklm_rest_client.clients.create_client(gkml_client_name)

        enctag = get_enctag(
            gklm_rest_client,
            gkml_client_name,
            gklm_cert_alias,
            gklm_user,
            cert,
        )

        log.info("Creating CephFS subvolume group for NFS")
        Ceph(client0).fs.sub_volume_group.create(volume=fs_name, group=subvolume_group)
        Ceph(nfs_node).execute("systemctl start rpcbind", sudo=True)

        log.info("Deploying NFS Ganesha with BYOK")
        create_nfs_instance_for_byok(
            installer=installer,
            nfs_node=nfs_node,
            nfs_name=nfs_name,
            kmip_host_list=gklm_hostname,
            rsa_key=rsa_key,
            cert=cert,
            ca_cert=ca_cert,
        )

        log.info("Step 1: Encrypted export %s (enctag set)", enc_export)
        Ceph(client0).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=enc_export,
            fs=fs_name,
            enctag=enctag,
        )
        sleep(2)
        export_ls = json.loads(Ceph(client0).nfs.export.ls(nfs_name))
        if enc_export not in export_ls:
            raise OperationFailedError(
                f"Encrypted export {enc_export} missing from {export_ls}"
            )

        mount_versions = _get_client_specific_mount_versions(nfs_version, clients)
        for ver, ver_clients in mount_versions.items():
            ver_clients[0].create_dirs(dir_path=enc_mount, sudo=True)
            mount_retry(
                client=ver_clients[0],
                mount_name=enc_mount,
                version=ver,
                port=port,
                nfs_server=nfs_node.hostname,
                export_name=enc_export,
            )

        enc_dict = {client0: {"mount": [enc_mount], "export": [enc_export]}}
        log.info("Step 2: I/O + FUSE validation on encrypted export only")
        perform_io_operations_and_validate_fuse(
            enc_dict,
            clients,
            file_count=5,
            dd_command_size_in_M=2,
            nfs_name=nfs_name,
        )

        log.info("Step 3: Second export %s without enctag (same CephFS)", plain_export)
        Ceph(client0).nfs.export.create(
            fs_name=fs_name,
            nfs_name=nfs_name,
            nfs_export=plain_export,
            fs=fs_name,
        )
        sleep(2)
        export_ls = json.loads(Ceph(client0).nfs.export.ls(nfs_name))
        if plain_export not in export_ls:
            raise OperationFailedError(
                f"Plain export {plain_export} missing from {export_ls}"
            )

        for ver, ver_clients in mount_versions.items():
            ver_clients[0].create_dirs(dir_path=plain_mount, sudo=True)
            mount_retry(
                client=ver_clients[0],
                mount_name=plain_mount,
                version=ver,
                port=port,
                nfs_server=nfs_node.hostname,
                export_name=plain_export,
            )
        create_file(client0, plain_mount, "plain_export_smoke.txt")

        log.info("Step 4: Re-validate encrypted export after plain export exists")
        perform_io_operations_and_validate_fuse(
            enc_dict,
            clients,
            file_count=5,
            dd_command_size_in_M=2,
            nfs_name=nfs_name,
        )

        return 0
    except Exception as e:
        log.error("BYOK mixed export test failed: %s", e)
        log.error(traceback.format_exc())
        return 1
    finally:
        log.info("Cleanup: plain mount/export, then standard BYOK export cleanup")
        try:
            try:
                mount_cleanup_retry(client0, plain_mount)
            except Exception:
                log.debug("plain mount dir cleanup skipped or failed", exc_info=True)
            if Unmount(client0).unmount(plain_mount):
                log.warning(
                    "Unmount %s returned failure (may already be unmounted)",
                    plain_mount,
                )
            client0.exec_command(sudo=True, cmd=f"rm -rf {plain_mount}", check_ec=False)
            Ceph(client0).nfs.export.delete(nfs_name, plain_export)
        except Exception as ex:
            log.warning("Plain export cleanup (best effort): %s", ex)

        try:
            cleanup_custom_nfs_cluster_multi_export_client(
                clients,
                nfs_mount,
                nfs_name,
                nfs_export,
                1,
                nfs_nodes=nfs_nodes,
            )
        except Exception as ex:
            log.warning("cleanup_custom_nfs_cluster_multi_export_client: %s", ex)

        if gklm_rest_client:
            clean_up_gklm(
                gklm_rest_client=gklm_rest_client,
                gkml_client_name=gkml_client_name,
                gklm_cert_alias=gklm_cert_alias,
            )
