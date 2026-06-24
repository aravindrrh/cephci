"""
Shared gRPC client helpers for NFS-Ganesha admin tests.

Wraps grpcurl installation, plaintext/TLS invocation, and parsing of
responses from nfsService.proto (GetClientId, GetNfsGrace, StartNfsGrace,
GetSessionId).
"""

import json
import re

from cli.ceph.ceph import Ceph
from cli.exceptions import ConfigError, OperationFailedError
from utility.log import Log

log = Log(__name__)

DEFAULT_GRPC_PORT = 50051
DEFAULT_GRPC_UDS_PATH = "/var/run/ganesha/ganesha.grpc.sock"

DEFAULT_CERT_PATHS = {
    "ca_cert": "/etc/ganesha/certs/ca.crt",
    "client_cert": "/etc/ganesha/certs/client.crt",
    "client_key": "/etc/ganesha/certs/client.key",
    "server_cert": "/etc/ganesha/certs/server.crt",
    "server_key": "/etc/ganesha/certs/server.key",
}

EXPECTED_GRPC_SERVICES = [
    "nfsService.GetClientId",
    "nfsService.GetNfsGrace",
    "nfsService.GetSessionId",
    "nfsService.StartNfsGrace",
    "grpc.reflection.v1alpha.ServerReflection",
]

# Operations that require ExportMgr/Admin/Log protos not yet in upstream.
BLOCKED_OPERATIONS = frozenset(
    {
        "add_export",
        "remove_export",
        "update_export",
        "show_exports",
        "display_export",
        "shutdown",
        "reload",
        "grace_client_ip",
        "set_log_level",
        "get_log_level",
        "remove_client",
    }
)


def install_grpcurl(node):
    """Install grpcurl on *node* if not already present."""
    log.info("Installing grpcurl on %s", node.hostname)
    out, _ = node.exec_command(cmd="which grpcurl", check_ec=False)
    if "grpcurl" in out:
        log.info("grpcurl already installed on %s", node.hostname)
        return

    wget_cmd = (
        "curl -LO "
        "https://github.com/fullstorydev/grpcurl/releases/download/v1.8.9/"
        "grpcurl_1.8.9_linux_x86_64.tar.gz"
    )
    install_cmd = (
        f"{wget_cmd} && tar -xvzf grpcurl_1.8.9_linux_x86_64.tar.gz && "
        "chmod +x grpcurl && mv grpcurl /usr/local/bin/"
    )
    node.exec_command(sudo=True, cmd=install_cmd)
    out, _ = node.exec_command(cmd="grpcurl --version", check_ec=False)
    log.info("grpcurl version on %s: %s", node.hostname, out.strip())


def open_grpc_firewall(nfs_node, port=DEFAULT_GRPC_PORT):
    """Open *port*/tcp on the NFS node firewall (best-effort)."""
    cmd = f"firewall-cmd --permanent --add-port={port}/tcp"
    nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    nfs_node.exec_command(sudo=True, cmd="firewall-cmd --reload", check_ec=False)


def build_grpcurl_target(host, port=DEFAULT_GRPC_PORT):
    """Return host:port target string for grpcurl."""
    if ":" in host and not host.startswith("["):
        return f"{host}:{port}"
    return f"{host}:{port}"


def build_grpcurl_cmd(
    target,
    method,
    data=None,
    plaintext=True,
    tls_paths=None,
):
    """
    Build a grpcurl shell command.

    Args:
        target: host:port
        method: full method e.g. nfsService.GetClientId/GetClientIds
        data: optional JSON request body
        plaintext: use -plaintext when True
        tls_paths: dict with ca_cert, client_cert, client_key for mTLS
    """
    parts = ["grpcurl"]
    if plaintext:
        parts.append("-plaintext")
    elif tls_paths:
        parts.extend(
            [
                f"-cacert {tls_paths['ca_cert']}",
                f"-cert {tls_paths['client_cert']}",
                f"-key {tls_paths['client_key']}",
            ]
        )
    if data is not None:
        parts.append(f"-d '{data}'")
    parts.append(target)
    parts.append(method)
    return " ".join(parts)


def grpcurl_exec(node, cmd):
    """Run grpcurl *cmd* on *node*; return (stdout, stderr, success)."""
    out, err = node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    combined = f"{out}\n{err}".lower()
    failed = "rpc error" in combined or (
        err and "error" in err.lower() and "desc" in err.lower()
    )
    return out, err, not failed


def list_grpc_services(client_node, target, plaintext=True, tls_paths=None):
    """List gRPC services on *target*."""
    cmd = build_grpcurl_cmd(target, "list", plaintext=plaintext, tls_paths=tls_paths)
    out, err, ok = grpcurl_exec(client_node, cmd)
    if not ok:
        log.error("Failed to list gRPC services: %s", err or out)
        return False, []
    services = [line.strip() for line in out.strip().split("\n") if line.strip()]
    log.info("Discovered gRPC services: %s", services)
    return True, services


def list_grpc_services_uds(nfs_node, socket_path, plaintext=True):
    """List gRPC services via Unix domain socket on the NFS node."""
    parts = ["grpcurl"]
    if plaintext:
        parts.append("-plaintext")
    parts.extend(["-unix", socket_path, "list"])
    cmd = " ".join(parts)
    out, err, ok = grpcurl_exec(nfs_node, cmd)
    if not ok:
        log.error("UDS gRPC discovery failed on %s: %s", socket_path, err or out)
        return False, []
    services = [line.strip() for line in out.strip().split("\n") if line.strip()]
    log.info("UDS gRPC services on %s: %s", socket_path, services)
    return True, services


def invoke_grpc_method(
    client_node,
    target,
    method,
    data=None,
    plaintext=True,
    tls_paths=None,
    max_time_sec=None,
):
    """
    Invoke a gRPC method via grpcurl.

    Returns:
        tuple: (success, stdout, stderr)
    """
    cmd = build_grpcurl_cmd(
        target, method, data=data, plaintext=plaintext, tls_paths=tls_paths
    )
    if max_time_sec is not None:
        cmd = cmd.replace("grpcurl", f"grpcurl -max-time {max_time_sec}", 1)
    out, err, ok = grpcurl_exec(client_node, cmd)
    return ok, out, err


def expect_rpc_error(client_node, target, method, data, plaintext=True):
    """Return True when grpcurl reports an RPC error (negative test helper)."""
    ok, out, err = invoke_grpc_method(
        client_node, target, method, data=data, plaintext=plaintext
    )
    combined = f"{out}\n{err}".lower()
    has_error = not ok or "rpc error" in combined or "code =" in combined
    log.info("RPC negative check ok=%s combined=%s", ok, combined[:300])
    return has_error


def verify_expected_services(discovered):
    """Return True if at least one expected nfsService is present."""
    found = []
    for expected in EXPECTED_GRPC_SERVICES:
        prefix = expected.split(".")[0]
        if any(prefix in svc for svc in discovered):
            found.append(expected)
    log.info("Matched expected services: %s", found)
    return len(found) > 0


def verify_grpc_port_listening(nfs_node, port=DEFAULT_GRPC_PORT):
    """Return True when ganesha is listening on *port*."""
    cmd = f"ss -tulnp | grep {port}"
    out, _ = nfs_node.exec_command(sudo=True, cmd=cmd, check_ec=False)
    if str(port) not in out:
        log.error("gRPC port %s is not listening", port)
        return False
    log.info("gRPC port %s listening: %s", port, out.strip())
    return True


def get_grace_period(client_node, target, plaintext=True, tls_paths=None):
    """Call GetNfsGrace.GetGracePeriod; return (ok, ingrace_or_none, raw)."""
    method = "nfsService.GetNfsGrace/GetGracePeriod"
    cmd = build_grpcurl_cmd(target, method, plaintext=plaintext, tls_paths=tls_paths)
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("GetGracePeriod response: %s", out)
    if not ok:
        return False, None, out or err

    ingrace = None
    try:
        payload = json.loads(out) if out.strip() else {}
        ingrace = payload.get("ingrace", payload.get("inGrace"))
    except json.JSONDecodeError:
        match = re.search(r'"?ingrace"?\s*:\s*(true|false)', out, re.IGNORECASE)
        if match:
            ingrace = match.group(1).lower() == "true"
    return True, ingrace, out


def start_grace_with_event(
    client_node,
    target,
    event_id,
    node_id=1,
    ip_addr=None,
    plaintext=True,
    tls_paths=None,
):
    """Call StartNfsGrace.StartGraceWithEvent."""
    host = target.split(":")[0]
    ip_addr = ip_addr or host
    request_data = json.dumps(
        {"Event": int(event_id), "NodeId": int(node_id), "IpAddr": ip_addr}
    )
    method = "nfsService.StartNfsGrace/StartGraceWithEvent"
    cmd = build_grpcurl_cmd(
        target,
        method,
        data=request_data,
        plaintext=plaintext,
        tls_paths=tls_paths,
    )
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("StartGraceWithEvent response: %s", out)
    if int(event_id) == 0:
        ok = ok and "graceStarted" in out and "true" in out.lower()
    return ok, out or err


def parse_id_list(raw, json_keys, regex_pattern):
    """Parse repeated ID fields from grpcurl JSON or text output."""
    try:
        payload = json.loads(raw) if raw.strip() else {}
        for key in json_keys:
            if key in payload:
                return list(payload[key])
    except json.JSONDecodeError:
        pass
    return re.findall(regex_pattern, raw, re.IGNORECASE)


def get_client_ids(client_node, target, plaintext=True, tls_paths=None):
    """Call GetClientId.GetClientIds; return (ok, ids, raw)."""
    method = "nfsService.GetClientId/GetClientIds"
    cmd = build_grpcurl_cmd(target, method, plaintext=plaintext, tls_paths=tls_paths)
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("GetClientIds response: %s", out)
    if not ok:
        return False, [], out or err
    ids = parse_id_list(
        out,
        ("clientIds", "client_ids"),
        r'"?client_?ids?"?\s*:\s*\[([^\]]*)\]|"?clientId"?\s*:\s*"?(\d+)"?',
    )
    if not ids and out.strip():
        ids = re.findall(r'"?clientId"?\s*:\s*"?(\d+)"?', out, re.IGNORECASE)
    return True, ids, out


def get_session_ids(client_node, target, plaintext=True, tls_paths=None):
    """Call GetSessionId.GetSessionIds; return (ok, ids, raw)."""
    method = "nfsService.GetSessionId/GetSessionIds"
    cmd = build_grpcurl_cmd(target, method, plaintext=plaintext, tls_paths=tls_paths)
    out, err, ok = grpcurl_exec(client_node, cmd)
    log.info("GetSessionIds response: %s", out)
    if not ok:
        return False, [], out or err
    ids = parse_id_list(
        out,
        ("sessionIds", "session_ids"),
        r'"?sessionId"?\s*:\s*"?([^",\s]+)"?',
    )
    return True, ids, out


def tls_paths_from_config(config):
    """Build TLS path dict from test config, falling back to defaults."""
    paths = dict(DEFAULT_CERT_PATHS)
    for key in paths:
        if config.get(key):
            paths[key] = config[key]
    return paths


def resolve_grpc_target(config, nfs_ip, port=None):
    """Resolve grpcurl target from config (supports loopback override)."""
    port = int(config.get("grpc_port", port or DEFAULT_GRPC_PORT))
    host = config.get("grpc_host", nfs_ip)
    return build_grpcurl_target(host, port)


def check_blocked_operation(operation, config):
    """
    Return -1 to skip when *operation* is blocked pending upstream proto.

    Raises ConfigError when blocked and skip_if_blocked is False.
    """
    blocked = config.get("blocked", operation in BLOCKED_OPERATIONS)
    if not blocked:
        return None
    msg = (
        f"Operation '{operation}' is blocked pending ExportMgr/Admin/Log "
        "gRPC proto in upstream nfs-ganesha"
    )
    if config.get("skip_if_blocked", True):
        log.info("SKIP: %s", msg)
        return -1
    raise ConfigError(msg)


def prepare_cluster_nodes(ceph_cluster, config):
    """
    Validate and return common node handles from *ceph_cluster*.

    Returns:
        tuple: (clients, nfs_nodes, client, nfs_node, nfs_server, nfs_ip)
    """
    num_clients = int(config.get("clients", 1))
    clients = ceph_cluster.get_nodes(role="client")
    nfs_nodes = ceph_cluster.get_nodes(role="nfs")

    if not clients:
        raise OperationFailedError("No client nodes available")
    if not nfs_nodes:
        raise OperationFailedError("No NFS nodes available")
    if num_clients > len(clients):
        raise ConfigError(
            f"Test requires {num_clients} clients but only {len(clients)} available"
        )

    clients = clients[:num_clients]
    nfs_node = nfs_nodes[0]
    return (
        clients,
        nfs_nodes,
        clients[0],
        nfs_node,
        nfs_node.hostname,
        nfs_node.ip_address,
    )


def ensure_subvolume_group(client, fs_name, group="ganeshagroup"):
    """Create NFS subvolume group if needed."""
    Ceph(client).fs.sub_volume_group.create(volume=fs_name, group=group)
