from threading import Thread

from utility.log import Log

log = Log(__name__)

KERNEL_TAR_URL = "https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.4.54.tar.xz"
KERNEL_TAR_NAME = "linux-5.4.54.tar.xz"
KERNEL_TAR_DEST = f"/root/{KERNEL_TAR_NAME}"


def _ensure_curl(client):
    """Install curl on the client if missing (linux_untar downloads a kernel tarball)."""
    out, _ = client.exec_command(cmd="command -v curl", sudo=True, check_ec=False)
    if (out or "").strip():
        return
    log.info("Ensuring curl is present on %s for linux_untar", client.hostname)
    install_cmds = (
        "dnf install -y curl",
        "microdnf install -y curl",
        "yum install -y curl",
    )
    for pkg_cmd in install_cmds:
        client.exec_command(
            cmd=pkg_cmd,
            sudo=True,
            long_running=True,
            check_ec=False,
            timeout=3600,
        )
        out, _ = client.exec_command(cmd="command -v curl", sudo=True, check_ec=False)
        if (out or "").strip():
            log.info("curl available on %s after: %s", client.hostname, pkg_cmd)
            return
    raise RuntimeError(
        "curl is required for linux_untar but could not be installed on "
        f"{client.hostname}"
    )


def linux_untar(clients, mountpoint, dirs=("."), full_untar=False):
    """
    Performs Linux untar on the given clients
    Args:
        clients (list): Client (s)
        mountpoint(str): Mount point where the volume is
                       mounted.
        dirs(tuple): A tuple of dirs where untar has to
                    started. (Default:('.'))
        full_untar(bool): Whether to perform a complete untar or not
    """
    threads = []
    if not isinstance(clients, list):
        clients = [clients]

    for client in clients:
        _ensure_curl(client)
        cmd = f"curl -fL -o {KERNEL_TAR_DEST} {KERNEL_TAR_URL}"
        client.exec_command(cmd=cmd, sudo=True, long_running=True, timeout=3600)

        for directory in dirs:
            # copy linux tar to dir
            cmd = f"cp {KERNEL_TAR_DEST} {mountpoint}/{directory}"
            client.exec_command(cmd=cmd, sudo=True)

            # Start linux untar
            cmd = "cd {}/{};tar -xvf linux-5.4.54.tar.xz".format(mountpoint, directory)
            if not full_untar:
                # If full untar is not required, perform untar of few directories alone
                cmd += (
                    f"{mountpoint}/{directory} linux-5.4.54/drivers linux-5.4.54/tools"
                )
            untar = Thread(
                target=lambda: client.exec_command(
                    cmd=cmd, sudo=True, long_running=True
                ),
                args=(),
            )
            untar.start()
            threads.append(untar)
    return threads
