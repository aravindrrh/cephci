"""
Shared helpers for Spectrum Scale upstream custom-case modules.

Used by nfs_run_spectrum_scale_upstream_*.py after suite deploy
(skip_deployment: true). Mounts on client(s), not the installer.
"""

import shlex

from tests.nfs.lib.upstream_gpfs_nfs_setup import setup_gpfs_nfs, teardown_gpfs_nfs
from utility.log import Log

log = Log(__name__)

DEFAULT_MOUNT = "/mnt/nfsv4"
DEFAULT_EXPORT = "/ibm/scale_volume/export1"
DEFAULT_DURATION = 300  # seconds

# Read-lock stress binary (overlapping F_RDLCK). Duration via argv[2] (alarm).
READ_LOCK_C = r"""
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <signal.h>

#define NUM_THREADS 5
#define MAX_PATH_LEN 1024

char filepath[MAX_PATH_LEN];
volatile int keep_running = 1;

void stop_handler(int sig) { (void)sig; keep_running = 0; }

void *thread_function(void *arg) {
   int rfd;
   struct flock rfl;
   long thread_id = (long)arg;
   int toggle = 0;

   while (keep_running) {
       rfd = open(filepath, O_RDONLY);
       if (rfd == -1) {
           printf("Thread %ld: Failed to open file %s\n", thread_id, filepath);
           continue;
       }

       rfl.l_type = F_RDLCK;
       rfl.l_whence = SEEK_SET;
       rfl.l_start = toggle ? 5 : 0;
       rfl.l_len = 0;

       if (fcntl(rfd, F_SETLKW, &rfl) == -1) {
           printf("Thread %ld: Failed to set F_RDLCK\n", thread_id);
           close(rfd);
           continue;
       }

       rfl.l_type = F_UNLCK;
       if (fcntl(rfd, F_SETLKW, &rfl) == -1) {
           printf("Thread %ld: Failed to unlock\n", thread_id);
       }

       close(rfd);
       toggle = !toggle;
   }
   pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
   int duration = 300;
   if (argc < 2 || argc > 3) {
       fprintf(stderr, "Usage: %s <directory> [duration_sec]\n", argv[0]);
       exit(EXIT_FAILURE);
   }
   if (argc == 3)
       duration = atoi(argv[2]);
   if (duration <= 0)
       duration = 300;

   snprintf(filepath, MAX_PATH_LEN, "%s/testfile.txt", argv[1]);
   int fd = open(filepath, O_CREAT | O_RDWR, 0644);
   if (fd == -1) {
       perror("create testfile");
       exit(EXIT_FAILURE);
   }
   write(fd, "locktest\n", 9);
   close(fd);

   signal(SIGALRM, stop_handler);
   alarm(duration);

   pthread_t threads[NUM_THREADS];
   long t;
   for (t = 0; t < NUM_THREADS; t++) {
       if (pthread_create(&threads[t], NULL, thread_function, (void *)t)) {
           fprintf(stderr, "pthread_create failed\n");
           exit(EXIT_FAILURE);
       }
   }
   for (t = 0; t < NUM_THREADS; t++)
       pthread_join(threads[t], NULL);
   return 0;
}
"""

# Write-lock stress binary (overlapping F_WRLCK).
WRITE_LOCK_C = r"""
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <signal.h>

#define NUM_THREADS 5
#define MAX_PATH_LEN 1024

char filepath[MAX_PATH_LEN];
volatile int keep_running = 1;

void stop_handler(int sig) { (void)sig; keep_running = 0; }

void *thread_function(void *arg) {
   int wfd;
   struct flock wfl;
   long thread_id = (long)arg;
   int toggle = 0;

   while (keep_running) {
       wfd = open(filepath, O_WRONLY);
       if (wfd == -1) {
           printf("Thread %ld: Failed to open file %s\n", thread_id, filepath);
           continue;
       }

       wfl.l_type = F_WRLCK;
       wfl.l_whence = SEEK_SET;
       wfl.l_start = toggle ? 5 : 0;
       wfl.l_len = 0;

       if (fcntl(wfd, F_SETLKW, &wfl) == -1) {
           printf("Thread %ld: Failed to set F_WRLCK\n", thread_id);
           close(wfd);
           continue;
       }

       wfl.l_type = F_UNLCK;
       if (fcntl(wfd, F_SETLKW, &wfl) == -1) {
           printf("Thread %ld: Failed to unlock\n", thread_id);
       }

       close(wfd);
       toggle = !toggle;
   }
   pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
   int duration = 300;
   if (argc < 2 || argc > 3) {
       fprintf(stderr, "Usage: %s <directory> [duration_sec]\n", argv[0]);
       exit(EXIT_FAILURE);
   }
   if (argc == 3)
       duration = atoi(argv[2]);
   if (duration <= 0)
       duration = 300;

   snprintf(filepath, MAX_PATH_LEN, "%s/testfile.txt", argv[1]);
   int fd = open(filepath, O_CREAT | O_WRONLY, 0644);
   if (fd == -1) {
       perror("create testfile");
       exit(EXIT_FAILURE);
   }
   close(fd);

   signal(SIGALRM, stop_handler);
   alarm(duration);

   pthread_t threads[NUM_THREADS];
   long t;
   for (t = 0; t < NUM_THREADS; t++) {
       if (pthread_create(&threads[t], NULL, thread_function, (void *)t)) {
           fprintf(stderr, "pthread_create failed\n");
           exit(EXIT_FAILURE);
       }
   }
   for (t = 0; t < NUM_THREADS; t++)
       pthread_join(threads[t], NULL);
   return 0;
}
"""


def merge_custom_config(kw_config):
    """Apply custom-case defaults; ensure skip_deployment after suite bootstrap."""
    conf = dict(kw_config or {})
    conf.setdefault("mount_point", DEFAULT_MOUNT)
    conf.setdefault("nfs_export", DEFAULT_EXPORT)
    conf.setdefault("nfs_version", "4.1")
    conf.setdefault("port", "2049")
    conf.setdefault("clients", 1)
    conf.setdefault("skip_deployment", True)
    conf.setdefault("duration", DEFAULT_DURATION)
    return conf


def prepare_mount(ceph_cluster, config):
    """Mount export on client(s); return setup_gpfs_nfs result dict."""
    return setup_gpfs_nfs(ceph_cluster, config)


def cleanup_mount(clients, nfs_mount):
    """Best-effort umount."""
    try:
        teardown_gpfs_nfs(clients, nfs_mount)
    except Exception as exc:
        log.warning("teardown_gpfs_nfs: %s", exc)


def ensure_client_build_tools(client):
    """gcc + pthread deps for lock stress binaries."""
    client.exec_command(
        sudo=True,
        cmd="dnf -y install gcc make nfs-utils || yum -y install gcc make nfs-utils",
        long_running=True,
        check_ec=False,
    )


def write_remote(client, path, content):
    """Write *content* to *path* on *client* as root."""
    client.exec_command(sudo=True, cmd=f"mkdir -p $(dirname {shlex.quote(path)})")
    with client.remote_file(sudo=True, file_name=path, file_mode="w") as fh:
        fh.write(content)


def build_lock_binaries(client, workdir="/tmp/nfs_custom_locks"):
    """
    Compile read_lookc_thr and write_lookc_thr on the client.

    Returns (read_bin, write_bin) absolute paths.
    """
    ensure_client_build_tools(client)
    client.exec_command(sudo=True, cmd=f"rm -rf {workdir} && mkdir -p {workdir}")
    read_c = f"{workdir}/read_lookc_thr.c"
    write_c = f"{workdir}/write_lookc_thr.c"
    read_bin = f"{workdir}/read_lookc_thr"
    write_bin = f"{workdir}/write_lookc_thr"
    write_remote(client, read_c, READ_LOCK_C)
    write_remote(client, write_c, WRITE_LOCK_C)
    client.exec_command(
        sudo=True,
        cmd=f"gcc -O2 -pthread -o {read_bin} {read_c} && "
        f"gcc -O2 -pthread -o {write_bin} {write_c}",
        long_running=True,
    )
    return read_bin, write_bin


def discover_ces_nodes(installer, config=None):
    """
    Return CES node hostnames for recycle tests.

    Order of preference:
      1. config ``ces_nodes`` list (explicit)
      2. ``mmlscluster --ces`` node name column
      3. empty list (caller should skip or fail clearly)
    """
    conf = config or {}
    if conf.get("ces_nodes"):
        nodes = list(conf["ces_nodes"])
        log.info("Using ces_nodes from config: %s", nodes)
        return nodes

    mm = "/usr/lpp/mmfs/bin"
    # CES table rows typically: <id> <node name> <node ip> <ces ip> ...
    script = f"""
set +e
PATH="{mm}:$PATH"
if mmlscluster --ces >/tmp/mmls_ces.out 2>/dev/null; then
  awk '
    BEGIN {{ skip=1 }}
    /Node Name/ || /node name/ {{ skip=0; next }}
    skip {{ next }}
    /^[[:space:]]*[0-9]+[[:space:]]+/ {{ print $2 }}
  ' /tmp/mmls_ces.out
else
  # Fallback: all GPFS nodes that are not obviously clients
  mmlscluster 2>/dev/null | awk '/^[ ]*[0-9]+[ ]+/ {{ print $2 }}'
fi
"""
    out, _ = installer.exec_command(
        sudo=True, cmd=f"bash -lc {shlex.quote(script)}", check_ec=False
    )
    nodes = []
    for line in (out or "").splitlines():
        name = line.strip()
        if not name or name.lower() in ("node", "name", "daemon"):
            continue
        if name not in nodes:
            nodes.append(name)
    log.info("Discovered CES/Scale nodes: %s", nodes)
    return nodes
