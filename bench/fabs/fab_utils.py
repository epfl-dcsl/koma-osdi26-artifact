#!/usr/bin/python
# Author: Rui Yang (DCSL at EPFL)

"""
Utils for benchmarking code 
"""
import getpass
import math
import os
import shlex
from statistics import  mean

from fabric import Connection
import fab_config


def getenv_any(names: list[str], default: str | None = None) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return default


def resolve_remote_username(username: str | None = None) -> str:
    if username and username != getpass.getuser():
        return username
    return fab_config.DEFAULT_REMOTE_USER


def resolve_remote_home_dir(username: str, conn: Connection | None = None) -> str:
    configured_home_dir = getenv_any(["KOMA_OSDI_HOME_DIR"])
    if configured_home_dir:
        return configured_home_dir

    if conn is not None:
        result = conn.run('printf "%s" "$HOME"', warn=True, hide=True)
        home_dir = result.stdout.strip()
        if result.ok and home_dir:
            return home_dir

    expanded_home_dir = os.path.expanduser("~{}".format(username))
    if expanded_home_dir == "~{}".format(username):
        raise RuntimeError("failed to resolve home directory for user {}".format(username))
    return expanded_home_dir


class Logger(object):
    def __init__(self, log_path="default_log.log"):
        import sys
        self.terminal = sys.stdout
        self.log = open(log_path, "wb", buffering=0)

    def print(self, *message):
        message = ",".join([str(it) for it in message])
        self.terminal.write(str(message) + "\n")
        self.log.write(str(message).encode('utf-8') + b"\n")

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def close(self):
        self.log.close()


class ProjDir(object):
    def __init__(self, username: str | None = None, conn: Connection | None = None):
        """
        :param username: Ubuntu username of (e.g., ryang, root)
        """ 
        self.username = resolve_remote_username(username)
        self.home_dir = resolve_remote_home_dir(self.username, conn)

        self.grpc_dir = "{}/grpc".format(self.home_dir)
        self.proj_dir = getenv_any(
            ["KOMA_OSDI_PROJECT_DIR"],
            "{}/koma-osdi26-artifact".format(self.home_dir),
        )
        self.project_repo_url = getenv_any(
            ["KOMA_OSDI_PROJECT_REPO"],
            "git@github.com:epfl-dcsl/koma-osdi26-artifact.git",
        )
        self.ssh_private_key = getenv_any(
            ["KOMA_OSDI_SSH_KEY"],
            "{}/.ssh/id_rsa".format(self.home_dir),
        )

        self.bench_dir = "{}/bench".format(self.proj_dir)
        self.exp_config_dir = "{}/exp_config".format(self.bench_dir)
        self.res_dir = "{}/results".format(self.proj_dir)
        self.plot_dir = "{}/plots".format(self.bench_dir)

        self.third_party_dir = "{}/third_party".format(self.proj_dir)
        self.lancet_dir = "{}/lancet-cplusplus".format(self.third_party_dir)
        self.server_dir = "{}/servers".format(self.bench_dir)
        self.coord_dir = "{}/coordinator".format(self.lancet_dir)
        self.schedsim_dir = "{}/schedsim".format(self.third_party_dir)

        self.koma_root_dir = "{}/koma".format(self.proj_dir)
        self.koma_patches_dir = "{}/patches".format(self.koma_root_dir)
        self.koma_plain_dir = getenv_any(
            ["KOMA_OSDI_KOMA_PLAIN_DIR"],
            "{}/plain-tcp".format(self.koma_root_dir),
        )
        self.koma_grpc_dir = getenv_any(
            ["KOMA_OSDI_KOMA_GRPC_DIR"],
            "{}/grpc".format(self.koma_root_dir),
        )
        self.koma_tls_dir = getenv_any(
            ["KOMA_OSDI_KOMA_TLS_DIR"],
            "{}/tls".format(self.koma_root_dir),
        )
        self.grpc_go_module_url = getenv_any(
            ["KOMA_OSDI_GRPC_GO_MODULE_URL"],
            "github.com/rainayangg/grpc-go",
        )
        self.grpc_go_module_version = getenv_any(
            ["KOMA_OSDI_GRPC_GO_MODULE_VERSION"],
            "v0.0.0-20260325234553-a764f0be0b0d",
        )
        self.net_go_module_url = getenv_any(
            ["KOMA_OSDI_NET_GO_MODULE_URL"],
            "github.com/rainayangg/net-go",
        )
        self.net_go_module_version = getenv_any(
            ["KOMA_OSDI_NET_GO_MODULE_VERSION"],
            "v0.0.0-20260313180441-b5081d74e7cf",
        )
        self.kernel_tree_dir = getenv_any(
            ["KOMA_OSDI_KERNEL_TREE"],
            "{}/linux-6.8".format(self.home_dir),
        )
        self.kernel_tarball = getenv_any(
            ["KOMA_OSDI_KERNEL_TARBALL"],
            "{}/linux-6.8.tar.gz".format(self.home_dir),
        )
        self.kernel_tarball_url = getenv_any(
            ["KOMA_OSDI_KERNEL_TARBALL_URL"],
            "https://mirrors.edge.kernel.org/pub/linux/kernel/v6.x/linux-6.8.tar.gz",
        )
        self.ktls_kernel_release = getenv_any(
            ["KOMA_OSDI_KTLS_KERNEL_RELEASE"],
            "6.8.0-koma",
        )

        self.throu_res = "{}/throughput_res.txt".format(self.res_dir)
        self.lat_res = "{}/latency_res.txt".format(self.res_dir)

    @classmethod
    def from_connection(cls, conn: Connection | None = None):
        conn_user = getattr(conn, "user", None) if conn is not None else None
        return cls(username=conn_user, conn=conn)

    def __str__(self):
        """
        return the string type of the project directory name
        """
        return self.proj_dir

    def __repr__(self):
        return self.__str__()       


def _first_nonempty_output(conn: Connection, commands: list[str]) -> str:
    for cmd in commands:
        result = conn.run(cmd, warn=True, hide=True)
        output = result.stdout.strip()
        if output:
            return output.splitlines()[0].strip()
    raise RuntimeError("failed to detect network interface information")


def get_iface(conn) -> str:
    configured_iface = os.environ.get("KOMA_OSDI_IFACE")
    if configured_iface:
        return configured_iface

    return _first_nonempty_output(conn, [
        "ip -o -4 addr show scope global | awk '$4 ~ /^10\\./ {print $2; exit}'",
        "ip -o route show to default | awk '{print $5; exit}'",
        "ifconfig | grep BROADCAST | cut -d \":\" -f1 | head -n1",
    ])

def get_ip_addr(conn) -> str:
    configured_ip = os.environ.get("KOMA_OSDI_IP_ADDR")
    if configured_ip:
        return configured_ip

    iface = get_iface(conn)
    return _first_nonempty_output(conn, [
        "ip -o -4 addr show dev {} scope global | awk '$4 ~ /^10\\./ {{sub(/\\/.*/, \"\", $4); print $4; exit}}'".format(iface),
        "ip -o -4 addr show dev {} | awk '{{print $4}}' | cut -d/ -f1".format(iface),
        "ifconfig {} | grep 'inet ' | awk '{{print $2}}' | head -n1".format(iface),
    ])

def get_cores_from_numa(conn):
    iface = get_iface(conn)
    numa_node = conn.run("cat /sys/class/net/{}/device/numa_node".format(iface)).stdout.rsplit('\n')[0]
    numa_cores = conn.run("numactl --hardware | grep \"node {} cpus\"".format(numa_node)).stdout

    ######### Get all cores which are in the numa node sharing memory with NIC #########
    # numa_cores = numa_cores.rsplit('\n')[0].rsplit(': ')[-1].replace(" ", ",")

    ######### DANGEROUS: Get 4 cores in the numa node sharing memory with NIC #########
    numa_cores = numa_cores.rsplit('\n')[0].rsplit(': ')[-1]
    numa_cores = ",".join(numa_cores.split()[:24])
    print("NUMA node: {}, cores: {}".format(numa_node, numa_cores))

    return numa_node, numa_cores
    
def kill_process(c: Connection, keyword: str):
    """
    key one or multiple processes which match the keyword
    :param c: connection 
    :param keyword: keyword belonging to the process to be killed
    """
    # cmd = "kill -9 $(ps ax | grep {} | fgrep -v grep | awk '{{ print $1 }}')".format(keyword)
    cmd = "sudo pkill -9 -f {}".format(keyword)
    print(cmd)
    c.run(cmd, warn=True)

def unload_kmodule(conn: Connection, mdir: str, mname: str):
    """
    Unload a kernel module 
    :param conn: connection
    :param mdir: kept for call-site symmetry with load_kmodule
    :param mname: the name of the kernel module, e.g., koma
    """
    conn.run("sudo rmmod {}".format(mname), warn=True)

def load_kmodule(conn: Connection, mdir: str, mname: str):
    """
    load a kernel module 
    :param c: connection 
    :param mdir: directory which contains "<mname>.ko"
    :param mname: the name of the kernel module, e.g., koma
    """
    if not conn.run("test -f {}/{}.ko".format(mdir, mname), warn=True, hide=True).ok:
        raise RuntimeError("kernel module {} is missing from {}".format(mname, mdir))
    with conn.cd(mdir):
        conn.run("sudo insmod ./{}.ko".format(mname), warn=False)

def mean_val(l):
    values = [i for i in l if not (isinstance(i, float) and math.isnan(i))]
    if not values:
        return float("nan")
    return mean(values)

def lowest_val(l):
    return min([i for i in l])

def ulimit_set(conn):
    fd_limit = 655350
    conn.sudo("sh -c 'echo \"fs.file-max={}\" >> /etc/sysctl.conf'".format(fd_limit))
    conn.sudo("sysctl -w fs.file-max={}".format(fd_limit), warn=True)
    cmd = "sh -c 'echo \"* soft     nproc          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)
    cmd = "sh -c 'echo \"* hard     nproc          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)
    cmd = "sh -c 'echo \"* soft     nofile          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)
    cmd = "sh -c 'echo \"* hard     nofile          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)

    cmd = "sh -c 'echo \"root soft     nproc          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)
    cmd = "sh -c 'echo \"root hard     nproc          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)
    cmd = "sh -c 'echo \"root soft     nofile          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)
    cmd = "sh -c 'echo \"root hard     nofile          {} \" >> /etc/security/limits.conf'".format(fd_limit)
    conn.sudo(cmd)

    # Keep this idempotent. Repeatedly appending pam_limits.so can break SSH/PAM
    # sessions. Recovery on a bad node:
    #   sudo awk 'BEGIN{seen=0} /^session[[:space:]]+required[[:space:]]+pam_limits\.so[[:space:]]*$/ { if (seen++) next } { print }' /etc/pam.d/common-session | sudo tee /tmp/common-session.clean >/dev/null
    #   sudo cp /tmp/common-session.clean /etc/pam.d/common-session
    #   sudo sed -i -E 's/^session[[:space:]]+required[[:space:]]+pam_loginuid\.so/session optional pam_loginuid.so/' /etc/pam.d/sshd
    cmd = "sh -c 'grep -qxF \"session required pam_limits.so\" /etc/pam.d/common-session || echo \"session required pam_limits.so\" >> /etc/pam.d/common-session'"
    conn.sudo(cmd, warn=True)
    conn.run("ulimit -n {}".format(fd_limit), warn=True)
