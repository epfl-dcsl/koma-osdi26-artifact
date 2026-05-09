"""
experiment-related classes for benchmarking code 
"""

from fabric import Connection
from fab_config import *
from fab_utils import *
from enum import Enum
from dataclasses import dataclass, field
import os, datetime, time, shlex, textwrap
import yaml
import concurrent.futures
import shutil

class ExperimentType(Enum):
    GRPC20_Conn24 = 1
    GRPC20_Conn5000 = 2

    Vanilla100_Conn20 = 3
    Vanilla100_Conn80 = 4
    Vanilla100_Conn5000 = 5
    Vanilla20_Conn80 = 6
    TLS100_Conn80 = 7
    TLS20_Conn80 = 8
    Silo_Conn80 = 9
    Silo_Conn80_TLS = 10
    Silo_GRPC_Conn24 = 12
    Silo_GRPC_Conn5000 = 13

    UNCLASSIFIED = 24


@dataclass
class ExperimentConfig(object):
    """
    :param proj_dir: proj directory info -> ProjDir
    :param conn: fabric2 connection object
    :param exp_type: experiment type -> ExperimentType
    :param lancet_config: the configuration of lancet
    :param server_config: the configuration of servers
    :param dist_config: the configuration of distribution for the service time
    :param res_head: the head line for result csv file
    """ 
    proj_dir: ProjDir
    conn: Connection
    exp_config: MachinesConfig 
    exp_type: str = ExperimentType.UNCLASSIFIED.name 
    server_config: list[str] = field(default_factory=list)
    dist_config: list[str] = field(default_factory=list)
    res_head: str = ""
        
    def __post_init__(self):
        self.clients = self.exp_config['client']
        self.servers = self.exp_config['server']
        self.coordinator = self.exp_config['coordinator']
        self.listening_port = self.exp_config['port']
        self.lancet_config = self.exp_config['lancet_config']

        self.clients_conn: list[Connection] = list()
        self.servers_conn: list[Connection] = list()
        self.coordinator_conn: list[Connection] = list()
        self.connect_all()
        self.exp_timestamp = "{:%Y%m%d-%H%M%S}".format(datetime.datetime.now())
        
        # config used for the experiment run
        if self.exp_type != ExperimentType.UNCLASSIFIED.name:
            self.config_path = "{}/{}.yaml".format(self.proj_dir.exp_config_dir, self.exp_type)
            self.get_exp_config()
            self.res_dir = os.path.join(self.proj_dir.res_dir, "{}/{}".format(self.exp_type, self.exp_timestamp))
            self.create_result_dir(self.res_dir)

            # result files: i) logger including commands, intermediate results, etc.,  ii) raw csv result, iii) sanitized csv result
            self.logger = Logger(os.path.join(self.res_dir, "{}_{}.log").format(self.exp_type, self.exp_timestamp))
            self.raw_res = os.path.join(self.res_dir, "raw_{}_{}.csv").format(self.exp_type, self.exp_timestamp)
            self.sanitized_res = os.path.join(self.res_dir, "sanitized_{}_{}.csv").format(self.exp_type, self.exp_timestamp)
            self.write_res_head()

    def create_result_dir(self, path_name: str):
         self.conn.run("mkdir -p {}".format(path_name))

    def get_exp_config(self):
        with open(self.config_path, 'r') as config_file:
            self.run_config = yaml.safe_load(config_file)

    def connect_all(self):
        remote_user = resolve_remote_username(getattr(self.conn, "user", None))
        ssh_key_path = os.path.expanduser(DEFAULT_SSH_PRIVATE_KEY)
        connect_kwargs = {
            "allow_agent": ENABLE_SSH_AGENT_FORWARDING,
            "look_for_keys": True,
            "disabled_algorithms": {"pubkeys": ["dss"]},
        }
        if ssh_key_path and os.path.exists(ssh_key_path):
            connect_kwargs["key_filename"] = [ssh_key_path]
        for client in self.clients:
            self.clients_conn.append(
                Connection(
                    host=client,
                    user=remote_user,
                    forward_agent=ENABLE_SSH_AGENT_FORWARDING,
                    connect_kwargs=dict(connect_kwargs),
                )
            )
        for server in self.servers:
            self.servers_conn.append(
                Connection(
                    host=server,
                    user=remote_user,
                    forward_agent=ENABLE_SSH_AGENT_FORWARDING,
                    connect_kwargs=dict(connect_kwargs),
                )
            )
        for coordinator in self.coordinator:
            self.coordinator_conn.append(
                Connection(
                    host=coordinator,
                    user=remote_user,
                    forward_agent=ENABLE_SSH_AGENT_FORWARDING,
                    connect_kwargs=dict(connect_kwargs),
                )
            )

            
    def write_res_head(self):
        with open(self.raw_res, 'w') as raw_out_file:
            with open(self.sanitized_res, 'w') as sanitized_out_file:
                with open(self.config_path, 'r') as config_file:
                    config_lines = config_file.readlines()
                    raw_out_file.writelines(config_lines)
                raw_out_file.write(self.res_head)
                sanitized_out_file.write(self.res_head)

@dataclass
class ExperimentRun(object):
    def __init__(
        self, 
        exp_config: ExperimentConfig, 
        # proj_dir: ProjDir,
        ):
        """
        :param exp_config: configuration of the experiment run -> ExperimentConfig
        :param dirs: proj directory info -> ProjDir
        :param client_script: function name of the client script (e.g., lancet)
        :paran server_script: function name of the server script (e.g., spin-linux)
        """
        self.exp_config = exp_config 
        self.dirs = exp_config.proj_dir

        self.lancet_agents: str = ",".join(self.exp_config.clients)
        self.load_agents: str = ",".join(self.exp_config.clients[1:])
        self.latency_agents: str = self.exp_config.clients[0]

        self.coord_conn = self.exp_config.coordinator_conn[0]
        self.server_conn = self.exp_config.servers_conn[0]
        self._apt_updated_hosts = set()
        self._go_setup_hosts = set()
        self._silo_hugepages_ready = False

    def log(self, *message):
        if hasattr(self.exp_config, "logger"):
            self.exp_config.logger.print(*message)
        else:
            print(",".join([str(it) for it in message]))

    def get_target_host(self):
        if 'target_host' in self.exp_config.run_config:
            return self.exp_config.run_config['target_host']
        return "{}:{}".format(get_ip_addr(self.exp_config.servers_conn[0]), self.exp_config.listening_port)

    def git_env_prefix(self, repo_url: str) -> str:
        if repo_url.startswith("git@") or repo_url.startswith("ssh://"):
            ssh_cmd = "ssh -o StrictHostKeyChecking=accept-new"
            return "GIT_SSH_COMMAND={} ".format(shlex.quote(ssh_cmd))
        return ""

    def ensure_repo(self, conn: Connection, repo_url: str, repo_dir: str, recursive: bool = False):
        repo_parent = os.path.dirname(repo_dir)
        repo_dir_q = shlex.quote(repo_dir)
        repo_git_dir_q = shlex.quote(os.path.join(repo_dir, ".git"))
        repo_parent_q = shlex.quote(repo_parent)
        repo_url_q = shlex.quote(repo_url)
        git_env = self.git_env_prefix(repo_url)

        conn.run("mkdir -p {}".format(repo_parent_q))
        if not conn.run("test -d {}".format(repo_git_dir_q), warn=True, hide=True).ok:
            clone_flags = "--recursive " if recursive else ""
            conn.run("{}git clone {}{} {}".format(git_env, clone_flags, repo_url_q, repo_dir_q))

        with conn.cd(repo_dir):
            conn.run("{}git fetch --all --tags --prune".format(git_env), warn=True)
            conn.run("{}git pull --ff-only".format(git_env), warn=True)
            if recursive:
                conn.run("{}git submodule update --init --recursive".format(git_env), warn=True)

    def ensure_project_repo(self, conn: Connection):
        self.ensure_repo(conn, self.dirs.project_repo_url, self.dirs.proj_dir, recursive=True)

    def _configure_server_grpc_go_mod(self, use_koma: bool):
        replace_lines: list[str] = []
        if use_koma:
            replace_lines = [
                "replace google.golang.org/grpc => {} {}".format(
                    self.dirs.grpc_go_module_url,
                    self.dirs.grpc_go_module_version,
                ),
                "replace golang.org/x/net => {} {}".format(
                    self.dirs.net_go_module_url,
                    self.dirs.net_go_module_version,
                ),
            ]
        replace_block = "\n".join(replace_lines + [""]) if replace_lines else ""
        go_mod_path = os.path.join(self.dirs.server_dir, "go.mod")
        script = textwrap.dedent(
            """
            from pathlib import Path
            import re

            go_mod_path = Path({go_mod_path!r})
            text = go_mod_path.read_text()
            text = re.sub(r'(?m)^\\s*(?://)?replace google\\.golang\\.org/grpc => .*\\n?', '', text)
            text = re.sub(r'(?m)^\\s*(?://)?replace golang\\.org/x/net => .*\\n?', '', text)

            match = re.search(r'(?m)^require \\($', text)
            if match is None:
                raise SystemExit("failed to locate require block in %s" % (go_mod_path,))

            replace_block = {replace_block!r}
            prefix = replace_block + "\\n" if replace_block else ""
            text = text[:match.start()] + prefix + text[match.start():]
            go_mod_path.write_text(text)
            """
        ).format(go_mod_path=go_mod_path, replace_block=replace_block)
        self.server_conn.run("python3 - <<'PY'\n{}\nPY".format(script))

    def _refresh_server_go_modules(self):
        with self.server_conn.cd(self.dirs.server_dir):
            self.server_conn.run("/usr/local/go/bin/go mod tidy")

    def _build_server_target(self, target: str):
        with self.server_conn.cd(self.dirs.server_dir):
            self.server_conn.run("make {}".format(target))

    def ensure_silo_hugepages(self):
        if self._silo_hugepages_ready:
            return

        hugepage_node = 0
        hugepage_count = 16384
        hugepage_path = (
            "/sys/devices/system/node/node{}/hugepages/hugepages-2048kB/nr_hugepages".format(
                hugepage_node
            )
        )
        self.log(
            "Configuring 2 MiB hugepages on NUMA node {}: {}".format(
                hugepage_node, hugepage_count
            )
        )
        self.server_conn.run(
            "printf '%s\\n' {count} | sudo tee {path} >/dev/null".format(
                count=shlex.quote(str(hugepage_count)),
                path=shlex.quote(hugepage_path),
            )
        )
        configured = self.server_conn.run(
            "cat {}".format(shlex.quote(hugepage_path)),
            hide=True,
        ).stdout.strip()
        if configured != str(hugepage_count):
            raise RuntimeError(
                "failed to configure Silo hugepages: expected {}, got {}".format(
                    hugepage_count,
                    configured or "<empty>",
                )
            )
        self._silo_hugepages_ready = True

    def wait_for_remote_tcp_port(self, port: int, timeout_secs: int = 30):
        wait_cmd = textwrap.dedent(
            """
            for _ in $(seq 1 {attempts}); do
                if ss -ltn | grep -q ':{port} '; then
                    exit 0
                fi
                sleep 1
            done
            exit 1
            """
        ).format(port=port, attempts=timeout_secs)
        self.server_conn.run(wait_cmd)

    def _conn_key(self, conn: Connection):
        return (getattr(conn, "user", None), getattr(conn, "host", None), getattr(conn, "port", None))

    def _unique_connections(self, conns: list[Connection]) -> list[Connection]:
        unique = {}
        for conn in conns:
            unique[self._conn_key(conn)] = conn
        return list(unique.values())

    def _apt_install(self, conn: Connection, packages: list[str]):
        conn_key = self._conn_key(conn)
        if conn_key not in self._apt_updated_hosts:
            conn.run("sudo apt-get update")
            self._apt_updated_hosts.add(conn_key)
        package_list = " ".join(shlex.quote(pkg) for pkg in packages)
        conn.run("sudo DEBIAN_FRONTEND=noninteractive apt-get install -y {}".format(package_list))

    def patch_cloudlab_koma_kernel(self):
        self.server_conn.run("sudo apt-get update")
        self.server_conn.run(
            "sudo apt-get install -y build-essential bc cpio rsync git wget ca-certificates "
            "libncurses-dev gawk flex bison openssl libssl-dev dkms dwarves libelf-dev "
            "libudev-dev libpci-dev libiberty-dev autoconf sysstat iperf "
            # Silo build dependencies (used by bench/servers/silo).
            "libjemalloc-dev libdb++-dev libaio-dev libnuma-dev"
        )
        self.ensure_project_repo(self.server_conn)
        with self.server_conn.cd(self.dirs.home_dir):
            self.server_conn.run(
                "test -f {tarball} || wget -O {tarball} {url}".format(
                    tarball=shlex.quote(self.dirs.kernel_tarball),
                    url=shlex.quote(self.dirs.kernel_tarball_url),
                )
            )
            self.server_conn.run(
                "test -d {kernel_dir} || tar xzvf {tarball}".format(
                    kernel_dir=shlex.quote(self.dirs.kernel_tree_dir),
                    tarball=shlex.quote(self.dirs.kernel_tarball),
                )
            )

        koma_patch = os.path.join(self.dirs.koma_patches_dir, "koma.patch")
        ktls_patch = os.path.join(self.dirs.koma_patches_dir, "ktls-module.patch")
        self._apply_kernel_patch_if_needed(koma_patch, "koma.patch")
        self._apply_kernel_patch_if_needed(ktls_patch, "ktls-module.patch")

        with self.server_conn.cd(self.dirs.kernel_tree_dir):
            self.server_conn.run("cp /boot/config-`uname -r` ./.config")
            self.server_conn.run("make olddefconfig")
            self.server_conn.run("scripts/config --disable SYSTEM_TRUSTED_KEYS")
            self.server_conn.run("scripts/config --disable SYSTEM_REVOCATION_KEYS")
            self.server_conn.run("yes '' | make -j$(nproc) bzImage")
            self.server_conn.run("make -j$(nproc) modules")
            self.server_conn.run("sudo make modules_install")
            self.server_conn.run("sudo make install")
            self.server_conn.run(
                r"sudo sed -i 's/^GRUB_DEFAULT=.*/GRUB_DEFAULT=\"1>Ubuntu, with Linux 6.8.0-koma\"/' /etc/default/grub"
            )
            self.server_conn.run("sudo update-grub")
            self.log("Finished patching cloudlab server with KOMA kernel (koma + ktls)")
        self._reboot_server_and_wait()

    def _apply_kernel_patch_if_needed(self, patch_path: str, patch_label: str):
        patch_q = shlex.quote(patch_path)
        with self.server_conn.cd(self.dirs.kernel_tree_dir):
            patch_check = self.server_conn.run(
                "git apply --check {}".format(patch_q),
                warn=True,
                hide=True,
            )
            if patch_check.ok:
                self.server_conn.run("git apply {}".format(patch_q))
                self.log("Applied {}".format(patch_label))
                return

            already_applied = self.server_conn.run(
                "git apply --reverse --check {}".format(patch_q),
                warn=True,
                hide=True,
            )
            if already_applied.ok:
                self.log("{} already applied".format(patch_label))
                return

            patch_error = (patch_check.stderr or patch_check.stdout).strip()
            raise RuntimeError(
                "failed to apply {} on cloudlab server: {}".format(
                    patch_label, patch_error or "git apply --check failed"
                )
            )

    def _wait_for_server_ssh(self, timeout_secs: int = 360):
        deadline = time.time() + timeout_secs
        last_exc = None
        while time.time() < deadline:
            try:
                self.server_conn.run("true", hide=True)
                self.log("Server SSH is back")
                return
            except Exception as exc:
                last_exc = exc
                try:
                    self.server_conn.close()
                except Exception:
                    pass
                time.sleep(5)
        raise RuntimeError("server did not return after reboot") from last_exc

    def _reboot_server_and_wait(self):
        self.log("Rebooting server")
        self.server_conn.run(
            "sudo nohup sh -c 'sleep 1; reboot' >/tmp/koma-bench-reboot.log 2>&1 &",
            warn=True,
        )
        self.server_conn.close()
        time.sleep(10)
        self._wait_for_server_ssh()

    def _installed_ktls_module_paths(self, release: str) -> tuple[str, str, str]:
        installed_dir = "/lib/modules/{}/kernel/net/tls".format(release)
        installed_ko = "{}/tls.ko".format(installed_dir)
        installed_zst = "{}/tls.ko.zst".format(installed_dir)
        return installed_dir, installed_ko, installed_zst

    def verify_cloudlab_ktls_module(self):
        release = self.dirs.ktls_kernel_release
        _, installed_ko, installed_zst = self._installed_ktls_module_paths(release)
        installed_candidates = [installed_zst, installed_ko]
        self.server_conn.run(
            "test \"$(uname -r)\" = {}".format(shlex.quote(release))
        )
        self.server_conn.run("grep -qx 'CONFIG_TLS=m' /boot/config-$(uname -r)")
        installed_module = None
        for candidate in installed_candidates:
            if self.server_conn.run(
                "test -f {}".format(shlex.quote(candidate)),
                warn=True,
                hide=True,
            ).ok:
                installed_module = candidate
                break
        if installed_module is None:
            raise RuntimeError(
                "failed to locate installed tls module under {}".format(
                    os.path.dirname(installed_zst)
                )
            )
        self.server_conn.run("sudo modprobe tls")

        loaded_src = self.server_conn.run(
            "cat /sys/module/tls/srcversion",
            hide=True,
        ).stdout.strip()
        installed_src = self.server_conn.run(
            "modinfo -F srcversion {}".format(shlex.quote(installed_module)),
            hide=True,
        ).stdout.strip()
        resolved_module_path = self.server_conn.run(
            "modinfo -n tls",
            hide=True,
        ).stdout.strip()
        if not loaded_src or loaded_src != installed_src:
            raise RuntimeError(
                "kTLS module verification failed: loaded srcversion {} != installed srcversion {} ({})".format(
                    loaded_src, installed_src, installed_module
                )
            )
        self.log(
            "Verified patched kTLS module srcversion {} from {}".format(
                loaded_src, installed_module
            )
        )
        return {
            "kernel_release": release,
            "loaded_srcversion": loaded_src,
            "installed_srcversion": installed_src,
            "installed_module": installed_module,
            "resolved_module_path": resolved_module_path,
        }

    def setup_go(self, conn):
        conn_key = self._conn_key(conn)
        if conn_key in self._go_setup_hosts:
            return
        go_version = os.environ.get("KOMA_OSDI_GO_VERSION", "1.25.0")
        go_archive = "go{}.linux-amd64.tar.gz".format(go_version)
        go_binary = "/usr/local/go/bin/go"
        with conn.cd(self.dirs.home_dir):
            conn.run(
                "command -v wget >/dev/null 2>&1 || "
                "(sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y wget ca-certificates)"
            )
            if not conn.run("{} version | grep -q 'go{}'".format(go_binary, go_version), warn=True, hide=True).ok:
                conn.run(
                    "test -f {archive} || wget -O {archive} https://go.dev/dl/{archive}".format(
                        archive=shlex.quote(go_archive)
                    )
                )
                conn.run(
                    "sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf {}".format(
                        shlex.quote(go_archive)
                    )
                )
            conn.run("sudo ln -sf /usr/local/go/bin/go /usr/local/bin/go")
            conn.run("sudo ln -sf /usr/local/go/bin/gofmt /usr/local/bin/gofmt")
            conn.run("{} env -w GO111MODULE=auto".format(go_binary))
            conn.run("rm -rf ~/tmp_gomod && mkdir -p ~/tmp_gomod")
        with conn.cd("~/tmp_gomod"):
            conn.run("{} mod init tmp".format(go_binary))
            conn.run("{} get golang.org/x/crypto/...".format(go_binary))
        with conn.cd(self.dirs.home_dir):
            conn.run("rm -rf ~/tmp_gomod")
            # conn.run("go get -u golang.org/x/crypto/...")
        self._go_setup_hosts.add(conn_key)

    def __setup_lancet(self, conn):
        """
        Set up lancet, including installing its dependencies, builting, etc
        Param conn: connection towards the target server to set up lancet 
        """
        # install golang 
        self.setup_go(conn)

        self._apt_install(conn, [
            "cmake",
            "git",
            "ca-certificates",
            "python3-pip",
            "libssl-dev",
            "python3-virtualenv",
            "python3-venv",
            "libprotobuf-dev",
            "libgrpc++-dev",
            "libgrpc-dev",
        ])

    def __setup_lancet_coordinator(self):
        self.__setup_lancet(self.coord_conn)
        self._apt_install(self.coord_conn, [
            "build-essential",
            "autoconf",
            "libtool",
            "pkg-config",
            "libprotobuf-dev",
            "protobuf-compiler",
            "protobuf-compiler-grpc",
            "libgrpc++-dev",
            "libgrpc-dev",
            "python3-setuptools",
            "python3-wheel",
        ])

    def __setup_grpc(self, conn):
        """
        Set up gRPC, including installing its dependencies, building, etc
        Param conn: connection towards the target server to set up lancet
        """
        with conn.cd(self.dirs.home_dir):
            conn.run("sudo apt-get -y install build-essential autoconf libtool pkg-config cmake")
            conn.run("git clone --recurse-submodules -b v1.71.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc")
        with conn.cd(self.dirs.grpc_dir):
            conn.run("mkdir -p cmake/build")
        with conn.cd(os.path.join(self.dirs.grpc_dir, "cmake/build")):
            conn.run("cmake ../..")
            conn.run("make")
        with conn.cd(self.dirs.grpc_dir):
            conn.run("sudo test/distrib/cpp/run_distrib_test_cmake_module_install.sh")
    
    def __update_linux(self):
        self.patch_cloudlab_koma_kernel()

    def __clean_lancet(self):
        for client in self.exp_config.clients_conn:
            kill_process(client, "lancet")

    def __clean_server(self):
        kill_process(self.server_conn, "spin")
        kill_process(self.server_conn, "silo")
        unload_kmodule(self.server_conn, self.dirs.koma_plain_dir, "koma")

    def __setup_server(self):
        self.server_conn.run(
            "sudo apt install -y python3-pip libbpf-dev libevent-dev gengetopt "
            "libzmq3-dev numactl dtach libssl-dev libjemalloc-dev libdb++-dev "
            "libaio-dev libnuma-dev"
        )
        self.server_conn.run("sudo apt install -y zip bison build-essential cmake flex git libedit-dev \
  libllvm18 llvm-18-dev libclang-18-dev python3 zlib1g-dev libelf-dev libfl-dev python3-setuptools \
  liblzma-dev libdebuginfod-dev arping netperf iperf libpolly-18-dev")

        bcc_dir = os.path.join(self.dirs.home_dir, "bcc")
        self.ensure_repo(self.server_conn, "https://github.com/iovisor/bcc.git", bcc_dir)
        bcc_build_dir = os.path.join(bcc_dir, "build")
        self.server_conn.run("mkdir -p {}".format(shlex.quote(bcc_build_dir)))
        with self.server_conn.cd(bcc_build_dir):
            self.server_conn.run("cmake ..")
            self.server_conn.run("make")
            self.server_conn.run("sudo make install")
            self.server_conn.run("cmake -DPYTHON_CMD=python3 ..")
        with self.server_conn.cd(os.path.join(bcc_build_dir, "src/python")):
            self.server_conn.run("make")
            self.server_conn.run("sudo make install")

        # Ensure the artifact repo is present on the server (koma kernel
        # modules build inside it; bench/servers/make runs from it). The
        # helper is idempotent and updates submodules.
        self.ensure_project_repo(self.server_conn)

        # set up golang on the server (needed for spin-grpc-go / silo-grpc-go)
        self.setup_go(self.server_conn)
        # Build all pinned Koma module variants against the currently running
        # kernel so experiments can load them without checkout-time mutation.
        for koma_dir in [
            self.dirs.koma_plain_dir,
            self.dirs.koma_grpc_dir,
            self.dirs.koma_tls_dir,
        ]:
            with self.server_conn.cd(koma_dir):
                self.server_conn.run("make clean", warn=True)
                self.server_conn.run("make")
        # one-time build of the user-space servers; per-experiment make refreshes
        with self.server_conn.cd(self.dirs.server_dir):
            self.server_conn.run("make")

        ulimit_set(self.server_conn)


    def setup_experiment(self):
        # setup server
        self.__setup_server()

        # setup grpc at clients and server
        # with concurrent.futures.ThreadPoolExecutor() as executor:
            # futures = [executor.submit(self.__setup_grpc, node) for node in self.exp_config.clients_conn + [self.server_conn]]
            # futures = [executor.submit(self.__setup_grpc, node) for node in self.exp_config.clients_conn ]
            # concurrent.futures.wait(futures)
        # setup lancet clients 
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self.__setup_lancet, node): node
                for node in self._unique_connections(self.exp_config.clients_conn)
            }
            for future in concurrent.futures.as_completed(futures):
                node = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    raise RuntimeError(
                        "failed to set up Lancet on {}@{}".format(node.user, node.host)
                    ) from exc
        self.__setup_lancet_coordinator()
        self.ensure_project_repo(self.coord_conn)
        
    def __update_repo(self, conn):
        with conn.cd(self.dirs.proj_dir):
            conn.run("git fetch --all --tags --prune", warn=True)
            conn.run("git pull --ff-only", warn=True)
            conn.run("git submodule sync --recursive", warn=True)
            conn.run("git submodule update --init --recursive", warn=True)

    def deploy_experiment(self):
        """
        Deploy lancet, including setup the coordinator machines, and prepare agents 
        """
        self.__setup_lancet_coordinator()
        self.ensure_project_repo(self.coord_conn)
        with self.coord_conn.cd(self.dirs.lancet_dir):
            self.coord_conn.run("make prepare_clients HOSTS={}".format(self.lancet_agents))
            self.coord_conn.run("make coordinator agents manager")
            self.coord_conn.run("make deploy HOSTS={}".format(self.lancet_agents))

    def update_experiment(self):
        self.__update_repo(self.server_conn)

    def clean_experiment(self):
        self.__clean_lancet()
        self.__clean_server()

    def run_server(self, s_config, d_config):
        """
        s_config: config for the server (e.g., grpc-go, grpc-koma-go)
        d_config: distribution for service time (fixed, exponential, bimodal)
        """
        server_conn =self.exp_config.servers_conn[0]
        ulimit_set(self.exp_config.servers_conn[0])

        ######## gRPC server ########
        if s_config == "grpc-koma-go":
            server_name = "spin-grpc-go"
            unload_kmodule(server_conn, self.dirs.koma_grpc_dir, "koma")
            load_kmodule(server_conn, self.dirs.koma_grpc_dir, "koma")
        elif s_config == "grpc-go":
            server_name = "spin-grpc-go"

        ######### vanilla servers #########
        elif s_config == "partition" or s_config == "floating":
            server_name = "spin-linux"
        elif s_config == "kcm_partition" or s_config == "kcm_floating":
            server_name = "spin-kcm"
        elif s_config == "pool":
            server_name = "spin-linux-pool"
        elif s_config == "koma":
            server_name = "spin-koma"
            unload_kmodule(server_conn, self.dirs.koma_plain_dir, "koma")
            load_kmodule(server_conn, self.dirs.koma_plain_dir, "koma")
        
        ######### vanilla TLS servers #########
        elif s_config == "floating-tls":
            server_name = "spin-linux-tls"

        elif s_config == "pool-tls":
            server_name = "spin-linux-pool-tls"

        elif s_config == "koma-tls":
            server_name = "spin-koma-tls"
            unload_kmodule(server_conn, self.dirs.koma_tls_dir, "koma")
            load_kmodule(server_conn, self.dirs.koma_tls_dir, "koma")
            self.verify_cloudlab_ktls_module()

        ######### SILO servers #########
        elif s_config == "silo-kcm_partition" or s_config == "silo-kcm_floating":
            server_name = "silo-kcm"
        elif s_config == "silo-partition" or s_config == "silo-floating":
            server_name = "silo-linux"
        elif s_config == "silo-koma":
            server_name = "silo-koma"
            unload_kmodule(server_conn, self.dirs.koma_plain_dir, "koma")
            load_kmodule(server_conn, self.dirs.koma_plain_dir, "koma")
        elif s_config == "silo-grpc-koma-go":
            server_name = "silo-grpc-go"
            unload_kmodule(server_conn, self.dirs.koma_grpc_dir, "koma")
            load_kmodule(server_conn, self.dirs.koma_grpc_dir, "koma")
        elif s_config == "silo-grpc-go":
            server_name = "silo-grpc-go"
        elif s_config == "silo-pool":
            server_name = "silo-linux-pool"

        ######### SILO TLSservers #########
        elif s_config == "silo-koma-tls":
            server_name = "silo-koma-tls"
            unload_kmodule(server_conn, self.dirs.koma_tls_dir, "koma")
            load_kmodule(server_conn, self.dirs.koma_tls_dir, "koma")
            self.verify_cloudlab_ktls_module()
        elif s_config == "silo-floating-tls":
            server_name = "silo-linux-tls"
        elif s_config == "silo-pool-tls":
            server_name = "silo-linux-pool-tls"
        else:
            self.exp_config.logger.print("Wrong server configuration: {}, exit!".format(s_config))
            exit(1)

        with server_conn.cd(self.dirs.server_dir):
            server_conn.run("ulimit -n 100000")
            numa_node, numa_cores = get_cores_from_numa(server_conn)
            # numa_cmd = "numactl -N{} -m{} nice -20 taskset -c {} ".format("0", "0", "0,1")
            numa_cmd = "sudo numactl -N{} -m{} nice -20 taskset -c {} ".format(numa_node, numa_node, numa_cores)
            if d_config == 'fixed':
                cmd = "./{} {}:{} {}".format(server_name, d_config, 
                                             self.exp_config.run_config['service_time'],
                                             self.exp_config.lancet_config['app_proto'])
            elif d_config == 'exponential':
                cmd = "./{} {}:{} {}".format(server_name, d_config, 
                                             1.0 / float(self.exp_config.run_config['service_time']),
                                             self.exp_config.lancet_config['app_proto'])
            elif d_config == 'bimodal':
                # bimodal-1:P[X = S¯/2] = .9; P[X = 5.5 × S¯] = .1
                # bimodal-2:P[X = S¯/2] = .999; P[X = 500.5×S¯] = .001
                cmd = "./{} {}:{},{},{} {}".format(server_name, d_config, 
                                             0.9, float(self.exp_config.run_config['service_time']) / 2,
                                             float(self.exp_config.run_config['service_time']) * 5.5,
                                             self.exp_config.lancet_config['app_proto'])
            elif "silo" in server_name:
                cmd = "./{}".format(server_name)
            else:
                self.exp_config.logger.print("Wrong server configuration, exit!")
                exit(1)
            if s_config == "pool":
                pool_args = []
                pool_io_threads = self.exp_config.run_config.get("poolIOThreads")
                if pool_io_threads is not None:
                    pool_io_threads = int(pool_io_threads)
                    if pool_io_threads <= 0:
                        raise RuntimeError("poolIOThreads must be positive")
                    self.exp_config.logger.print(
                        "Passing pool I/O threads={} to pool server".format(pool_io_threads))
                    pool_args.append(str(pool_io_threads))
                pool_worker_threads = self.exp_config.run_config.get("poolWorkerThreads")
                if pool_worker_threads is not None:
                    if pool_io_threads is None:
                        raise RuntimeError("poolWorkerThreads requires poolIOThreads")
                    pool_worker_threads = int(pool_worker_threads)
                    if pool_worker_threads <= 0:
                        raise RuntimeError("poolWorkerThreads must be positive")
                    self.exp_config.logger.print(
                        "Passing pool worker threads={} to pool server".format(pool_worker_threads))
                    pool_args.append(str(pool_worker_threads))
                if pool_args:
                    cmd = "{} {}".format(
                        cmd, " ".join(shlex.quote(arg) for arg in pool_args))
            if s_config == "grpc-koma-go" or s_config == "silo-grpc-koma-go":
                cmd = "env GRPC_KOMA_CORES={} {}".format(
                    shlex.quote(numa_cores), cmd
                )
            cmd = 'dtach -n `mktemp -u /tmp/%s.XXXX` %s' % ('dtach', cmd)
            self.exp_config.logger.print(f"numactl cmd is {numa_cmd} {cmd}")
            server_conn.run("{} {}".format(numa_cmd, cmd), warn=True)
            if "grpc" in server_name:
                self.wait_for_remote_tcp_port(self.exp_config.listening_port)
            if "silo" in server_name:
                time.sleep(24)

    def compile_server_config(self, server_config: str):
        with self.server_conn.cd(self.dirs.server_dir):
            self.server_conn.run("git checkout config.h")
            for key,value in server_config.items():
                # the sed command should be sed -i -e "s/\(#define <key>\).*/\1 <value>/" config.h
                self.server_conn.run(r'sed -i -e "s/\\(#define {}\\).*/\1 {}/" {}'.format(key, value, "config.h"))

    def apply_server_config(self, config: str):
        """
        apply the configuration for server (e.g., partition/floating) to the server
        :param config: 
        """ 
        # config linux accordingly
        server_config: dict[str, int] = dict()
        if config == "grpc-go" or config == "silo-grpc-go":
            self._configure_server_grpc_go_mod(use_koma=False)
            self._refresh_server_go_modules()
            self._build_server_target("silo-grpc-go" if config == "silo-grpc-go" else "spin-grpc-go")
        elif config == "grpc-koma-go" or config == "silo-grpc-koma-go":
            self._configure_server_grpc_go_mod(use_koma=True)
            self._refresh_server_go_modules()
            self._build_server_target("silo-grpc-go" if config == "silo-grpc-koma-go" else "spin-grpc-go")
        if config == "partition" or config == "silo-partition":
            server_config['CONFIG_REGISTER_FD_TO_ALL_EPOLLS'] = 0 
            server_config['CONFIG_USE_EPOLLEXCLUSIVE'] = 0 
            server_config['CONFIG_MAX_EVENTS'] = 1 
            self.compile_server_config(server_config)
        elif config == "floating" or config == "silo-floating" or config == "silo-floating-tls" or config == "floating-tls":
            server_config['CONFIG_REGISTER_FD_TO_ALL_EPOLLS'] = 1 
            server_config['CONFIG_USE_EPOLLEXCLUSIVE'] = 1 
            server_config['CONFIG_MAX_EVENTS'] = 1 
            self.compile_server_config(server_config)
        elif config == "kcm_partition" or config == "silo-kcm_partition":
            server_config['CONFIG_REGISTER_FD_TO_ALL_EPOLLS'] = 0 
            server_config['CONFIG_USE_EPOLLEXCLUSIVE'] = 0 
            server_config['CONFIG_MAX_EVENTS'] = 1 
            self.compile_server_config(server_config)
        elif config == "kcm_floating" or config == "silo-kcm_floating":
            server_config['CONFIG_REGISTER_FD_TO_ALL_EPOLLS'] = 1 
            server_config['CONFIG_USE_EPOLLEXCLUSIVE'] = 1 
            server_config['CONFIG_MAX_EVENTS'] = 1 
            self.compile_server_config(server_config)
        elif config == "pool" or config == "pool-tls" or config == "silo-pool" or config == "silo-pool-tls":
            server_config['CONFIG_REGISTER_FD_TO_ALL_EPOLLS'] = 1 
            server_config['CONFIG_USE_EPOLLEXCLUSIVE'] = 1 
            server_config['CONFIG_MAX_EVENTS'] = 1 
            self.compile_server_config(server_config)
 
        # else:
            # self.exp_config.logger.print("Wrong server configuration {}, exit!".format(config))
            # exit(1)
        with self.server_conn.cd(self.dirs.server_dir):
            # self.server_conn.run("make clean")
            self.server_conn.run("make")

    def run_lancet(self):
        """
        Run lancet
        """
        lancet_config = self.exp_config.lancet_config #alias
        coord_opts:str = ""  
        coord_opts += "-targetHost {} ".format(self.get_target_host())

        print(lancet_config)
        if 'transport_proto' in lancet_config:
            coord_opts += "-comProto {} ".format(lancet_config['transport_proto'])
        else: 
            coord_opts += "-comProto {} ".format(self.exp_config.run_config['transport_proto'])

        coord_opts += "-idist {} ".format(lancet_config['idist'])  
        coord_opts += "-privateKey {} ".format(self.dirs.ssh_private_key)

        if 'symAgents' in lancet_config and lancet_config['symAgents'] == "True":
            if self.exp_config.run_config['loadConns'] % len(self.exp_config.clients) != 0:
                self.exp_config.logger.print("number of load conns can not be allocated to agents equally!")
                exit(1)
            if self.exp_config.run_config['loadThreads'] % len(self.exp_config.clients) != 0:
                self.exp_config.logger.print("number of load threads can not be allocated to agents equally!")
                exit(1)

            loadConns_per_agent = int(int(self.exp_config.run_config['loadConns']) / len(self.exp_config.clients))
            loadthread_per_agent = int(int(self.exp_config.run_config['loadThreads']) / len(self.exp_config.clients)) 
            coord_opts += "-ifName {} ".format(get_iface(self.exp_config.clients_conn[0]))

            if 'nicTS' in lancet_config and lancet_config['nicTS'] == 'True':
                coord_opts += "-nicTS "
            if 'app_proto' in lancet_config and lancet_config['app_proto'] == 'memcache-id':
                coord_opts += "-symIdAgents {} ".format(self.lancet_agents)
            else:
                coord_opts += "-symAgents {} ".format(self.lancet_agents)
            coord_opts += "-loadConns {} ".format(loadConns_per_agent)
            coord_opts += "-loadThreads {} ".format(loadthread_per_agent)
            coord_opts += "-loadPattern {} ".format(self.exp_config.run_config['loadPattern'])
            coord_opts += "-reqPerConn {} ".format(self.exp_config.run_config['reqPerConn'])
        else:
            # SW mode
            if self.exp_config.run_config['loadConns'] % len(self.exp_config.clients[1:]) != 0:
                self.exp_config.logger.print("number of load conns can not be allocated to load agents equally!")
                exit(1)
            if self.exp_config.run_config['loadThreads'] % len(self.exp_config.clients[1:]) != 0:
                self.exp_config.logger.print("number of load threads can not be allocated to load agents equally!")
                exit(1)

            loadConns_per_agent = int(int(self.exp_config.run_config['loadConns']) / len(self.exp_config.clients[1:]))
            loadthread_per_agent = int(int(self.exp_config.run_config['loadThreads']) / len(self.exp_config.clients[1:])) 

            coord_opts += "-ltAgents {} ".format(self.latency_agents)
            coord_opts += "-loadAgents {} ".format(self.load_agents)
            coord_opts += "-loadConns {} ".format(loadConns_per_agent)
            coord_opts += "-loadThreads {} ".format(loadthread_per_agent)
            coord_opts += "-loadPattern {} ".format(self.exp_config.run_config['loadPattern'])
            coord_opts += "-reqPerConn {} ".format(self.exp_config.run_config['reqPerConn'])

        app_proto_config: str = ""
        if 'app_proto' not in lancet_config:
            self.exp_config.logger.print("Lancet could not run without application protocol specified!")
            exit(1)
        elif lancet_config['app_proto'].startswith(('memcache', 'redis')):
            app_proto_config += "{}_".format(lancet_config['app_proto'])
            app_proto_config += "{}_{}_{}_{}_{} ".format(self.exp_config.run_config['key_size_gen'],
                                                            self.exp_config.run_config['value_size_gen'], 
                                                            self.exp_config.run_config['key_cnt'], 
                                                            self.exp_config.run_config['r_w_ratio'], 
                                                            self.exp_config.run_config['key_selector'])
        elif lancet_config['app_proto'].startswith('echo'):
            app_proto_config += "{}".format(lancet_config['app_proto'])
        coord_opts += "--appProto {}".format(app_proto_config)

        with self.coord_conn.cd(self.dirs.coord_dir):
            self.coord_conn.run("echo {}".format(coord_opts))
            # allow command failure to deal with Lancet's gray failure TODO: figure out why
            self.exp_config.logger.print("LANCET command is: ./coordinator {}".format(coord_opts))
            run_output = self.coord_conn.run("./coordinator {}".format(coord_opts), warn=True)
        return run_output

    def run_experiment(self, server_config, dist_config):
        self.clean_experiment()
        self.apply_server_config(server_config)
        self.run_server(server_config, dist_config)
        time.sleep(5)
        lancet_output = self.run_lancet()
        lines = lancet_output.stdout.rsplit('\n')
        self.exp_config.logger.print("[Lancet Result]: {} \n".format(server_config))
        for l in lines[4:]:
            self.exp_config.logger.print(l)
        return self.parse_lancet_results(lines)
    
    def run_perf(self):
        self.server_conn.run("sudo apt install -y python3-pip libbpf-dev scons libevent-dev gengetopt libzmq3-dev numactl dtach")

    def parse_lancet_results(self, lines):
        # parse, check and save Lancet Result
        if_load: bool = False
        if_lat: bool = False
        res_load = 0
        res_tail_lat = 0
        for l_num, l in enumerate(lines):
            if l.startswith("#ReqCount"):
                if lines[l_num + 1].startswith("Spearman"):
                    res_load = lines[l_num + 2].split('\t')[1].split('(')[0]
                else:
                    res_load = lines[l_num + 1].split('\t')[1].split('(')[0]
                if_load = True
            if lines[l_num].startswith("Aggregate latency") and lines[l_num + 1].startswith("#Avg Lat"):
                if lines[l_num + 2].startswith("Spearman"):
                    res_tail_lat = lines[l_num + 3].split('\t')[4].split('(')[0]
                else:
                    res_tail_lat = lines[l_num + 2].split('\t')[4].split('(')[0]

                if_lat = True
                break

        if not if_load or not if_lat:
            self.exp_config.logger.print("Lancet does not perform measurement properly! Plase check logger record for more detail")
            return float("nan"), float("nan")
        res_load = float(res_load)
        res_tail_lat = float(res_tail_lat)
        return res_load, res_tail_lat

    def run_schedsim(self, dist_config):
        schedsim_res_dir = os.path.join(self.dirs.schedsim_dir, "scripts/data")
        dist_config_map = {'fixed': 'd', 'exponential': 'm', 'bimodal': 'b'}
        numa_node, numa_cores = get_cores_from_numa(self.server_conn)
        n_cores = len(numa_cores.split(","))
        service_time = self.exp_config.run_config['service_time']
        csv_name_list = ["FIFO_{}_{}_{}_{}.csv".format(service_time, n_cores, dist_config_map[i], self.exp_config.run_config['loadConns']) for i in dist_config]

        # first search if the required csv already exists
        for queue_type in ["single_queue", "conn_multi_queue", "conn_single_queue"]:
            if_exists = all([os.path.exists(os.path.join(schedsim_res_dir + "/{}".format(queue_type), csv)) for csv in csv_name_list])
            if not if_exists:
                # TODO: rewritee number of workers in schedsim
                # run schedsim
                with self.coord_conn.cd(os.path.join(self.dirs.schedsim_dir, "scripts/")):
                    self.coord_conn.config.run.env = {"PATH": "$PATH:{}".format(self.dirs.schedsim_dir)}
                    self.coord_conn.run("python3 run_new.py -s {} -n {} -q {} -c {}".format(service_time, n_cores, queue_type, self.exp_config.run_config['loadConns']))

    def plot_results(self):
        plot_scripts_dir = os.path.join(self.dirs.plot_dir, self.exp_config.exp_type) 
        local_plot_dir = os.path.join(self.exp_config.res_dir, "plots/")
        if not os.path.exists(plot_scripts_dir):
            self.exp_config.logger.print("No ploting scripts found!")
            exit(1)
        if not os.path.exists(local_plot_dir):
            os.makedirs(local_plot_dir)

            # Loop through all files in the source directory
            for filename in os.listdir(plot_scripts_dir):
                # Get full file paths
                src_file = os.path.join(plot_scripts_dir, filename)
                dst_file = os.path.join(local_plot_dir, filename)

                # Copy the file if it's a file (not a directory)
                if os.path.isfile(src_file):
                    shutil.copy(src_file, dst_file)
        with self.coord_conn.cd(local_plot_dir):
            self.coord_conn.run("python3 ./plot.py")
