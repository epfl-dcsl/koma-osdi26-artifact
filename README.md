# Koma — OSDI Artifact

This repository is the artifact for the OSDI'26 paper "Koma: Achieving Low Tail Latency with In-Kernel Message-Oriented Scheduling". It contains the
Koma kernel-module sources (three variants: plain TCP, gRPC, kTLS), a
benchmark harness driven by Fabric, and the third-party tools needed to
reproduce the paper's performance figures on a CloudLab allocation.

## Quickstart

End-to-end smoke test on a CloudLab allocation: patch the kernel, build
everything, run the smallest TCP experiment, and render its figure. All
commands run on the **driver** machine (your laptop or a CloudLab
jumpbox); the harness ssh's into the cluster from there. See the
[Hardware](#hardware) section for the host roles this assumes.

```bash
# 1. Clone and create the driver venv.
git clone <this-artifact-url> ~/koma-osdi26-artifact
cd ~/koma-osdi26-artifact
python3 -m venv .fab-venv
.fab-venv/bin/pip install -r bench/requirements.txt

# 2. Point the harness at your allocation. See "Configure your
#    allocation" for every variable; these six are the minimum.
export KOMA_OSDI_REMOTE_USER=<cloudlab-user>
export KOMA_OSDI_SSH_KEY=$HOME/.ssh/id_rsa_cloudlab
export KOMA_OSDI_SERVER=<server-fqdn>
export KOMA_OSDI_COORDINATOR=<coord-fqdn>
export KOMA_OSDI_SYM_CLIENTS=<c1>,<c2>,<c3>,<c4>,<c5>
export KOMA_OSDI_ASYM_CLIENTS=<c1>,<c2>,<c3>,<c4>,<c5>

# 3. Patch and install the 6.8.0-koma kernel; the server reboots.
bench/fabs/run_all.sh --patch-kernel

# 4. Build koma modules + benchmark servers, deploy Lancet to clients.
bench/fabs/run_all.sh --setup

# 5. Run the smallest TCP experiment (~Figure 8 input).
bench/fabs/run_all.sh --experiments vanilla20-Conn80

# 6. Render the matching figure.
python3 bench/plot/render.py synthetic_20us
```

Once that loop succeeds, swap step 5 for the experiments listed in
[What this artifact reproduces](#what-this-artifact-reproduces) and step 6
for the corresponding plotter name (`synthetic_100us`, `synthetic_grpc`,
`ktls_eval`, `silo`), or run `--experiments all` to drive the whole
matrix. The rest of this document is the deep-dive reference for each
step.

## What this artifact reproduces

| Paper output          | Driver task(s)                                                                     | Result directory(ies) under `results/`                                                                  |
|-----------------------|------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `Figure 7` | `vanilla100-Conn20`, `vanilla100-Conn80`, `vanilla100-Conn5000`                    | `Vanilla100_Conn20/`, `Vanilla100_Conn80/`, `Vanilla100_Conn5000/`                                      |
| `Figure 8`  | `vanilla20-Conn80`                                                                 | `Vanilla20_Conn80/`                                                                                     |
| `Figure 9`  | `gRPC20-Conn24`, `gRPC20-Conn5000`                                                 | `GRPC20_Conn24/`, `GRPC20_Conn5000/`                                                                    |
| `Figure 10`       | `TLS100-Conn80`, `TLS20-Conn80` (+ `vanilla100-Conn80`, `vanilla20-Conn80` baselines) | `TLS100_Conn80/`, `TLS20_Conn80/` (+ `Vanilla100_Conn80/`, `Vanilla20_Conn80/`)                      |
| `Figure 11`            | `silo-vanilla-80`, `silo-vanilla-tls-80`, `Silo-GRPC-Conn24`, `Silo-GRPC-Conn5000` | `Silo_Conn80/`, `Silo_Conn80_TLS/`, `Silo_GRPC_Conn24/`, `Silo_GRPC_Conn5000/`                          |

Each result directory holds one timestamped subdirectory per run with
`raw_<ExperimentType>_<ts>.csv`, `sanitized_<ExperimentType>_<ts>.csv`, and
`<ExperimentType>_<ts>.log`. The plotter consumes the newest `sanitized_*.csv`

## Hardware

The paper figures were produced on Utah CloudLab `xl170` nodes. The harness expects:

- **1 server** — patched 6.8.0-koma kernel; runs the spin/silo server
  binary and (per experiment) the koma kernel module.
- **1 coordinator** — runs the Lancet coordinator; can be the same host
  as one of the client agents. Defaults to the latency agent.
- **N clients** — Lancet load/latency agents. The TCP figures use
  ~5 hosts; gRPC figures benefit from more (see
  `KOMA_OSDI_ASYM_CLIENTS`).

**OS requirement: Ubuntu 24.04 LTS** on every host (server,
coordinator, agents). The kernel-build, bcc, and silo apt-package
names baked into the harness (e.g., `libllvm18`, `llvm-18-dev`,
`libclang-18-dev`, `libpolly-18-dev`, `libjemalloc-dev`,
`libdb++-dev`) are 24.04-specific and will fail on other releases.
The Utah CloudLab default `UBUNTU24-64-STD` image satisfies this
requirement.

The harness ssh's to all hosts using the same `KOMA_OSDI_REMOTE_USER`
and `KOMA_OSDI_SSH_KEY`. Public-key authentication must already be
working from the driver to every host. During Lancet deployment, the
coordinator also needs SSH access to the agents, either via forwarded
agent credentials or a key installed locally on the coordinator.

## One-time driver setup

Run these on the machine that will drive Fabric (your laptop or a
CloudLab jumpbox). Fabric ssh's from here into the cluster.

```bash
git clone <this-artifact-url> ~/koma-osdi26-artifact
cd ~/koma-osdi26-artifact
python3 -m venv .fab-venv
.fab-venv/bin/pip install -r bench/requirements.txt
```

`bench/fabs/run_all.sh` prefers `./.fab-venv/bin/fab` when present,
falls back to `fab` from `PATH`, and will bootstrap `./.fab-venv`
automatically only if neither exists. Keeping the repo-local venv is the
most predictable option.

## Configure your allocation

Set these environment variables to point the harness at your hosts.
Defaults in [`bench/fabs/fab_config.py`](bench/fabs/fab_config.py)
target our specific Utah allocation and **will not work** for others.

```bash
export KOMA_OSDI_REMOTE_USER=<cloudlab-user>
export KOMA_OSDI_SSH_KEY=$HOME/.ssh/id_rsa_cloudlab     # private key on the driver; forwarded or copied to coord as needed
export KOMA_OSDI_SERVER=<server-fqdn>
export KOMA_OSDI_COORDINATOR=<coord-fqdn>
export KOMA_OSDI_SYM_CLIENTS=<c1>,<c2>,<c3>,<c4>,<c5>   # TCP / TLS / Silo native
export KOMA_OSDI_ASYM_CLIENTS=<c1>,<c2>,...             # gRPC; first host is the latency agent
```

`bench/fabs/run_all.sh` uses `ssh-agent` for Fabric auth and will try to
add `KOMA_OSDI_SSH_KEY` automatically on first use if it is not already
loaded. If your private key is passphrase-protected, expect a prompt the
first time the script needs to talk to the cluster. For the Lancet
deployment steps, either keep agent forwarding enabled
(`KOMA_OSDI_FORWARD_AGENT=true`, the current default) or install the key
on the coordinator explicitly.

## Server-side bring-up

The harness now handles the server checkout automatically. No manual
clone step is required before `--patch-kernel` or `--setup`.

### 1. Patch and install the Koma kernel

From the driver:

```bash
bench/fabs/run_all.sh --patch-kernel
```

This task:

- Installs the kernel build dependencies on the server.
- Clones or fast-forwards the artifact repo on the server.
- Downloads `linux-6.8.tar.gz` if needed.
- Applies both
[`koma/patches/koma.patch`](koma/patches/koma.patch) (`AF_KOMA` +
hooks) and [`koma/patches/ktls-module.patch`](koma/patches/ktls-module.patch)
(in-kernel TLS hooks needed by the kTLS variant).
- Builds and installs the patched kernel plus its modules.
- Updates GRUB, reboots the server automatically, and waits for SSH to
  come back.

After the task returns, verify the rebooted kernel:

```bash
ssh $KOMA_OSDI_REMOTE_USER@$KOMA_OSDI_SERVER 'uname -r'   # must print 6.8.0-koma
```

For runs that exercise kTLS (`TLS100-Conn80`, `TLS20-Conn80`,
`silo-vanilla-tls-80`), confirm the patched `tls.ko` is loaded:

```bash
bench/fabs/run_all.sh --verify-ktls
```

The out-of-tree Koma modules (`koma/plain-tcp`, `koma/grpc`,
`koma/tls`) and the user-space benchmark servers are built later by
`bench/fabs/run_all.sh --setup`, after the server is already running
`6.8.0-koma`.

### 2. (Silo experiments only) Optional verification / troubleshooting

For the normal artifact flow, you do not need any extra manual Silo
setup. Fabric already takes care of the Silo-specific pieces:

- `bench/fabs/run_all.sh --setup` builds the Silo binaries as part of
  `bench/servers/`.
- Each Silo experiment configures the required hugepages automatically
  before launch.

The commands below are only for verification or troubleshooting if a
Silo experiment fails and you want to inspect the server state manually.

The Silo storage engine in `bench/servers/silo/` is built automatically
as part of `make` in `bench/servers/`. Its build dependencies
(`libjemalloc-dev`, `libdb++-dev`, `libaio-dev`, `libnuma-dev`) are
installed by `--patch-kernel`. The CXXFLAGS suppression that Silo
needs against modern toolchains is already wired into
`bench/servers/silo/Makefile`.

If you want to verify the Silo build in isolation (or rebuild after a
toolchain change) before running experiments, on the server:

```bash
cd ~/koma-osdi26-artifact/bench/servers/silo
MODE=perf make -j$(nproc)               # production objects (out-perf.masstree/)
MODE=perf make -j$(nproc) dbtest        # standalone smoke-test binary
```

The harness's `bench/servers/Makefile` reads from
`silo/out-perf.masstree/`, so `MODE=perf` matches what `make` in
`bench/servers/` will look for.

The Fabric harness automatically configures `16384` 2 MiB hugepages on
NUMA node 0 just before each Silo experiment starts. If you want to
verify the setting manually on the server, use:

```bash
ssh $KOMA_OSDI_REMOTE_USER@$KOMA_OSDI_SERVER \
    'sudo tee /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages <<<16384'
```

This setting is not persisted across reboot, which is why the harness
re-applies it whenever a Silo experiment is launched.

## Coordinator + client setup

From the driver, after `uname -r` on the server reports `6.8.0-koma`:

```bash
bench/fabs/run_all.sh --setup
```

This:

- Installs the extra server-side build/runtime packages used by
  `bench/servers/`, Silo, and BCC.
- Clones or fast-forwards the artifact repo on both the server and the
  coordinator.
- Installs Go on the server, coordinator, and clients
  (default `1.25.0`; override with `KOMA_OSDI_GO_VERSION`).
- Builds all three Koma module variants on the server against the
  currently running kernel.
- Builds the user-space benchmark servers on the server.
- Installs Lancet dependencies on every client and on the coordinator.
- Builds the Lancet coordinator, agents, and manager on the coordinator,
  then deploys the agent binaries to every client over SSH.

Use `--update` for a lighter refresh:

```bash
bench/fabs/run_all.sh --update
```

`--update` fast-forwards the server repo, refreshes the coordinator
checkout during deploy, and redeploys Lancet without rerunning the apt
install steps. If you changed server-side Koma modules, kernel state, or
toolchain-sensitive binaries, prefer `--setup`.

## Running experiments

```bash
bench/fabs/run_all.sh --list                              # show all experiment names
bench/fabs/run_all.sh --experiments vanilla20-Conn80       # one experiment
bench/fabs/run_all.sh --experiments TLS100-Conn80,TLS20-Conn80
bench/fabs/run_all.sh --experiments all                    # everything in the matrix
```

Each experiment writes results into
`results/<ExperimentType>/<YYYYMMDD-HHMMSS>/` under the artifact checkout
used by the Fabric run:

- `<exp>.log` — full Lancet command lines + intermediate output.
- `raw_<exp>.csv` — per-rep + per-load measurement points.
- `sanitized_<exp>.csv` — averaged points (one row per load level).

## Plotting figures

The artifact includes a paper-style TikZ/LaTeX plotting flow under
[`bench/plot/`](bench/plot/). It reads the newest timestamped sanitized CSV
for each figure from `results/<ExperimentType>/` and still renders the figure
layout when some inputs are missing by substituting placeholder CSVs.

Useful commands:

```bash
python3 bench/plot/render.py --list
python3 bench/plot/render.py --check
python3 bench/plot/render.py --prepare-only
python3 bench/plot/render.py                       # render all figures
python3 bench/plot/render.py synthetic_100us silo  # render a subset
```

Rendered PDFs are written to `bench/plot/out/`. Temporary build files live
under `bench/plot/out/build/`.

For PDF compilation, install a system LaTeX toolchain plus `pdfcrop`:

```bash
sudo apt install latexmk texlive-latex-extra texlive-pictures texlive-science texlive-extra-utils ghostscript
```

`texlive-extra-utils` provides `pdfcrop`, which trims the standalone figure
PDFs to the actual plot content instead of leaving a large blank page.
Additional input-mapping details live in
[`bench/plot/README.md`](bench/plot/README.md).

## Repository layout

| Path                       | Contents                                                                  |
|----------------------------|---------------------------------------------------------------------------|
| `koma/plain-tcp/`          | Koma kernel module sources for the plain-TCP path.                        |
| `koma/grpc/`               | Koma kernel module sources for the gRPC path.                             |
| `koma/tls/`                | Koma kernel module sources for the kTLS path.                             |
| `koma/patches/`            | Kernel patch set applied by `--patch-kernel`.                             |
| `bench/servers/`           | spin/silo user-space server binaries (built per experiment).              |
| `bench/exp_config/`        | YAML run parameters for each experiment.                                  |
| `bench/fabs/`              | Fabric tasks plus the `run_all.sh` orchestration entry point.             |
| `third_party/`             | `lancet-cplusplus`, `schedsim`, `FlameGraph` (pinned).                    |

## Environment variable reference

Cluster (consumed by `fab_config.py`):

| Variable                      | Default                              | Purpose                                                  |
|-------------------------------|--------------------------------------|----------------------------------------------------------|
| `KOMA_OSDI_REMOTE_USER`       | `raina96`                            | ssh user on every host                                   |
| `KOMA_OSDI_SSH_KEY`           | `~/.ssh/id_rsa`                      | private key path used by Fabric and Lancet deployment    |
| `KOMA_OSDI_FORWARD_AGENT`     | `true`                               | forward the driver's ssh-agent into Fabric sessions      |
| `KOMA_OSDI_SERVER`            | `hp161.utah.cloudlab.us`             | server hostname                                          |
| `KOMA_OSDI_COORDINATOR`       | `hp171.utah.cloudlab.us`             | coordinator hostname                                     |
| `KOMA_OSDI_SYM_CLIENTS`       | 12 Utah `hp` nodes from `fab_config.py` | TCP/TLS/Silo client agents                            |
| `KOMA_OSDI_ASYM_CLIENTS`      | 12 Utah `hp` nodes from `fab_config.py` | gRPC client agents; first host is the latency agent   |
| `KOMA_OSDI_TLS_SERVER`        | = `KOMA_OSDI_SERVER`                 | server for TLS experiments (in case it differs)          |
| `KOMA_OSDI_HOME_DIR`          | remote `$HOME`                       | override remote home directory                           |
| `KOMA_OSDI_PROJECT_DIR`       | `$HOME/koma-osdi26-artifact`         | repo path on remote hosts                                |
| `KOMA_OSDI_PROJECT_REPO`      | `git@github.com:epfl-dcsl/koma-osdi26-artifact.git` | repo cloned/updated by Fabric             |
| `KOMA_OSDI_GO_VERSION`        | `1.25.0`                             | Go version installed by `--setup`                        |
| `KOMA_OSDI_IFACE`             | auto-detected                        | override NIC/interface detection in `fab_utils.get_iface` |
| `KOMA_OSDI_IP_ADDR`           | auto-detected                        | override NIC IP detection in `fab_utils.get_ip_addr`     |
| `KOMA_OSDI_KOMA_PLAIN_DIR`    | `$KOMA_OSDI_PROJECT_DIR/koma/plain-tcp` | plain-TCP Koma module source dir                     |
| `KOMA_OSDI_KOMA_GRPC_DIR`     | `$KOMA_OSDI_PROJECT_DIR/koma/grpc`   | gRPC Koma module source dir                              |
| `KOMA_OSDI_KOMA_TLS_DIR`      | `$KOMA_OSDI_PROJECT_DIR/koma/tls`    | kTLS Koma module source dir                              |

Module pins (rarely overridden):

| Variable                              | Default                                                     |
|---------------------------------------|-------------------------------------------------------------|
| `KOMA_OSDI_GRPC_GO_MODULE_URL`        | `github.com/rainayangg/grpc-go`                             |
| `KOMA_OSDI_GRPC_GO_MODULE_VERSION`    | `v0.0.0-20260325234553-a764f0be0b0d`                        |
| `KOMA_OSDI_NET_GO_MODULE_URL`         | `github.com/rainayangg/net-go`                              |
| `KOMA_OSDI_NET_GO_MODULE_VERSION`     | `v0.0.0-20260313180441-b5081d74e7cf`                        |
| `KOMA_OSDI_KERNEL_TREE`               | `$HOME/linux-6.8`                                           |
| `KOMA_OSDI_KERNEL_TARBALL`            | `$HOME/linux-6.8.tar.gz`                                    |
| `KOMA_OSDI_KERNEL_TARBALL_URL`        | `https://mirrors.edge.kernel.org/.../linux-6.8.tar.gz`      |
| `KOMA_OSDI_KTLS_KERNEL_RELEASE`       | `6.8.0-koma`                                                |

## Troubleshooting

- **`uname -r` does not print `6.8.0-koma`** — server didn't boot the
  patched kernel. Confirm GRUB_DEFAULT, then `sudo reboot`.
- **kTLS verification fails after `--verify-ktls`** — the loaded
  `tls.ko` doesn't match the one installed under
  `/lib/modules/6.8.0-koma/kernel/net/tls/`. Most often this means the
  server is not booted into `6.8.0-koma` (run `uname -r`) or the
  `--patch-kernel` step didn't include `ktls-module.patch` (re-run
  `--patch-kernel` and reboot).
- **Lancet hangs or does not reach published QPS** — gRPC at
  Conn5000 may be load-generator bound. Add more hosts to
  `KOMA_OSDI_ASYM_CLIENTS`.
- **Silo experiment refuses to start / segfaults early** —
  the automatic hugepage setup likely failed on the server (for example
  due to insufficient free memory or a sudo/write failure under `/sys`).
  Check `nr_hugepages` with the command in §"Server-side bring-up —
  step 2" and retry the experiment.
- **Permission denied (publickey) during deploy** — the coordinator
  ssh's to agents during `make deploy`. With the default
  `KOMA_OSDI_FORWARD_AGENT=true`, make sure the driver's `ssh-agent`
  has `KOMA_OSDI_SSH_KEY` loaded and that the matching public key is
  accepted by every agent. If you disable agent forwarding, install the
  key on the coordinator explicitly.
- **Server stalls, freezes, or exits with errors** —
  `NUM_KOMA_SOCKETS` in `koma/{plain-tcp,grpc,tls}/koma.h`
  must match the number of cores the server binary runs on. Update the
  `#define` in each variant you use, then rebuild the modules
  (`bench/fabs/run_all.sh --setup` or `make` in the variant directory)
  and reload the module on the server.
