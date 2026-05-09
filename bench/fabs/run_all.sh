#!/usr/bin/env bash
set -euo pipefail

FABS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$FABS_DIR/../.." && pwd)"
FAB_VENV_DIR="$REPO_ROOT/.fab-venv"
FAB_BIN="$FAB_VENV_DIR/bin/fab"

# Hosts are read directly from fab_config.py
COORDINATOR=$(cd "$FABS_DIR" && python3 -c "from fab_config import COORDINATOR; print(COORDINATOR[0])")
SERVER=$(cd "$FABS_DIR" && python3 -c "from fab_config import SERVER; print(SERVER[0])")
REMOTE_USER=$(cd "$FABS_DIR" && python3 -c "from fab_config import DEFAULT_REMOTE_USER; print(DEFAULT_REMOTE_USER)")
SSH_KEY=$(cd "$FABS_DIR" && python3 -c "from fab_config import DEFAULT_SSH_PRIVATE_KEY; print(DEFAULT_SSH_PRIVATE_KEY)")

install_fabric() {
    echo "Bootstrapping local Fabric toolchain in $FAB_VENV_DIR"
    python3 -m venv "$FAB_VENV_DIR"
    "$FAB_VENV_DIR/bin/pip" install --upgrade pip setuptools wheel fabric
}

ensure_fabric() {
    if [[ -x "$FAB_BIN" ]]; then
        return 0
    fi

    if command -v fab >/dev/null 2>&1; then
        FAB_BIN="$(command -v fab)"
        return 0
    fi

    install_fabric
    if [[ ! -x "$FAB_BIN" ]]; then
        echo "Error: failed to install fabric into $FAB_VENV_DIR" >&2
        exit 1
    fi
}

expand_user_path() {
    local path="$1"
    if [[ "$path" == "~/"* ]]; then
        printf '%s/%s' "$HOME" "${path#~/}"
    else
        printf '%s' "$path"
    fi
}

host_spec() {
    local host="$1"
    if [[ "$host" == *@* || -z "$REMOTE_USER" ]]; then
        printf '%s' "$host"
    else
        printf '%s@%s' "$REMOTE_USER" "$host"
    fi
}

SSH_KEY_PATH=$(expand_user_path "$SSH_KEY")

agent_has_key() {
    local key_path="$1"
    local pub_path="${key_path}.pub"

    [[ -f "$pub_path" ]] || return 1
    ssh-add -L 2>/dev/null | grep -Fqx "$(tr -d '\r' < "$pub_path")"
}

ensure_ssh_agent_key() {
    local ssh_add_status

    if [[ -z "$SSH_KEY_PATH" || ! -f "$SSH_KEY_PATH" ]]; then
        return 0
    fi

    if ! command -v ssh-agent >/dev/null 2>&1 || ! command -v ssh-add >/dev/null 2>&1; then
        return 0
    fi

    ssh-add -l >/dev/null 2>&1
    ssh_add_status=$?
    if [[ -z "${SSH_AUTH_SOCK:-}" || $ssh_add_status -eq 2 ]]; then
        eval "$(ssh-agent -s)" >/dev/null
    fi

    if ! agent_has_key "$SSH_KEY_PATH"; then
        echo "Adding SSH identity $SSH_KEY_PATH to ssh-agent"
        ssh-add "$SSH_KEY_PATH" >/dev/null
    fi
}

ALL_EXPERIMENTS=(
    gRPC20-Conn24
    gRPC20-Conn5000
    Silo-GRPC-Conn24
    Silo-GRPC-Conn5000
    vanilla100-Conn20
    vanilla100-Conn80
    vanilla100-Conn5000
    vanilla20-Conn80
    TLS100-Conn80
    TLS20-Conn80
    silo-vanilla-80
    silo-vanilla-tls-80
)

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --setup                     Full machine setup: install deps, build lancet, deploy agents
                              (run once per fresh cluster allocation)
  --patch-kernel              Patch and install the Koma kernel on the server machine
                              (applies both koma.patch and ktls-module.patch)
  --verify-ktls               Verify the patched tls.ko is loaded (run after rebooting)
  --update                    Pull latest repo code and redeploy lancet agents
  --experiments EXP[,EXP...]  Comma-separated list of experiments to run, or 'all'
  --list                      List all available experiment names and exit
  -h, --help                  Show this help and exit

Notes:
  - --patch-kernel and --verify-ktls run against the SERVER from fab_config.py.
  - All other tasks (--setup, --update, experiments) run against the COORDINATOR
    from fab_config.py.

Examples:
  # One-time full setup
  $(basename "$0") --setup

  # Patch kernel on the server, then set up the rest
  $(basename "$0") --patch-kernel --setup

  # Run all experiments
  $(basename "$0") --experiments all

  # Run a specific subset
  $(basename "$0") --experiments gRPC20-Conn24,vanilla100-Conn80,TLS20-Conn80
EOF
}

fab_run() {
    local host="$1"
    local task="$2"
    local fabric_host
    ensure_fabric
    ensure_ssh_agent_key
    fabric_host=$(host_spec "$host")
    echo ""
    echo "======================================================"
    echo "  fab task : $task"
    echo "  host     : $fabric_host"
    echo "======================================================"
    "$FAB_BIN" -H "$fabric_host" "$task"
}

is_valid_experiment() {
    local name="$1"
    for e in "${ALL_EXPERIMENTS[@]}"; do
        [[ "$e" == "$name" ]] && return 0
    done
    return 1
}

do_setup=false
do_patch_kernel=false
do_verify_ktls=false
do_update=false
experiments=()

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --setup)
            do_setup=true
            shift
            ;;
        --patch-kernel)
            do_patch_kernel=true
            shift
            ;;
        --verify-ktls)
            do_verify_ktls=true
            shift
            ;;
        --update)
            do_update=true
            shift
            ;;
        --experiments)
            IFS=',' read -ra experiments <<< "$2"
            shift 2
            ;;
        --list)
            echo "Available experiments:"
            printf '  %s\n' "${ALL_EXPERIMENTS[@]}"
            exit 0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: unknown option '$1'" >&2
            usage
            exit 1
            ;;
    esac
done

cd "$FABS_DIR"

# --- server-side setup tasks ---
if $do_patch_kernel; then
    fab_run "$SERVER" cloudlab-patch-koma-kernel
fi

if $do_verify_ktls; then
    fab_run "$SERVER" cloudlab-verify-ktls-module
fi

# --- coordinator-side setup tasks ---
if $do_setup; then
    fab_run "$COORDINATOR" bench-setup
fi

if $do_update; then
    fab_run "$COORDINATOR" bench-update
fi

# --- experiments ---
if [[ ${#experiments[@]} -gt 0 ]]; then
    if [[ "${experiments[0]}" == "all" ]]; then
        experiments=("${ALL_EXPERIMENTS[@]}")
    fi

    for exp in "${experiments[@]}"; do
        if ! is_valid_experiment "$exp"; then
            echo "Error: unknown experiment '$exp'" >&2
            echo "Run '$(basename "$0") --list' to see available experiments." >&2
            exit 1
        fi
    done

    for exp in "${experiments[@]}"; do
        fab_run "$COORDINATOR" "$exp"
    done
fi
