#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_dir=$(cd -- "$script_dir/.." && pwd)

usage() {
    cat <<'EOF'
Usage: ./scripts/start_demo.sh [options]
  --preset debug|release  Build configuration (default: debug)
  --session NAME          tmux session name (default: raft-demo)
  --nodes K               Number of server nodes (default: 3)
  --base-port PORT        First of K consecutive server ports (default: 5555)
  --no-build              Use existing executables
  -h, --help              Show help

Runs K servers and one business client in tmux in the current terminal.
Inside tmux, switches the current client to the new demo session.
Node IDs are 1..K; every server is configured with all other servers as peers.
tmux tiles the K+1 panes; one client process connects to all K servers.
Each pane keeps its output and opens a shell when the example exits.
EOF
}

fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }

# Internal pane entry point. Arguments are passed as separate shell-quoted words.
if [[ ${1:-} == --pane ]]; then
    shift
    cd -- "$project_dir"
    printf 'Running:'
    printf ' %q' "$@"
    printf '\n\n'
    status=0
    "$@" || status=$?
    printf '\nProcess exited with status %s. Output preserved; type exit to close this pane.\n' "$status"
    exec bash --noprofile --norc -i
fi

preset=debug
session=raft-demo
base_port=5555
nodes=3
build=1
while (($#)); do
    case "$1" in
        --preset|--session|--base-port|--nodes)
            (($# >= 2)) || fail "Missing value for $1"
            case "$1" in
                --preset) preset=$2 ;;
                --session) session=$2 ;;
                --base-port) base_port=$2 ;;
                --nodes) nodes=$2 ;;
            esac
            shift 2 ;;
        --no-build) build=0; shift ;;
        -h|--help) usage; exit 0 ;;
        *) fail "Unknown option: $1" ;;
    esac
done
[[ $preset == debug || $preset == release ]] || fail "Preset must be debug or release"
[[ $session =~ ^[a-zA-Z0-9_-]+$ ]] || fail "Session name must contain only letters, numbers, _ or -"
[[ $base_port =~ ^[0-9]{1,5}$ ]] || fail "Invalid base port"
[[ $nodes =~ ^[0-9]{1,5}$ ]] || fail "Node count must be a positive integer"
base_port=$((10#$base_port))
nodes=$((10#$nodes))
((nodes >= 1)) || fail "Node count must be at least 1"
((base_port >= 1024 && base_port + nodes - 1 <= 65535)) || fail "Server port range must fit within 1024..65535"

[[ -t 0 && -t 1 ]] || fail "Run this script from an interactive terminal"
for dependency in tmux; do
    command -v "$dependency" >/dev/null || fail "Missing $dependency. Install with: sudo apt install tmux"
done
if tmux has-session -t "=$session" 2>/dev/null; then
    fail "Session '$session' already exists. Attach with: tmux attach-session -t '$session'"
fi

cd -- "$project_dir"
if ((build)); then
    command -v cmake >/dev/null || fail "Missing cmake"
    cmake --preset "$preset"
    cmake --build --preset "$preset" --target zmq_server zmq_client --parallel
fi
binary_dir="$project_dir/build/$preset"
[[ -x $binary_dir/zmq_server && -x $binary_dir/zmq_client ]] || fail "Executables missing; run without --no-build"

# A fresh window is created; no existing tmux windows or sessions are modified.
created=0
cleanup() {
    if ((created)); then tmux kill-session -t "=$session" 2>/dev/null || true; fi
}
trap cleanup EXIT
pane_command() { printf '%q ' env "SPDLOG_LEVEL=${SPDLOG_LEVEL:-info}" bash "$script_dir/start_demo.sh" --pane "$@"; }
endpoints=()
for ((i = 0; i < nodes; ++i)); do
    endpoints+=("tcp://127.0.0.1:$((base_port + i))")
done

# Leave enough virtual space for tiled panes before attaching the current terminal.
columns=1
while ((columns * columns < nodes + 1)); do columns=$((columns + 1)); done
rows=$(((nodes + 1 + columns - 1) / columns))
width=$((columns * 40))
height=$((rows * 10))
((width >= 160)) || width=160
((height >= 44)) || height=44

for ((i = 0; i < nodes; ++i)); do
    server_args=("$binary_dir/zmq_server" "${endpoints[i]}" --node-id "$((i + 1))")
    for ((j = 0; j < nodes; ++j)); do
        if ((i != j)); then server_args+=(--peer "$((j + 1))=${endpoints[j]}"); fi
    done
    command=$(pane_command "${server_args[@]}")
    if ((i == 0)); then
        pane=$(tmux new-session -d -x "$width" -y "$height" -P -F '#{pane_id}' \
            -s "$session" -n demo -c "$project_dir" "$command")
        created=1
        first_pane=$pane
        window=$(tmux display-message -p -t "$first_pane" '#{window_id}')
    else
        pane=$(tmux split-window -d -h -P -F '#{pane_id}' -t "$first_pane" -c "$project_dir" "$command")
        tmux select-layout -t "$window" tiled >/dev/null
    fi
    tmux select-pane -t "$pane" -T "server-$((i + 1)) :$((base_port + i))"
done
client_pane=$(tmux split-window -d -h -P -F '#{pane_id}' -t "$first_pane" -c "$project_dir" \
    "$(pane_command "$binary_dir/zmq_client" --all "${endpoints[@]}")")
tmux select-layout -t "$window" tiled >/dev/null
tmux set-window-option -t "$window" pane-border-status top
tmux set-window-option -t "$window" pane-border-format ' #{pane_title} '
tmux select-pane -t "$client_pane" -T client
tmux select-pane -t "$client_pane"
# Keep the session alive when the terminal detaches or the client disconnects.
created=0
printf "Started session '%s' with %s server nodes. Detach with Ctrl+b d; stop with: tmux kill-session -t '%s'\n" "$session" "$nodes" "$session"
if [[ -n ${TMUX:-} ]]; then
    tmux switch-client -t "=$session"
else
    tmux attach-session -t "=$session"
fi
