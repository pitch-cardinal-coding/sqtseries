#!/usr/bin/env bash
# mem-guard.sh — run a command under a HARD memory governor.
#
#   scripts/mem-guard.sh [--budget-mb N] [--floor-mb N] [--disk-floor-gb N]
#                        [--poll-s N] [--name TAG] -- CMD [ARGS...]
#
# Why this exists (2026-09-10 incident): a stress verification stacked a
# memray-tracked service (memray adds tracking overhead) on a 6.4M-row DB
# under 10k pts/s load with no ceiling and no watchdog. RSS ran away to
# ~900 MB service-side while the desktop held ~12 GB, the machine thrashed
# into OOM, and the desktop froze. NEVER AGAIN.
#
# Three rails, all enforced:
#   1. BUDGET    — the guarded process tree's total RSS must stay under
#                  --budget-mb. Breach => SIGKILL the whole tree, exit 42.
#   2. SYSTEM    — host MemAvailable must stay above --floor-mb. This
#                  protects the DESKTOP (browser, VM, apps) from our runs.
#                  Breach => SIGKILL the whole tree, exit 42.
#   3. TMPDIR    — if /tmp (or $TMPDIR) is tmpfs, it is RAM: refuse to start
#                  unless its free space >= --disk-floor-gb, and kill if it
#                  drops below during the run. A single big capture file can
#                  otherwise silently consume gigabytes of memory.
#
# Preflight refuses to start when the host is already tight (MemAvailable
# below budget + safety) — a run that *starts* by eating the last gigabytes
# is a freeze waiting to happen.
#
# Exit codes: 0 = clean, 42 = governor breach (budget/system/tmpfs),
# 124-ish passthrough from timeout(1), other = the command's own exit code.
#
# Defaults are sized for this 22 GiB development machine with a heavy
# desktop: budget 1500 MB, floor 4000 MB, tmpfs floor 2 GB.
set -u

BUDGET_MB=1500
FLOOR_MB=4000
DISK_FLOOR_GB=2
POLL_S=2
NAME="guarded"

while [ $# -gt 0 ]; do
    case "$1" in
        --budget-mb) BUDGET_MB="$2"; shift 2 ;;
        --floor-mb) FLOOR_MB="$2"; shift 2 ;;
        --disk-floor-gb) DISK_FLOOR_GB="$2"; shift 2 ;;
        --poll-s) POLL_S="$2"; shift 2 ;;
        --name) NAME="$2"; shift 2 ;;
        --) shift; break ;;
        *) echo "mem-guard: unknown arg: $1" >&2; exit 2 ;;
    esac
done
[ $# -gt 0 ] || { echo "mem-guard: no command given" >&2; exit 2; }

log() { echo "[mem-guard:$NAME] $*"; }

# ---- helpers ---------------------------------------------------------------
field_from_status() { # $1=pid  $2=field (VmRSS)
    awk -v f="$2" '$1 == f":" {print $2; exit}' "/proc/$1/status" 2>/dev/null
}

mem_available_mb() {
    awk '/^MemAvailable:/ {print int($2/1024)}' /proc/meminfo 2>/dev/null
}

tmpfs_free_gb() { # $1=mountpoint; empty if not tmpfs
    local fs fr
    fs=$(findmnt -n -o FSTYPE --target "$1" 2>/dev/null)
    [ "$fs" = "tmpfs" ] || return 0
    fr=$(df -BG --output=avail "$1" 2>/dev/null | tail -1 | tr -dc '0-9')
    echo "${fr:-0}"
}

# total RSS of the process tree rooted at $1 (kB), via /proc walk
tree_rss_kb() {
    local total=0 cur
    for cur in $(tree_pids "$1"); do
        total=$((total + $(field_from_status "$cur" VmRSS || echo 0)))
    done
    echo "$total"
}

# All live pids descended from $1 (including $1), one per line.
# Worklist walk: a bash `for` over a growing string iterates a SNAPSHOT, so
# descendants discovered mid-loop must be appended to an ARRAY worklist.
tree_pids() {
    local -a queue=("$1")
    local -A seen=()
    local out="" cur p
    while [ "${#queue[@]}" -gt 0 ]; do
        cur="${queue[0]}"
        queue=("${queue[@]:1}")
        [ -n "$cur" ] && [ -d "/proc/$cur" ] || continue
        [ -z "${seen[$cur]:-}" ] || continue
        seen[$cur]=1
        out="$out $cur"
        for p in $(ps -o pid= --ppid "$cur" 2>/dev/null | tr -d ' '); do
            queue+=("$p")
        done
    done
    echo "$out"
}

kill_tree() {
    local root="$1"
    local pids
    pids=$(tree_pids "$root")
    pids=$(echo "$pids" | xargs)  # trim
    [ -n "$pids" ] || return 0
    log "KILLING process tree:$pids"
    for p in $pids; do kill -9 "$p" 2>/dev/null; done
}

# ---- preflight -------------------------------------------------------------
avail=$(mem_available_mb)
if [ -z "$avail" ]; then
    log "FATAL: cannot read /proc/meminfo"; exit 3
fi
NEED=$((BUDGET_MB + 500))
if [ "$avail" -lt "$NEED" ]; then
    log "REFUSING: host MemAvailable=${avail}MB < budget(${BUDGET_MB}MB)+safety(500MB)."
    log "Free memory first (close apps) or lower --budget-mb. Not risking a freeze."
    exit 3
fi

TMPDIR_EXPANDED="${TMPDIR:-/tmp}"
tfree=$(tmpfs_free_gb "$TMPDIR_EXPANDED")
if [ -n "$tfree" ]; then
    if [ "$tfree" -lt "$DISK_FLOOR_GB" ]; then
        log "REFUSING: $TMPDIR_EXPANDED is tmpfs (RAM!) with ${tfree}GB free < floor ${DISK_FLOOR_GB}GB."
        log "Clear space under $TMPDIR_EXPANDED first (rm big /tmp artifacts) — it is memory."
        exit 3
    fi
    log "tmpfs $TMPDIR_EXPANDED: ${tfree}GB free (floor ${DISK_FLOOR_GB}GB)"
fi
log "start: cmd='$*' budget=${BUDGET_MB}MB floor=${FLOOR_MB}MB host_avail=${avail}MB poll=${POLL_S}s"

# ---- run + supervise -------------------------------------------------------
"$@" &
CMD_PID=$!
cleanup() {
    kill_tree "$CMD_PID" 2>/dev/null
    wait "$CMD_PID" 2>/dev/null
}
trap cleanup EXIT INT TERM

BREACH=0
while kill -0 "$CMD_PID" 2>/dev/null; do
    sleep "$POLL_S"

    # 1) tree RSS budget
    rss=$(tree_rss_kb "$CMD_PID")
    rss_mb=$((rss / 1024))
    if [ "$rss_mb" -gt "$BUDGET_MB" ]; then
        log "BREACH: tree RSS ${rss_mb}MB > budget ${BUDGET_MB}MB — killing tree."
        kill_tree "$CMD_PID"
        BREACH=1
        break
    fi

    # 2) host floor (protect the desktop)
    avail=$(mem_available_mb)
    if [ "$avail" -lt "$FLOOR_MB" ]; then
        log "BREACH: host MemAvailable ${avail}MB < floor ${FLOOR_MB}MB — killing tree."
        kill_tree "$CMD_PID"
        BREACH=1
        break
    fi

    # 3) tmpfs floor (tmpfs IS RAM)
    if [ -n "$tfree" ]; then
        tfree_now=$(tmpfs_free_gb "$TMPDIR_EXPANDED")
        if [ -n "$tfree_now" ] && [ "$tfree_now" -lt "$DISK_FLOOR_GB" ]; then
            log "BREACH: tmpfs $TMPDIR_EXPANDED ${tfree_now}GB free < floor ${DISK_FLOOR_GB}GB — killing tree."
            kill_tree "$CMD_PID"
            BREACH=1
            break
        fi
    fi
done

if [ "$BREACH" = "1" ]; then
    exit 42
fi

wait "$CMD_PID"
rc=$?
log "command exited rc=$rc (peak RSS stayed under ${BUDGET_MB}MB)"
exit "$rc"
