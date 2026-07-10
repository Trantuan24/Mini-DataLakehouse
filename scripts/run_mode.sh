#!/usr/bin/env bash
# Prepare and run a clean full-load baseline or a deterministic replay demo.

set -euo pipefail

COMPOSE=(docker compose)
SCHEDULER_SERVICE="airflow-scheduler"
POSTGRES_SERVICE="postgres"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_mode.sh full --reset [--no-pipeline] [--dry-run]
  scripts/run_mode.sh replay --reset [--lifecycle 0|1] [--no-pipeline] [--dry-run]
  scripts/run_mode.sh replay --next  [--lifecycle 0|1] [--month YYYY-MM] [--no-pipeline] [--dry-run]

Modes:
  full            Seed all source tables, then trigger the analytics pipeline.
  replay --reset  Start a fresh month-by-month incremental scenario.
  replay --next   Advance the existing replay scenario by one month, then run it.

Safety:
  --reset removes all Iceberg layer tables, snapshots, watermarks, and their
  MinIO layer files. It does not touch raw/ or dataset/*.csv.
  Switching between full and replay always requires --reset.
  A real reset also requires --confirm-reset; dry runs do not.

Options:
  --lifecycle 0|1  Replay only: 0 inserts final orders; 1 simulates CDC updates.
  --month YYYY-MM  Replay --next only: choose a source month explicitly.
  --no-pipeline    Do not trigger lakehouse_pipeline after source preparation.
  --confirm-reset  Required together with --reset when not using --dry-run.
  --timeout SEC     Maximum wait for each source DAG before failing (default: 1800).
  --dry-run        Print actions without changing data or triggering DAGs.
EOF
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

run() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '+ '
    printf '%q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

compose_exec() {
  run "${COMPOSE[@]}" exec -T "$@"
}

assert_no_running_dag() {
  local dag_id="$1"
  local output
  local running_count

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "+ check no active ${dag_id} DAG run"
    return
  fi

  output=$("${COMPOSE[@]}" exec -T "$SCHEDULER_SERVICE" \
    airflow dags list-runs --dag-id "$dag_id" --state running --output json)
  running_count=$(printf '%s' "$output" | python3 -c '
import json
import sys

print(len(json.load(sys.stdin)))
')
  [[ "$running_count" == "0" ]] || die "${dag_id} already has a running DAG run"
}

trigger_dag() {
  local dag_id="$1"
  local conf="$2"
  assert_no_running_dag "$dag_id"
  LAST_RUN_ID="mode_${MODE}_$(date -u +%Y%m%dT%H%M%S%N)_${dag_id}"
  compose_exec "$SCHEDULER_SERVICE" airflow dags trigger "$dag_id" \
    --run-id "$LAST_RUN_ID" --conf "$conf"
  echo "Triggered ${dag_id} (run_id=${LAST_RUN_ID})."
}

wait_for_dag() {
  local dag_id="$1"
  local run_id="$2"
  local elapsed=0
  local output

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "+ wait for ${dag_id} (run_id=${run_id}) to succeed"
    return
  fi

  echo "Waiting for ${dag_id} to finish..."
  while (( elapsed <= TIMEOUT_SECONDS )); do
    output=$("${COMPOSE[@]}" exec -T "$SCHEDULER_SERVICE" \
      airflow dags list-runs --dag-id "$dag_id" --output json)
    state=$(printf '%s' "$output" | python3 -c '
import json
import sys

run_id = sys.argv[1]
for dag_run in json.load(sys.stdin):
    if dag_run.get("run_id") == run_id:
        print(dag_run.get("state", ""))
        break
' "$run_id")
    if [[ "$state" == "success" ]]; then
      echo "${dag_id} succeeded."
      return
    fi
    if [[ "$state" == "failed" ]]; then
      die "${dag_id} failed; inspect its Airflow logs before continuing"
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  die "timed out waiting for ${dag_id} after ${TIMEOUT_SECONDS}s"
}

reset_state() {
  echo "Resetting Iceberg layers and replay cursor..."
  compose_exec "$SCHEDULER_SERVICE" spark-submit \
    /opt/pipeline/admin/reset_lakehouse_state.py --apply
  compose_exec "$POSTGRES_SERVICE" sh -c \
    'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$SOURCE_DB" -c "DROP TABLE IF EXISTS olist_source.sim_state"'
}

MODE="${1:-}"
[[ -n "$MODE" ]] || { usage; exit 2; }
shift

RESET=0
CONFIRM_RESET=0
NEXT=0
LIFECYCLE="0"
MONTH=""
RUN_PIPELINE=1
DRY_RUN=0
TIMEOUT_SECONDS=1800
LAST_RUN_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset) RESET=1 ;;
    --confirm-reset) CONFIRM_RESET=1 ;;
    --next) NEXT=1 ;;
    --lifecycle)
      shift
      [[ $# -gt 0 ]] || die "--lifecycle needs 0 or 1"
      LIFECYCLE="$1"
      ;;
    --month)
      shift
      [[ $# -gt 0 ]] || die "--month needs YYYY-MM"
      MONTH="$1"
      ;;
    --no-pipeline) RUN_PIPELINE=0 ;;
    --timeout)
      shift
      [[ $# -gt 0 ]] || die "--timeout needs a positive number of seconds"
      TIMEOUT_SECONDS="$1"
      ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done

[[ "$LIFECYCLE" =~ ^[01]$ ]] || die "--lifecycle must be 0 or 1"
[[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "--timeout must be a positive integer"
[[ -z "$MONTH" || "$MONTH" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] || \
  die "--month must use YYYY-MM"
if [[ "$RESET" == "1" && "$DRY_RUN" == "0" && "$CONFIRM_RESET" == "0" ]]; then
  die "--reset is destructive; add --confirm-reset after reviewing --dry-run output"
fi

case "$MODE" in
  full)
    [[ "$NEXT" == "0" ]] || die "--next is only valid for replay"
    [[ "$RESET" == "1" ]] || die "full requires --reset to avoid mixing scenarios"
    [[ -z "$MONTH" ]] || die "--month is only valid for replay --next"
    reset_state
    trigger_dag "seed_source_postgres" '{"mode":"full"}'
    if [[ "$RUN_PIPELINE" == "1" ]]; then
      wait_for_dag "seed_source_postgres" "$LAST_RUN_ID"
    fi
    ;;
  replay)
    if [[ "$NEXT" == "1" ]]; then
      [[ "$RESET" == "0" ]] || die "use either --reset or --next, not both"
    else
      [[ "$RESET" == "1" ]] || die "a new replay scenario requires --reset"
      [[ -z "$MONTH" ]] || die "--month is only valid with replay --next"
      reset_state
      trigger_dag "seed_source_postgres" '{"mode":"dims_only"}'
      if [[ "$RUN_PIPELINE" == "1" ]]; then
        wait_for_dag "seed_source_postgres" "$LAST_RUN_ID"
      fi
    fi

    replay_conf="{\"lifecycle\":\"${LIFECYCLE}\""
    if [[ -n "$MONTH" ]]; then
      replay_conf+=",\"month\":\"${MONTH}\""
    fi
    replay_conf+="}"
    trigger_dag "simulate_source" "$replay_conf"
    if [[ "$RUN_PIPELINE" == "1" ]]; then
      wait_for_dag "simulate_source" "$LAST_RUN_ID"
    fi
    ;;
  *)
    usage
    die "mode must be full or replay"
    ;;
esac

if [[ "$RUN_PIPELINE" == "1" ]]; then
  replay_flag="0"
  [[ "$MODE" == "replay" ]] && replay_flag="1"
  trigger_dag "lakehouse_pipeline" "{\"replay\":\"${replay_flag}\"}"
fi

echo "Done. Follow lakehouse_pipeline in Airflow for the remaining task status."
