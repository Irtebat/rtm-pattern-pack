#!/usr/bin/env bash
# Runs a batch of pattern benchmarks in one session.
#
# The load generator host is started once at the beginning and stopped at the
# end, also on failure. Each pattern's engines are started and stopped by
# run-benchmark.sh. Runs are sequential, so each one has the shared MSK cluster
# to itself.
#
# Usage:
#   infra/scripts/run-session.sh --patterns 01-dedup,02-unnest [--rate <pattern default>] [--duration 1800] [--warmup 300]
#                                [--continue-on-error]
# Run ids are <id>-tuned-r<rate>-<UTC stamp>.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_DIR="${RTMPACK_ENV_DIR:-$ROOT/infra/terraform/envs/sandbox-usw2}"
PATTERNS=""
RATE=""
DURATION=1800
WARMUP=300
CONTINUE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --patterns) PATTERNS="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --continue-on-error) CONTINUE=true; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$PATTERNS" ]] || { echo "--patterns is required" >&2; exit 2; }

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

host_state() {
  local running="$1" plan="$ENV_DIR/session.tfplan"
  terraform -chdir="$ENV_DIR" plan -input=false -no-color -out="$plan" \
    -var 'running_patterns=[]' -var "loadgen_running=$running" >/dev/null
  if terraform -chdir="$ENV_DIR" show -json "$plan" | python3 -c '
import json, sys
plan = json.load(sys.stdin)
sys.exit(1 if any("delete" in c["change"]["actions"] for c in plan.get("resource_changes", [])) else 0)'; then
    terraform -chdir="$ENV_DIR" apply -input=false -no-color "$plan" >/dev/null
  else
    echo "Refusing to apply: the plan would delete or replace resources" >&2
    rm -f "$plan"
    exit 1
  fi
  rm -f "$plan"
}

stop_host() {
  log "session: stopping the load generator host"
  host_state false || echo "WARNING: could not stop the host; run: terraform apply -var loadgen_running=false" >&2
}

log "session: starting the load generator host"
host_state true
trap stop_host EXIT

PROFILE="$(echo var.aws_profile | terraform -chdir="$ENV_DIR" console 2>/dev/null | tr -d '"')"
REGION="$(echo var.aws_region | terraform -chdir="$ENV_DIR" console 2>/dev/null | tr -d '"')"
INSTANCE_ID="$(terraform -chdir="$ENV_DIR" output -raw loadgen_instance_id)"
log "session: waiting for the host to come online"
for _ in $(seq 1 60); do
  [[ "$(aws ssm describe-instance-information --profile "$PROFILE" --region "$REGION" \
        --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
        --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null)" == "Online" ]] && break
  sleep 10
done
# Install the harness bundle Terraform last uploaded, so the host runs the current code.
log "session: installing the current harness on the host"
"$ROOT/infra/scripts/update-harness.sh"

export RTMPACK_IN_SESSION=true
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
status=0
IFS=',' read -r -a list <<< "$PATTERNS"
for p in "${list[@]}"; do
  log "session: pattern $p"
  rate="$RATE"
  [[ -n "$rate" ]] || rate="$(PYTHONPATH="$ROOT/harness" python3 -c 'import sys; from rtmpack_harness.patterns import get_pattern; print(get_pattern(sys.argv[1]).default_rate_eps or 10000)' "$p")"
  if ! "$ROOT/infra/scripts/run-benchmark.sh" --pattern "$p" --rate "$rate" --duration "$DURATION" \
        --warmup "$WARMUP" --run-id "${p}-tuned-r${rate}-${STAMP}"; then
    status=1
    log "session: pattern $p failed"
    [[ "$CONTINUE" == "true" ]] || break
  fi
done
# Stop the host before announcing the end, so that anything waiting for this line does not
# race the stop.
trap - EXIT
stop_host
log "session: finished with status $status"
exit "$status"
