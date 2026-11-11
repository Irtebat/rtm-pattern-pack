#!/usr/bin/env bash
# Runs one benchmark for one pattern against both engines and collects the results.
#
# Steps:
#   1. Make sure the load generator host is running (Terraform state toggle).
#   2. Create the pattern topics (idempotent).
#   3. Start both engines for this pattern through Terraform (RTM job unpaused,
#      Flink application started).
#   4. Probe until both output topics receive records, then run the load generator.
#   5. Collect latency, correctness and split-half stability for each engine.
#   6. Copy results to S3 and to results/, then stop both engines (also on failure).
#      The host is stopped too, unless the run is part of a session (run-session.sh).
#
# Every Terraform change goes through a saved plan. The run is aborted if a plan
# would delete or replace any resource.
#
# Usage:
#   infra/scripts/run-benchmark.sh --pattern 02-unnest [--rate <pattern default>] [--duration 1800] [--warmup 300]
#                                  [--partitions 8] [--run-id ID] [--keep-running]
#                                  [--tf-var name=value ...]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_DIR="${RTMPACK_ENV_DIR:-$ROOT/infra/terraform/envs/sandbox-usw2}"

PATTERN=""
RATE=""
DURATION=1800
WARMUP=300
PARTITIONS=8
RUN_ID=""
KEEP_RUNNING=false
TF_EXTRA=()
SETTLE_S="${RTMPACK_SETTLE_S:-90}"
IN_SESSION="${RTMPACK_IN_SESSION:-false}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pattern) PATTERN="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --partitions) PARTITIONS="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --keep-running) KEEP_RUNNING=true; shift ;;
    --tf-var) TF_EXTRA+=(-var "$2"); shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$PATTERN" ]] || { echo "--pattern is required" >&2; exit 2; }
# Without --rate, use the pattern's own default rate from the harness registry
# (10,000 events/s for most patterns; 08-join counts orders, 5,000/s).
if [[ -z "$RATE" ]]; then
  RATE="$(PYTHONPATH="$ROOT/harness" python3 -c 'import sys; from rtmpack_harness.patterns import get_pattern; print(get_pattern(sys.argv[1]).default_rate_eps or 10000)' "$PATTERN")"
fi
# Run ids have the form <id>-<profile>-r<rate>-<UTC stamp>; the tuned profile is the only one.
RUN_ID="${RUN_ID:-${PATTERN}-tuned-r${RATE}-$(date -u +%Y%m%dT%H%M%SZ)}"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
tf() { terraform -chdir="$ENV_DIR" "$@"; }
tf_out() { tf output -raw "$1"; }
tf_var() { echo "var.$1" | tf console ${TF_EXTRA[@]+"${TF_EXTRA[@]}"} 2>/dev/null | tr -d '"'; }
pattern_field() {
  # Reads one field of this pattern from the `patterns` output.
  tf output -json patterns | python3 -c 'import json,sys; print(json.load(sys.stdin)[sys.argv[1]][sys.argv[2]])' "$PATTERN" "$1"
}

AWS_PROFILE_NAME="$(tf_var aws_profile)"
AWS_REGION_NAME="$(tf_var aws_region)"
DATABRICKS_PROFILE_NAME="$(tf_var databricks_profile)"
export AWS_PROFILE="$AWS_PROFILE_NAME" AWS_REGION="$AWS_REGION_NAME"

# Fail early on expired credentials rather than in the middle of a run.
aws sts get-caller-identity >/dev/null || { echo "AWS credentials invalid; run: aws sso login --profile $AWS_PROFILE_NAME" >&2; exit 1; }
databricks current-user me --profile "$DATABRICKS_PROFILE_NAME" >/dev/null || { echo "Databricks auth invalid for profile $DATABRICKS_PROFILE_NAME" >&2; exit 1; }

# Applies the desired state through a saved plan, refusing any delete or replace.
#   $1: patterns whose engines should run, as an HCL list, for example ["02-unnest"] or []
#   $2: whether the load generator host should run (true/false)
apply_state() {
  local patterns="$1" host="$2" plan="$ENV_DIR/state.tfplan"
  log "terraform: running_patterns=$patterns loadgen_running=$host"
  tf plan -input=false -no-color -out="$plan" ${TF_EXTRA[@]+"${TF_EXTRA[@]}"} \
    -var "running_patterns=$patterns" -var "loadgen_running=$host" >/dev/null
  local deletes
  deletes="$(tf show -json "$plan" | python3 -c '
import json, sys
plan = json.load(sys.stdin)
bad = [c["address"] for c in plan.get("resource_changes", []) if "delete" in c["change"]["actions"]]
print("\n".join(bad))')"
  if [[ -n "$deletes" ]]; then
    echo "Refusing to apply: the plan would delete or replace:" >&2
    echo "$deletes" >&2
    rm -f "$plan"
    exit 1
  fi
  tf apply -input=false -no-color "$plan" >/dev/null
  rm -f "$plan"
}

JOB_ID="$(pattern_field rtm_job_id)"
FLINK_APP="$(pattern_field flink_app)"
INSTANCE_ID="$(tf_out loadgen_instance_id)"
MSK_BOOTSTRAP="$(tf_out msk_bootstrap)"

# Runs a shell command on the load generator host through SSM and prints its output.
ssm_run() {
  local timeout_s="$1"; shift
  local script="source /opt/rtmpack/rtmpack.env && export RTMPACK_BOOTSTRAP='$MSK_BOOTSTRAP' && cd /opt/rtmpack && $*"
  local params cmd_id status="InProgress"
  params="$(python3 -c 'import json,sys; print(json.dumps({"commands":[sys.argv[1]],"executionTimeout":[sys.argv[2]]}))' "$script" "$timeout_s")"
  cmd_id="$(aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
    --parameters "$params" --timeout-seconds 600 --query 'Command.CommandId' --output text)"
  local fails=0
  while [[ "$status" == "InProgress" || "$status" == "Pending" || "$status" == "Delayed" ]]; do
    sleep 10
    if status="$(aws ssm get-command-invocation --command-id "$cmd_id" --instance-id "$INSTANCE_ID" \
        --query Status --output text 2>/dev/null)"; then
      fails=0
    else
      # Keep waiting: the remote command keeps running, and polling resumes once
      # credentials or the network are back. Say so instead of waiting silently.
      status=Pending; fails=$((fails + 1))
      if (( fails % 60 == 1 )); then
        log "WARNING: cannot read the remote command status (AWS credentials or network)." \
          "If the SSO session expired, run: aws sso login --profile $AWS_PROFILE_NAME"
      fi
    fi
  done
  aws ssm get-command-invocation --command-id "$cmd_id" --instance-id "$INSTANCE_ID" \
    --query '[StandardOutputContent,StandardErrorContent]' --output text
  [[ "$status" == "Success" ]] || { echo "remote command failed with status $status" >&2; return 1; }
}

wait_for_host() {
  log "waiting for the load generator host to be reachable through SSM"
  for _ in $(seq 1 60); do
    # The SSM ping status stays "Online" for a while after the instance stops, so check the
    # EC2 state as well.
    if [[ "$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
          --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)" == "running" && \
          "$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
          --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null)" == "Online" ]]; then
      return 0
    fi
    sleep 10
  done
  echo "load generator host did not come online" >&2
  exit 1
}

cleanup() {
  if [[ "$KEEP_RUNNING" == "true" ]]; then
    return
  fi
  local host=false
  [[ "$IN_SESSION" == "true" ]] && host=true
  if ! ( apply_state "[]" "$host" ); then
    # Fallback when a full plan is impossible (for example an expired Databricks token
    # makes the plan incomplete): pause the RTM job directly and stop the Flink
    # application with a targeted apply that does not refresh Databricks resources.
    # The next full apply sets the same state, so nothing drifts permanently.
    echo "WARNING: full stop failed; using the fallback stop" >&2
    databricks jobs update --profile "$DATABRICKS_PROFILE_NAME" \
      --json "{\"job_id\": $JOB_ID, \"new_settings\": {\"continuous\": {\"pause_status\": \"PAUSED\"}}}" >/dev/null \
      || echo "WARNING: could not pause RTM job $JOB_ID" >&2
    local targets=(-target="module.flink_app[\"$PATTERN\"].aws_kinesisanalyticsv2_application.flink")
    [[ "$host" == "false" ]] && targets+=(-target=module.loadgen.aws_ec2_instance_state.loadgen)
    tf plan -refresh=false -input=false -no-color -out="$ENV_DIR/stop.tfplan" \
      -var 'running_patterns=[]' -var "loadgen_running=$host" "${targets[@]}" >/dev/null \
      && tf apply -input=false -no-color "$ENV_DIR/stop.tfplan" >/dev/null \
      || echo "WARNING: could not stop Flink application $FLINK_APP; stop it from the console" >&2
    rm -f "$ENV_DIR/stop.tfplan"
  fi
  # Cancel active RTM runs so the cluster stops.
  databricks jobs cancel-all-runs --profile "$DATABRICKS_PROFILE_NAME" --job-id "$JOB_ID" >/dev/null \
    || echo "WARNING: could not cancel RTM runs for job $JOB_ID" >&2
}

log "run $RUN_ID: pattern=$PATTERN rate=$RATE duration=$DURATION warmup=$WARMUP partitions=$PARTITIONS"

trap cleanup EXIT
# Outside a session the run owns the host. In a session the session started it, but start it
# anyway if it is not running (for example, a previous step stopped it).
if [[ "$IN_SESSION" != "true" || "$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
      --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null)" != "running" ]]; then
  apply_state "[]" true
fi
wait_for_host

log "creating topics"
ssm_run 300 "rtmpack topics create --pattern $PATTERN --partitions $PARTITIONS"

apply_state "[\"$PATTERN\"]" true

log "waiting for RTM job $JOB_ID and Flink application $FLINK_APP"
for _ in $(seq 1 90); do
  rtm_state="$(databricks jobs list-runs --profile "$DATABRICKS_PROFILE_NAME" --job-id "$JOB_ID" --active-only -o json \
    | python3 -c 'import json,sys; r=json.load(sys.stdin); r=r if isinstance(r,list) else r.get("runs",[]); print(r[0]["state"]["life_cycle_state"] if r else "NONE")')"
  flink_state="$(aws kinesisanalyticsv2 describe-application --application-name "$FLINK_APP" \
    --query 'ApplicationDetail.ApplicationStatus' --output text)"
  log "rtm=$rtm_state flink=$flink_state"
  [[ "$rtm_state" == "RUNNING" && "$flink_state" == "RUNNING" ]] && break
  sleep 20
done
[[ "$rtm_state" == "RUNNING" && "$flink_state" == "RUNNING" ]] || { echo "engines did not reach RUNNING" >&2; exit 1; }

# Send short probe bursts until both output topics contain probe records, then let
# the engines settle.
log "probing until both engines produce output"
probe_ok=false
for attempt in $(seq 1 30); do
  probe_id="probe-${RUN_ID}-${attempt}"
  # Probes use their own seeds so their event ids never collide with each other or the run.
  ssm_run 300 "rtmpack loadgen --pattern $PATTERN --run-id $probe_id --seed $((9000 + attempt)) --rate-eps 200 --duration-s 20 --warmup-s 0 --results-dir probes >/dev/null \
    && rtmpack collect --run-id $probe_id --engine rtm --engine-version probe --compute-desc probe --vcpu 1 --cost-per-hour-usd 0 --grace-s 20 --results-dir probes >/dev/null \
    && rtmpack collect --run-id $probe_id --engine flink --engine-version probe --compute-desc probe --vcpu 1 --cost-per-hour-usd 0 --grace-s 20 --results-dir probes >/dev/null \
    && python3 -c 'import json,sys; r={x[\"engine\"]: x[\"out_records\"] for x in map(json.loads, open(sys.argv[1]))}; print(\"probe\", r); sys.exit(0 if all(r.get(e) for e in (\"rtm\", \"flink\")) else 3)' probes/$probe_id.jsonl" \
    && { probe_ok=true; break; }
  log "probe $attempt: not ready yet"
done
[[ "$probe_ok" == "true" ]] || { echo "engines did not produce output during probing" >&2; exit 1; }
log "both producing output; settling for ${SETTLE_S}s before load"
sleep "$SETTLE_S"

log "running load generator"
ssm_run $((DURATION + 900)) "rtmpack loadgen --pattern $PATTERN --run-id $RUN_ID --rate-eps $RATE --duration-s $DURATION --warmup-s $WARMUP --results-dir results"

log "collecting results"
for engine in rtm flink; do
  ssm_run 2700 "rtmpack collect --run-id $RUN_ID --engine $engine \
    --engine-version '$(pattern_field ${engine}_version)' \
    --compute-desc '$(pattern_field ${engine}_desc)' \
    --vcpu $(pattern_field ${engine}_vcpu) \
    --cost-per-hour-usd $(pattern_field ${engine}_cost_per_hour) \
    --results-dir results"
done

BUCKET="$(tf_out artifact_bucket)"
S3_PREFIX="s3://$BUCKET/rtm-pattern-pack/results/$RUN_ID"
ssm_run 300 "aws s3 cp results/ $S3_PREFIX/ --recursive --exclude '*' --include '$RUN_ID*'"
mkdir -p "$ROOT/results"
aws s3 cp "$S3_PREFIX/" "$ROOT/results/" --recursive >/dev/null

log "results:"
cat "$ROOT/results/$RUN_ID.jsonl"

# A run where an engine produced no output is not a valid result. Check the engine
# logs before rerunning.
python3 - "$ROOT/results/$RUN_ID.jsonl" <<'PY'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
bad = [r["engine"] for r in rows if not r.get("out_records")]
if bad:
    sys.exit(f"invalid run: no output from {', '.join(bad)}")
unstable = [r["engine"] for r in rows if r.get("stable") is False]
if unstable:
    print(f"note: split-half check failed for {', '.join(unstable)}; repeat the run (CONTRACT section 8)")
p99 = {r["engine"]: r.get("p99_ms") for r in rows}
if all(p99.get(e) for e in ("rtm", "flink")):
    lo, hi = sorted((p99["rtm"], p99["flink"]))
    if (hi - lo) / hi <= 0.15:
        print("note: engines are within 15 percent on p99; report them as comparable at p99 (CONTRACT section 8)")
PY
log "done: $RUN_ID"
