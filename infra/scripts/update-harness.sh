#!/usr/bin/env bash
# Installs the current harness bundle on the running load generator host.
# Run after a harness change and `terraform apply` (which uploads the bundle).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_DIR="${RTMPACK_ENV_DIR:-$ROOT/infra/terraform/envs/sandbox-usw2}"
tf_out() { terraform -chdir="$ENV_DIR" output -raw "$1"; }
tf_var() { echo "var.$1" | terraform -chdir="$ENV_DIR" console 2>/dev/null | tr -d '"'; }

export AWS_PROFILE="$(tf_var aws_profile)" AWS_REGION="$(tf_var aws_region)"
INSTANCE_ID="$(tf_out loadgen_instance_id)"
BUNDLE="s3://$(tf_out artifact_bucket)/rtm-pattern-pack/loadgen/rtmpack-harness.zip"

SCRIPT="set -euo pipefail
cd /opt/rtmpack
aws s3 cp $BUNDLE rtmpack-harness.zip --quiet
rm -rf harness && mkdir harness && unzip -q -o rtmpack-harness.zip -d harness
venv/bin/pip install --quiet --force-reinstall --no-deps ./harness
venv/bin/rtmpack --version"

PARAMS="$(python3 -c 'import json,sys; print(json.dumps({"commands":[sys.argv[1]]}))' "$SCRIPT")"
CMD_ID="$(aws ssm send-command --instance-ids "$INSTANCE_ID" --document-name AWS-RunShellScript \
  --parameters "$PARAMS" --query Command.CommandId --output text)"
STATUS=InProgress
while [[ "$STATUS" == InProgress || "$STATUS" == Pending ]]; do
  sleep 5
  STATUS="$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" --query Status --output text 2>/dev/null || echo Pending)"
done
aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" \
  --query '[StandardOutputContent,StandardErrorContent]' --output text
[[ "$STATUS" == Success ]]
