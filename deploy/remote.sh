#!/usr/bin/env bash
# Runs a shell command on the Hermes instance through Systems Manager and prints its output (run in CloudShell).
#
#   bash deploy/remote.sh 'docker ps'
#   bash deploy/remote.sh update        # git pull + rebuild + restart changed containers
set -euo pipefail
export AWS_REGION="${AWS_REGION:-us-east-2}" AWS_PAGER=""
IID=$(aws ec2 describe-instances --filters Name=tag:Name,Values=hermes Name=instance-state-name,Values=running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
CMD="${1:?usage: remote.sh '<command>' | update}"
[ "$CMD" = "update" ] && CMD="cd /opt/hermes && git pull --ff-only && HERMES_PORT=80 docker compose up -d --build --remove-orphans && HERMES_PORT=80 docker compose up -d --force-recreate connect-init && docker image prune -f"
PARAMS=$(python3 -c 'import json,sys; print(json.dumps({"commands": ["set -e", sys.argv[1]], "executionTimeout": ["1800"]}))' "$CMD")
ID=$(aws ssm send-command --instance-ids "$IID" --document-name AWS-RunShellScript --parameters "$PARAMS" \
  --query Command.CommandId --output text)
while :; do
  sleep 3
  S=$(aws ssm get-command-invocation --command-id "$ID" --instance-id "$IID" --query Status --output text 2>/dev/null || echo Pending)
  case "$S" in Pending|InProgress|Delayed) ;; *) break ;; esac
done
aws ssm get-command-invocation --command-id "$ID" --instance-id "$IID" --query '[StandardOutputContent,StandardErrorContent]' --output text
echo "status: $S"
