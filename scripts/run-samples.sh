#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

samples=(
  01-HelloWorld 02-WorkQueues 03-PublishSubscribe 04-Routing 05-Topics 06-Headers 07-Rpc
  08-PublisherConfirms 09-DeadLetterRetry 10-QueueFeatures 11-Streams 12-OutboxInbox 14-MassTransit
)

failed=()
for sample in "${samples[@]}"; do
  echo "::group::$sample"
  if timeout 120 dotnet run --no-build --project "samples/$sample"; then
    echo "OK $sample"
  else
    failed+=("$sample")
  fi
  echo "::endgroup::"
done

if ((${#failed[@]})); then
  echo "Failed samples: ${failed[*]}"
  exit 1
fi

echo "All ${#samples[@]} samples passed (13-OrdersApi is a long-running web app, run it manually)."
