$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')

$samples = @(
    '01-HelloWorld', '02-WorkQueues', '03-PublishSubscribe', '04-Routing', '05-Topics', '06-Headers', '07-Rpc',
    '08-PublisherConfirms', '09-DeadLetterRetry', '10-QueueFeatures', '11-Streams', '12-OutboxInbox', '14-MassTransit'
)

$failed = @()
foreach ($sample in $samples) {
    dotnet run --no-build --project "samples/$sample"
    if ($LASTEXITCODE -ne 0) { $failed += $sample }
}

if ($failed.Count -gt 0) {
    Write-Host "Failed samples: $($failed -join ', ')" -ForegroundColor Red
    exit 1
}

Write-Host "All $($samples.Count) samples passed (13-OrdersApi is a long-running web app, run it manually)." -ForegroundColor Green
