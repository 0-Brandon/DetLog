param(
  [string]$Executable = "",
  [string]$OutputDirectory = "",
  [int]$Repetitions = 3,
  [long[]]$EntrySizes = @(100, 1000, 5000),
  [long[]]$PayloadBytes = @(64, 1024, 8192),
  [int[]]$GroupSizes = @(8, 32),
  [string]$Environment = "",
  [switch]$Resume,
  [switch]$AcknowledgeAmbiguousRun,
  [int]$MaxNewRuns = 0,
  [string]$Python = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
if ($Resume -and [string]::IsNullOrWhiteSpace($OutputDirectory)) {
  throw "-Resume requires an explicit -OutputDirectory"
}
if ([string]::IsNullOrWhiteSpace($Executable)) {
  $Executable = Join-Path $root "build\release\detlog-wal-bench.exe"
}
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $OutputDirectory = Join-Path $root "bench-results\wal-$stamp"
}
if ([string]::IsNullOrWhiteSpace($Environment)) {
  throw "-Environment is required for a provenance-bound benchmark campaign"
}

$arguments = @(
  (Join-Path $PSScriptRoot "run_checkpointed_campaign.py"),
  "wal", "--executable", $Executable, "--output", $OutputDirectory,
  "--repetitions", $Repetitions, "--entry-sizes"
)
foreach ($entries in $EntrySizes) { $arguments += $entries }
$arguments += "--payload-bytes"
foreach ($payload in $PayloadBytes) { $arguments += $payload }
$arguments += "--group-sizes"
foreach ($group in $GroupSizes) { $arguments += $group }
$arguments += @("--environment", $Environment)
if ($Resume) { $arguments += "--resume" }
if ($AcknowledgeAmbiguousRun) { $arguments += "--acknowledge-ambiguous-run" }
if ($MaxNewRuns -ne 0) { $arguments += @("--max-new-runs", $MaxNewRuns) }

& $Python @arguments
if ($LASTEXITCODE -ne 0) {
  throw "Checkpointed WAL campaign failed with exit code $LASTEXITCODE"
}
