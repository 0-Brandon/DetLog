param(
  [string]$Executable = "",
  [string]$OutputDirectory = "",
  [int]$Repetitions = 3,
  [int]$Operations = 1000,
  [int[]]$GroupSizes = @(2, 5),
  [int]$GroupDelayMs = 2,
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
  $Executable = Join-Path $root "build\release\detlog-bench.exe"
}
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $OutputDirectory = Join-Path $root "bench-results\runtime-fsync-$stamp"
}
if ([string]::IsNullOrWhiteSpace($Environment)) {
  throw "-Environment is required for a provenance-bound benchmark campaign"
}

$arguments = @(
  (Join-Path $PSScriptRoot "run_checkpointed_campaign.py"),
  "runtime-fsync", "--executable", $Executable,
  "--output", $OutputDirectory, "--repetitions", $Repetitions,
  "--operations", $Operations, "--group-delay-ms", $GroupDelayMs,
  "--group-sizes"
)
foreach ($group in $GroupSizes) { $arguments += $group }
$arguments += @("--environment", $Environment)
if ($Resume) { $arguments += "--resume" }
if ($AcknowledgeAmbiguousRun) { $arguments += "--acknowledge-ambiguous-run" }
if ($MaxNewRuns -ne 0) { $arguments += @("--max-new-runs", $MaxNewRuns) }

& $Python @arguments
if ($LASTEXITCODE -ne 0) {
  throw "Checkpointed runtime fsync campaign failed with exit code $LASTEXITCODE"
}
