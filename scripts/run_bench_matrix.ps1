param(
  [string]$Executable = "",
  [string]$OutputDirectory = "",
  [int]$Repetitions = 3,
  [int]$Operations = 1000,
  [string]$Python = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Executable)) {
  $Executable = Join-Path $root "build\release\detlog-bench.exe"
}
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $OutputDirectory = Join-Path $root "bench-results\$stamp"
}
if ($Repetitions -lt 1 -or $Operations -lt 1) {
  throw "Repetitions and Operations must be positive"
}
if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
  throw "Benchmark executable not found: $Executable"
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$raw = Join-Path $OutputDirectory "raw.jsonl"
$errors = Join-Path $OutputDirectory "stderr.log"
$matrix = Join-Path $OutputDirectory "matrix.json"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($raw, "", $utf8NoBom)
[System.IO.File]::WriteAllText($errors, "", $utf8NoBom)

$matrixDescription = [ordered]@{
  generated_at = (Get-Date).ToString("o")
  executable = $Executable
  repetitions = $Repetitions
  operations_per_run = $Operations
  nodes = @(3, 5)
  clients = @(1, 3, 5)
  payload_bytes = @(64, 1024, 16384)
  sim_scenarios = @("healthy", "leader-crash", "partition", "slow-follower", "slow-fsync")
  tcp_scenarios = @("healthy", "leader-crash")
  tcp_unsupported = @("partition", "slow-follower", "slow-fsync")
  note = "TCP nodes share one benchmark process but use real loopback sockets and distinct WALs."
}
$matrixDescription | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $matrix -Encoding utf8

$runIndex = 0
foreach ($mode in @("sim", "tcp")) {
  $scenarios = if ($mode -eq "sim") {
    @("healthy", "leader-crash", "partition", "slow-follower", "slow-fsync")
  } else {
    @("healthy", "leader-crash")
  }
  foreach ($nodes in @(3, 5)) {
    foreach ($clients in @(1, 3, 5)) {
      foreach ($payload in @(64, 1024, 16384)) {
        foreach ($scenario in $scenarios) {
          for ($trial = 1; $trial -le $Repetitions; $trial++) {
            $runIndex++
            $seed = [uint64](1000000 * $trial + $runIndex)
            $arguments = @(
              "--mode", $mode,
              "--nodes", $nodes,
              "--clients", $clients,
              "--payload", $payload,
              "--operations", $Operations,
              "--trial", $trial,
              "--seed", $seed,
              "--scenario", $scenario
            )
            Write-Host "[$runIndex] $mode $scenario n=$nodes c=$clients p=$payload trial=$trial"
            $lines = & $Executable @arguments 2>> $errors
            $exitCode = $LASTEXITCODE
            if ($null -ne $lines) {
              [System.IO.File]::AppendAllLines(
                $raw, [string[]]$lines, $utf8NoBom)
            }
            if ($exitCode -ne 0) {
              throw "Benchmark run failed with exit code $exitCode; raw output was preserved in $raw"
            }
          }
        }
      }
    }
  }
}

& $Python (Join-Path $PSScriptRoot "plot_bench.py") $raw `
  --csv (Join-Path $OutputDirectory "summary.csv") `
  --svg (Join-Path $OutputDirectory "throughput.svg")
if ($LASTEXITCODE -ne 0) {
  throw "Plot generation failed; raw results remain in $raw"
}
Write-Host "Benchmark artifacts: $OutputDirectory"
