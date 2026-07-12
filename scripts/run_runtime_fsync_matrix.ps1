param(
  [string]$Executable = "",
  [string]$OutputDirectory = "",
  [int]$Repetitions = 3,
  [int]$Operations = 1000,
  [int[]]$GroupSizes = @(2, 5),
  [int]$GroupDelayMs = 2,
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
  $OutputDirectory = Join-Path $root "bench-results\runtime-fsync-$stamp"
}
if ($Repetitions -lt 1 -or $Operations -le 5 -or
    $GroupDelayMs -lt 1 -or $GroupDelayMs -gt 1000 -or
    $GroupSizes.Count -eq 0) {
  throw "Repetitions/delay must be positive, operations must exceed 5, and group sizes must be present"
}
foreach ($group in $GroupSizes) {
  if ($group -lt 2 -or $group -gt 1024) {
    throw "Every group size must be in the range 2..1024"
  }
}
if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
  throw "Benchmark executable not found: $Executable"
}
if ((Test-Path -LiteralPath $OutputDirectory) -and
    @(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -ne 0) {
  throw "Refusing to overwrite nonempty output directory: $OutputDirectory"
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$raw = Join-Path $OutputDirectory "raw.jsonl"
$errors = Join-Path $OutputDirectory "stderr.log"
$matrix = Join-Path $OutputDirectory "matrix.json"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($raw, "", $utf8NoBom)
[System.IO.File]::WriteAllText($errors, "", $utf8NoBom)

function Invoke-StreamedBenchmark([string[]]$Arguments) {
  $startInfo = New-Object System.Diagnostics.ProcessStartInfo
  $startInfo.FileName = $Executable
  $startInfo.Arguments = ($Arguments -join " ")
  $startInfo.UseShellExecute = $false
  $startInfo.CreateNoWindow = $true
  $startInfo.RedirectStandardOutput = $true
  $startInfo.RedirectStandardError = $true
  $process = New-Object System.Diagnostics.Process
  $process.StartInfo = $startInfo
  $rawStream = [System.IO.File]::Open(
    $raw, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write,
    [System.IO.FileShare]::Read)
  $errorStream = [System.IO.File]::Open(
    $errors, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write,
    [System.IO.FileShare]::Read)
  try {
    if (-not $process.Start()) { throw "Could not start benchmark: $Executable" }
    $stdoutCopy = $process.StandardOutput.BaseStream.CopyToAsync($rawStream)
    $stderrCopy = $process.StandardError.BaseStream.CopyToAsync($errorStream)
    $process.WaitForExit()
    [System.Threading.Tasks.Task]::WaitAll(@($stdoutCopy, $stderrCopy))
    return $process.ExitCode
  } finally {
    $rawStream.Dispose()
    $errorStream.Dispose()
    $process.Dispose()
  }
}

$variants = @(@{ policy = "flush-every"; size = $GroupSizes[0] })
foreach ($group in $GroupSizes) {
  $variants += @{ policy = "group"; size = $group }
}
$expectedRuns = 36 * $variants.Count * $Repetitions
$matrixDescription = [ordered]@{
  schema = "detlog-runtime-fsync-matrix/v1"
  generated_at = (Get-Date).ToString("o")
  executable = $Executable
  repetitions = $Repetitions
  operations_per_run = $Operations
  expected_runs = $expectedRuns
  mode = "tcp"
  nodes = @(3, 5)
  clients = @(1, 3, 5)
  payload_bytes = @(64, 1024, 16384)
  scenarios = @("healthy", "leader-crash")
  policies = @("flush-every", "group")
  group_sizes = @($GroupSizes)
  group_delay_ms = $GroupDelayMs
  note = "End-to-end NodeHost comparison over real loopback TCP and distinct WAL files."
}
$matrixJson = $matrixDescription | ConvertTo-Json -Depth 4
[System.IO.File]::WriteAllText(
  $matrix, $matrixJson + [Environment]::NewLine, $utf8NoBom)

$runIndex = 0
foreach ($nodes in @(3, 5)) {
  foreach ($clients in @(1, 3, 5)) {
    foreach ($payload in @(64, 1024, 16384)) {
      foreach ($scenario in @("healthy", "leader-crash")) {
        foreach ($variant in $variants) {
          for ($trial = 1; $trial -le $Repetitions; $trial++) {
            $runIndex++
            $seed = [uint64](2000000 * $trial + $runIndex)
            $arguments = @(
              "--mode", "tcp", "--nodes", $nodes,
              "--clients", $clients, "--payload", $payload,
              "--operations", $Operations, "--trial", $trial,
              "--seed", $seed, "--scenario", $scenario,
              "--fsync-policy", $variant.policy,
              "--group-size", $variant.size,
              "--group-delay-ms", $GroupDelayMs
            )
            Write-Host "[$runIndex] $scenario n=$nodes c=$clients p=$payload fsync=$($variant.policy) g=$($variant.size) trial=$trial"
            $exitCode = Invoke-StreamedBenchmark $arguments
            if ($exitCode -ne 0) {
              throw "Benchmark failed with exit code $exitCode; raw output remains in $raw"
            }
          }
        }
      }
    }
  }
}

& $Python (Join-Path $PSScriptRoot "validate_benchmark_artifacts.py") `
  $OutputDirectory
if ($LASTEXITCODE -ne 0) {
  throw "Benchmark completeness validation failed; raw results remain in $raw"
}
& $Python (Join-Path $PSScriptRoot "plot_bench.py") $raw `
  --csv (Join-Path $OutputDirectory "summary.csv") `
  --svg (Join-Path $OutputDirectory "throughput.svg")
if ($LASTEXITCODE -ne 0) {
  throw "Plot generation failed; raw results remain in $raw"
}
Write-Host "Runtime fsync benchmark artifacts: $OutputDirectory"
