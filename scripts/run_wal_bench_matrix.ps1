param(
  [string]$Executable = "",
  [string]$OutputDirectory = "",
  [int]$Repetitions = 3,
  [long[]]$EntrySizes = @(100, 1000, 5000),
  [long[]]$PayloadBytes = @(64, 1024, 8192),
  [int[]]$GroupSizes = @(8, 32),
  [string]$Python = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Executable)) {
  $Executable = Join-Path $root "build\release\detlog-wal-bench.exe"
}
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
  $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
  $OutputDirectory = Join-Path $root "bench-results\wal-$stamp"
}
if ($Repetitions -lt 1 -or $Repetitions -gt 1000) {
  throw "Repetitions must be in the range 1..1000"
}
if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
  throw "WAL benchmark executable not found: $Executable"
}
if ($EntrySizes.Count -eq 0 -or $PayloadBytes.Count -eq 0 -or
    $GroupSizes.Count -eq 0) {
  throw "EntrySizes, PayloadBytes, and GroupSizes must all be nonempty"
}
foreach ($entries in $EntrySizes) {
  if ($entries -lt 1 -or $entries -gt 1000000) {
    throw "Every entry size must be in the range 1..1000000"
  }
}
foreach ($payload in $PayloadBytes) {
  if ($payload -lt 0 -or $payload -gt 8388608) {
    throw "Every payload size must be in the range 0..8388608"
  }
}
foreach ($group in $GroupSizes) {
  if ($group -lt 2 -or $group -gt 1024) {
    throw "Every group size must be in the range 2..1024"
  }
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$raw = Join-Path $OutputDirectory "raw-includes-nondurable.jsonl"
$errors = Join-Path $OutputDirectory "stderr.log"
$matrix = Join-Path $OutputDirectory "matrix-manifest.json"
$summary = Join-Path $OutputDirectory "summary-includes-nondurable.csv"
$figures = Join-Path $OutputDirectory "wal-figures-includes-nondurable.svg"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($raw, "", $utf8NoBom)
[System.IO.File]::WriteAllText($errors, "", $utf8NoBom)
Remove-Item -LiteralPath $summary, $figures -Force -ErrorAction SilentlyContinue

$variantsPerCell = 2 + $GroupSizes.Count
$expectedRuns = $EntrySizes.Count * $PayloadBytes.Count * $Repetitions * $variantsPerCell
$matrixDescription = [ordered]@{
  schema = "detlog-wal-bench-matrix/v1"
  generated_at = (Get-Date).ToString("o")
  executable = $Executable
  repetitions = $Repetitions
  entry_sizes = @($EntrySizes)
  payload_bytes = @($PayloadBytes)
  policies = @("flush-every", "group", "unsafe-no-flush")
  group_sizes = @($GroupSizes)
  expected_runs = $expectedRuns
  raw_jsonl = "raw-includes-nondurable.jsonl"
  diagnostics = "stderr.log"
  derived_csv = "summary-includes-nondurable.csv"
  derived_svg = "wal-figures-includes-nondurable.svg"
  unsafe_policy_note = "UNSAFE: append acknowledgements are nondurable; each run performs and times an explicit final flush before reopen recovery."
}
$matrixJson = $matrixDescription | ConvertTo-Json -Depth 4
[System.IO.File]::WriteAllText($matrix, $matrixJson + [Environment]::NewLine, $utf8NoBom)

$runIndex = 0
function Invoke-WalBenchmark {
  param(
    [long]$Entries,
    [long]$Payload,
    [int]$Trial,
    [string]$Policy,
    [object]$GroupSize
  )
  $script:runIndex++
  $arguments = @(
    "--entries", $Entries,
    "--payload", $Payload,
    "--trial", $Trial,
    "--policy", $Policy
  )
  $description = "[$script:runIndex/$expectedRuns] policy=$Policy entries=$Entries payload=$Payload trial=$Trial"
  if ($null -ne $GroupSize) {
    $arguments += @("--group-size", $GroupSize)
    $description += " group=$GroupSize"
  }
  Write-Host $description
  $startInfo = New-Object System.Diagnostics.ProcessStartInfo
  $startInfo.FileName = $Executable
  $startInfo.Arguments = ($arguments -join " ")
  $startInfo.UseShellExecute = $false
  $startInfo.CreateNoWindow = $true
  $startInfo.RedirectStandardOutput = $true
  $startInfo.RedirectStandardError = $true
  $process = New-Object System.Diagnostics.Process
  $process.StartInfo = $startInfo
  if (-not $process.Start()) {
    throw "Could not start WAL benchmark executable: $Executable"
  }
  $stdout = $process.StandardOutput.ReadToEnd()
  $stderrText = $process.StandardError.ReadToEnd()
  $process.WaitForExit()
  $exitCode = $process.ExitCode
  $process.Dispose()
  if (-not [string]::IsNullOrEmpty($stdout)) {
    [System.IO.File]::AppendAllText($raw, $stdout, $utf8NoBom)
  }
  if (-not [string]::IsNullOrEmpty($stderrText)) {
    [System.IO.File]::AppendAllText($errors, $stderrText, $utf8NoBom)
  }
  if ($exitCode -ne 0) {
    throw "WAL benchmark failed with exit code $exitCode; artifacts were preserved in $OutputDirectory"
  }
}

foreach ($entries in $EntrySizes) {
  foreach ($payload in $PayloadBytes) {
    for ($trial = 1; $trial -le $Repetitions; $trial++) {
      Invoke-WalBenchmark -Entries $entries -Payload $payload -Trial $trial `
        -Policy "flush-every" -GroupSize $null
      foreach ($group in $GroupSizes) {
        Invoke-WalBenchmark -Entries $entries -Payload $payload -Trial $trial `
          -Policy "group" -GroupSize $group
      }
      Invoke-WalBenchmark -Entries $entries -Payload $payload -Trial $trial `
        -Policy "unsafe-no-flush" -GroupSize $null
    }
  }
}

& $Python (Join-Path $PSScriptRoot "plot_wal_bench.py") $raw `
  --csv $summary --svg $figures
if ($LASTEXITCODE -ne 0) {
  throw "WAL plot generation failed; raw results remain in $raw"
}
Write-Host "WAL benchmark artifacts: $OutputDirectory"
