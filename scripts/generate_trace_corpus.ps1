param(
  [string]$Executable = "",
  [string]$OutputDirectory = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Executable)) {
  $Executable = Join-Path $root "build\release\detlog-sim.exe"
}
if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
  $OutputDirectory = Join-Path $root "artifacts\traces"
}
if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
  throw "Simulator executable not found: $Executable"
}
if (Test-Path -LiteralPath $OutputDirectory) {
  $outputItem = Get-Item -LiteralPath $OutputDirectory -Force
  if (-not $outputItem.PSIsContainer -or
      ($outputItem.Attributes -band
       [System.IO.FileAttributes]::ReparsePoint)) {
    throw "Trace output must be a real directory: $OutputDirectory"
  }
  foreach ($existing in Get-ChildItem -LiteralPath $OutputDirectory -Force) {
    if ($existing.PSIsContainer -or
        ($existing.Attributes -band
         [System.IO.FileAttributes]::ReparsePoint) -or
        ($existing.Name -ne "manifest.json" -and
         $existing.Extension -ne ".jsonl")) {
      throw "Refusing to replace unrelated trace artifact: $($existing.FullName)"
    }
  }
}
$parent = Split-Path -Parent $OutputDirectory
if ([string]::IsNullOrWhiteSpace($parent)) { $parent = "." }
New-Item -ItemType Directory -Force -Path $parent | Out-Null
$staging = "$OutputDirectory.tmp-$([guid]::NewGuid().ToString('N'))"
$backup = "$OutputDirectory.previous-$([guid]::NewGuid().ToString('N'))"
New-Item -ItemType Directory -Path $staging | Out-Null

$cases = @(
  @{ scenario = "leader-crash"; nodes = 3; seed = 42; file = "leader-crash-restart-3node-seed42.jsonl" },
  @{ scenario = "leader-crash"; nodes = 5; seed = 84; file = "leader-crash-restart-5node-seed84.jsonl" },
  @{ scenario = "symmetric-partition"; nodes = 3; seed = 43; file = "symmetric-partition-3node-seed43.jsonl" },
  @{ scenario = "asymmetric-partition"; nodes = 5; seed = 44; file = "asymmetric-partition-5node-seed44.jsonl" },
  @{ scenario = "ambiguous-retry"; nodes = 3; seed = 45; file = "ambiguous-retry-3node-seed45.jsonl" },
  @{ scenario = "torn-wal"; nodes = 3; seed = 46; file = "torn-wal-3node-seed46.jsonl" },
  @{ scenario = "slow-follower"; nodes = 5; seed = 47; file = "slow-follower-5node-seed47.jsonl" },
  @{ scenario = "slow-disk"; nodes = 3; seed = 48; file = "slow-disk-3node-seed48.jsonl" },
  @{ scenario = "saturation"; nodes = 3; seed = 49; file = "saturation-3node-seed49.jsonl" }
)

$published = $false
try {
  $records = @()
  foreach ($case in $cases) {
    $name = $case.file
    $path = Join-Path $staging $name
    Write-Host "Generating $name"
    & $Executable --scenario $case.scenario --nodes $case.nodes `
      --seed $case.seed --trace $path
    if ($LASTEXITCODE -ne 0) { throw "Trace generation failed for $name" }
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
      throw "Trace generator did not produce a regular file: $name"
    }
    $traceItem = Get-Item -LiteralPath $path -Force
    if (($traceItem.Attributes -band
         [System.IO.FileAttributes]::ReparsePoint) -or
        $traceItem.Length -eq 0) {
      throw "Trace generator did not produce a nonempty regular file: $name"
    }
    $records += [ordered]@{
      scenario = $case.scenario
      nodes = $case.nodes
      seed = $case.seed
      file = $name
      sha256 = (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash.ToLowerInvariant()
    }
  }

  $buildCommit = if ([string]::IsNullOrWhiteSpace($env:DETLOG_BUILD_COMMIT)) {
    "not_provided"
  } else {
    $env:DETLOG_BUILD_COMMIT
  }
  $manifest = [ordered]@{
    schema = "detlog-trace-corpus/v1"
    generated_at = (Get-Date).ToString("o")
    executable = $Executable
    build_commit = $buildCommit
    traces = $records
  }
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $manifestJson = $manifest | ConvertTo-Json -Depth 4
  [System.IO.File]::WriteAllText(
    (Join-Path $staging "manifest.json"),
    $manifestJson + [Environment]::NewLine, $utf8NoBom)

  if (Test-Path -LiteralPath $OutputDirectory) {
    [System.IO.Directory]::Move($OutputDirectory, $backup)
  }
  [System.IO.Directory]::Move($staging, $OutputDirectory)
  $published = $true
  if (Test-Path -LiteralPath $backup) {
    Remove-Item -LiteralPath $backup -Recurse -Force
  }
} finally {
  if (-not $published -and (Test-Path -LiteralPath $backup) -and
      -not (Test-Path -LiteralPath $OutputDirectory)) {
    [System.IO.Directory]::Move($backup, $OutputDirectory)
  }
  if (Test-Path -LiteralPath $staging) {
    Remove-Item -LiteralPath $staging -Recurse -Force
  }
}
Write-Host "Trace corpus: $OutputDirectory"
