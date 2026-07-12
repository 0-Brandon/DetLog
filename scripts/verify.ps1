param(
  [string]$Compiler = "g++",
  [ValidateSet("debug", "release")]
  [string]$Configuration = "debug"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$output = Join-Path $root "out/manual-$Configuration"
New-Item -ItemType Directory -Force -Path $output | Out-Null

$flags = @(
  "-std=c++20",
  "-Wall",
  "-Wextra",
  "-Wpedantic",
  "-Wconversion",
  "-Wsign-conversion",
  "-Wshadow",
  "-I$(Join-Path $root 'include')"
)
if ($Configuration -eq "debug") {
  $flags += @("-O0", "-g")
} else {
  $flags += @("-O3", "-DNDEBUG")
}

$sources = Get-ChildItem -LiteralPath (Join-Path $root "src") -Filter "*.cpp" |
  ForEach-Object { $_.FullName }
$tests = Get-ChildItem -LiteralPath (Join-Path $root "tests") -Filter "*_tests.cpp"
$linkFlags = @()
if ($env:OS -eq "Windows_NT") {
  $linkFlags += "-lws2_32"
} else {
  $linkFlags += "-pthread"
}

if ($tests.Count -eq 0) {
  throw "No standalone test files were found."
}

foreach ($test in $tests) {
  $executable = Join-Path $output ($test.BaseName + ".exe")
  Write-Host "Building $($test.Name)"
  & $Compiler @flags @sources $test.FullName @linkFlags "-o" $executable
  if ($LASTEXITCODE -ne 0) {
    throw "Compilation failed for $($test.Name)"
  }

  Write-Host "Running $($test.Name)"
  & $executable
  if ($LASTEXITCODE -ne 0) {
    throw "Tests failed in $($test.Name)"
  }
}

$scenarioFuzz = Join-Path $root "fuzz\scenario_fuzz.cpp"
if (Test-Path -LiteralPath $scenarioFuzz -PathType Leaf) {
  $executable = Join-Path $output "scenario_fuzz_smoke.exe"
  Write-Host "Building scenario_fuzz_smoke"
  & $Compiler @flags @sources $scenarioFuzz @linkFlags `
    "-DDETLOG_SCENARIO_FUZZ_STANDALONE=1" "-o" $executable
  if ($LASTEXITCODE -ne 0) {
    throw "Compilation failed for scenario_fuzz_smoke"
  }
  Write-Host "Running scenario_fuzz_smoke"
  & $executable
  if ($LASTEXITCODE -ne 0) {
    throw "Tests failed in scenario_fuzz_smoke"
  }
}

Write-Host "Running checkpoint_runner_tests.py"
& python (Join-Path $root "tests\checkpoint_runner_tests.py")
if ($LASTEXITCODE -ne 0) {
  throw "Tests failed in checkpoint_runner_tests.py"
}

Write-Host "All standalone tests passed."
