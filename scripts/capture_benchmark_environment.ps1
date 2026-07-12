param(
  [string]$Output = "",
  [string]$Compiler = "C:\msys64\mingw64\bin\g++.exe",
  [string]$BuildFlags = "",
  [string]$StorageDescription = "not_provided",
  [string]$Notes = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($Output)) {
  $Output = Join-Path $root "bench-results\environment.json"
}

function Capture-Text([scriptblock]$Command) {
  try {
    $value = (& $Command | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($value)) { return "unavailable" }
    return $value
  } catch {
    return "unavailable: $($_.Exception.Message)"
  }
}

$cpu = Capture-Text {
  Get-ItemPropertyValue `
    -LiteralPath "HKLM:\HARDWARE\DESCRIPTION\System\CentralProcessor\0" `
    -Name ProcessorNameString
}
$os = Get-ItemProperty `
  -LiteralPath "HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion"
$memoryBytes = try {
  Add-Type -AssemblyName Microsoft.VisualBasic
  [uint64](New-Object Microsoft.VisualBasic.Devices.ComputerInfo).TotalPhysicalMemory
} catch {
  $null
}
$powerScheme = Capture-Text { powercfg /GETACTIVESCHEME }
$storageInventory = Capture-Text {
  $diskEnum = Get-ItemProperty -LiteralPath `
    "HKLM:\SYSTEM\CurrentControlSet\Services\disk\Enum"
  for ($index = 0; $index -lt [int]$diskEnum.Count; ++$index) {
    $diskEnum."$index"
  }
}
$compilerVersion = Capture-Text { & $Compiler --version }
$commit = Capture-Text { git -C $root rev-parse HEAD }
$worktreeLines = @(git -C $root status --porcelain=v1)
$worktree = if ($LASTEXITCODE -eq 0 -and $worktreeLines.Count -eq 0) {
  "clean"
} elseif ($LASTEXITCODE -eq 0) {
  ($worktreeLines -join "`n")
} else {
  "unknown"
}

$record = [ordered]@{
  schema = "detlog-benchmark-environment/v1"
  captured_at = (Get-Date).ToString("o")
  machine_scope = "single local Windows workstation"
  cpu = $cpu
  logical_processors = [int]$env:NUMBER_OF_PROCESSORS
  physical_memory_bytes = $memoryBytes
  os_product = $os.ProductName
  os_display_version = $os.DisplayVersion
  os_build = "$($os.CurrentBuild).$($os.UBR)"
  power_scheme = $powerScheme
  temp_directory = $env:TEMP
  storage_description = $StorageDescription
  storage_inventory = $storageInventory
  compiler_path = $Compiler
  compiler_version = $compilerVersion
  build_flags = $BuildFlags
  build_commit = $commit
  worktree = $worktree
  timezone = (Get-TimeZone).Id
  notes = $Notes
}

$parent = Split-Path -Parent $Output
if (-not [string]::IsNullOrWhiteSpace($parent)) {
  New-Item -ItemType Directory -Force -Path $parent | Out-Null
}
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$recordJson = $record | ConvertTo-Json -Depth 4
[System.IO.File]::WriteAllText(
  $Output, $recordJson + [Environment]::NewLine, $utf8NoBom)
Write-Host "Benchmark environment: $Output"
