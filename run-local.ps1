# Run import-hawkeye pipeline locally with real data
# Usage:
#   .\run-local.ps1 path\to\file.zip [-ShowData] [-Rows 20] [-V]
#   .\run-local.ps1 path\to\directory [-ShowData] [-Rows 20] [-V]

param(
    [Parameter(Mandatory=$true, Position=0)]
    [string]$Path,

    [switch]$ShowData,

    [int]$Rows = 10,

    [switch]$V
)

# Change to script directory so uv can find the app module
Set-Location $PSScriptRoot

# Check if path exists
if (-not (Test-Path $Path)) {
    Write-Host "Error: Path not found: $Path" -ForegroundColor Red
    exit 1
}

# Determine if it's a file or directory
$item = Get-Item $Path

if ($item.PSIsContainer) {
    # Directory - process all ZIP files
    $zipFiles = Get-ChildItem -Path $Path -Filter "*.zip"

    if ($zipFiles.Count -eq 0) {
        Write-Host "No ZIP files found in directory: $Path" -ForegroundColor Yellow
        exit 0
    }

    Write-Host "Found $($zipFiles.Count) ZIP file(s) in $Path" -ForegroundColor Cyan
    Write-Host ""

    $successful = 0
    $failed = 0

    foreach ($zipFile in $zipFiles) {
        Write-Host "=" * 80 -ForegroundColor Cyan
        Write-Host "Processing: $($zipFile.Name)" -ForegroundColor Cyan
        Write-Host "=" * 80 -ForegroundColor Cyan

        $args = @($zipFile.FullName)

        if ($ShowData) {
            $args += "-d"
            $args += "--rows"
            $args += $Rows
        }

        if ($V) {
            $args += "-v"
        }

        uv run python -m app.cli @args

        if ($LASTEXITCODE -eq 0) {
            $successful++
            Write-Host "`nSuccess: $($zipFile.Name)" -ForegroundColor Green
        } elseif ($LASTEXITCODE -eq 2) {
            $successful++
            Write-Host "`nPartial success: $($zipFile.Name)" -ForegroundColor Yellow
        } else {
            $failed++
            Write-Host "`nFailed: $($zipFile.Name)" -ForegroundColor Red
        }

        Write-Host ""
    }

    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host "SUMMARY" -ForegroundColor Cyan
    Write-Host "=" * 80 -ForegroundColor Cyan
    Write-Host "Total files: $($zipFiles.Count)"
    Write-Host "Successful: $successful" -ForegroundColor Green
    Write-Host "Failed: $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })

} else {
    # Single file
    if ($item.Extension -ne ".zip") {
        Write-Host "Error: File must be a ZIP file: $Path" -ForegroundColor Red
        exit 1
    }

    $args = @($item.FullName)

    if ($ShowData) {
        $args += "-d"
        $args += "--rows"
        $args += $Rows
    }

    if ($Debug) {
        $args += "-v"
    }

    Write-Host "Processing: $($item.Name)" -ForegroundColor Cyan
    uv run python -m app.cli @args

    exit $LASTEXITCODE
}
