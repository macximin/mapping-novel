param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$Python = "C:\Users\User\AppData\Local\Programs\Python\Python312\python.exe"
)

$ErrorActionPreference = "Stop"

$logDir = Join-Path $RepoRoot "logs\s2_refresh"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logDir "daily_s2_refresh_$timestamp.log"
$envFile = Join-Path $RepoRoot ".env"

function Write-Log {
    param([string]$Message)
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $logPath -Value $line -Encoding UTF8
    Write-Output $line
}

function Invoke-Step {
    param(
        [string]$Name,
        [string[]]$Arguments
    )

    Write-Log "START $Name"
    Push-Location $RepoRoot
    try {
        & $Python @Arguments 2>&1 | Tee-Object -FilePath $logPath -Append
        if ($LASTEXITCODE -ne 0) {
            throw "$Name failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
    Write-Log "DONE $Name"
}

Write-Log "Daily S2 refresh started"

if (-not (Test-Path $envFile)) {
    throw ".env file not found: $envFile"
}

Invoke-Step "S2 auth check" @(
    "scripts\refresh_kiss_payment_settlement.py",
    "--env-file", $envFile,
    "--check-auth-only",
    "--auth-timeout", "10"
)

Invoke-Step "S2 payment settlement full replace" @(
    "scripts\refresh_kiss_payment_settlement.py",
    "--env-file", $envFile,
    "--mode", "full-replace",
    "--page-size", "1000000",
    "--content-style-code", "102"
)

Invoke-Step "S2 reference guards refresh" @(
    "scripts\refresh_s2_reference_guards.py",
    "--env-file", $envFile,
    "--page-size", "1000000",
    "--content-style-code", "102"
)

Write-Log "Daily S2 refresh finished"
