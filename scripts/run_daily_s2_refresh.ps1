param(
    [string]$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$Python = "C:\Users\User\AppData\Local\Programs\Python\Python312\python.exe",
    [string]$Git = ""
)

$ErrorActionPreference = "Stop"
$env:GIT_TERMINAL_PROMPT = "0"
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$logDir = Join-Path $RepoRoot "logs\s2_refresh"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logPath = Join-Path $logDir "daily_s2_refresh_$timestamp.log"
$envFile = Join-Path $RepoRoot ".env"
$today = Get-Date -Format "yyyy-MM-dd"

function Write-Log {
    param([string]$Message)
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $logPath -Value $line -Encoding UTF8
    Write-Output $line
}

function Write-ProcessOutput {
    process {
        $line = "$_"
        Add-Content -Path $logPath -Value $line -Encoding UTF8
        Write-Output $line
    }
}

function Resolve-GitExecutable {
    param([string]$RequestedGit)

    $candidates = @(
        $RequestedGit,
        (Get-Command git.exe -ErrorAction SilentlyContinue).Source,
        "C:\Program Files (x86)\Git\cmd\git.exe",
        "C:\Program Files\Git\cmd\git.exe",
        "C:\Program Files\Git\bin\git.exe"
    ) | Where-Object { $_ }

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    throw "git.exe was not found. Pass -Git or install Git in a standard location."
}

$GitExe = Resolve-GitExecutable $Git

function Invoke-Step {
    param(
        [string]$Name,
        [string[]]$Arguments
    )

    Write-Log "START $Name"
    Push-Location $RepoRoot
    $exitCode = 0
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $Python @Arguments 2>&1 | Write-ProcessOutput
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
        Pop-Location
    }
    if ($exitCode -ne 0) {
        throw "$Name failed with exit code $exitCode"
    }
    Write-Log "DONE $Name"
}

function Invoke-GitStep {
    param(
        [string]$Name,
        [string[]]$Arguments
    )

    Write-Log "START $Name"
    Push-Location $RepoRoot
    $exitCode = 0
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $GitExe @Arguments 2>&1 | Write-ProcessOutput
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
        Pop-Location
    }
    if ($exitCode -ne 0) {
        throw "$Name failed with exit code $exitCode"
    }
    Write-Log "DONE $Name"
}

function Publish-RefreshArtifacts {
    $docDir = Join-Path (Join-Path $RepoRoot "doc") $today
    $artifactPaths = @(
        (Join-Path $RepoRoot "data\kiss_payment_settlement_s2_lookup.csv"),
        (Join-Path $RepoRoot "data\s2_payment_missing_lookup.csv"),
        (Join-Path $RepoRoot "data\s2_billing_settlement_lookup.csv"),
        (Join-Path $RepoRoot "data\s2_sales_channel_content_lookup.csv"),
        (Join-Path $docDir "kiss_payment_settlement_refresh_summary.json"),
        (Join-Path $docDir "s2_reference_guards_refresh_summary.json"),
        (Join-Path $docDir "s2_sales_channel_contents_refresh_audit.csv"),
        (Join-Path $docDir "s2_sales_channel_contents_refresh_summary.json")
    ) | Where-Object { Test-Path $_ }

    if (-not $artifactPaths -or $artifactPaths.Count -eq 0) {
        throw "No S2 refresh artifacts found for git publish."
    }

    Invoke-GitStep "Git stage S2 refresh artifacts" (@("add", "--") + $artifactPaths)

    Push-Location $RepoRoot
    try {
        & $GitExe diff --cached --quiet --exit-code -- @artifactPaths
        $diffExitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($diffExitCode -eq 0) {
        Write-Log "No S2 refresh artifact changes to commit"
        return
    }
    if ($diffExitCode -ne 1) {
        throw "Git staged diff check failed with exit code $diffExitCode"
    }

    Invoke-GitStep "Git commit S2 refresh artifacts" (@("commit", "-m", "Refresh S2 lookup data $today", "--") + $artifactPaths)
    Invoke-GitStep "Git rebase latest main before push" @("pull", "--quiet", "--rebase", "origin", "main")
    Invoke-GitStep "Git push S2 refresh artifacts" @("push", "--quiet", "origin", "main")
}

try {
    Write-Log "Daily S2 refresh started"
    Write-Log "Using Git executable: $GitExe"

    if (-not (Test-Path $Python)) {
        throw "Python executable not found: $Python"
    }

    if (-not (Test-Path $envFile)) {
        throw ".env file not found: $envFile"
    }

    Invoke-GitStep "Git pull latest main" @("pull", "--quiet", "--ff-only", "origin", "main")

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
        "--lookup-only",
        "--page-size", "1000000",
        "--content-style-code", "102"
    )

    Invoke-Step "S2 reference guards refresh" @(
        "scripts\refresh_s2_reference_guards.py",
        "--env-file", $envFile,
        "--page-size", "1000000",
        "--content-style-code", "102"
    )

    Invoke-Step "S2 sales-channel contents refresh" @(
        "scripts\refresh_s2_sales_channel_contents.py",
        "--env-file", $envFile,
        "--content-style-code", "102"
    )

    Publish-RefreshArtifacts

    Write-Log "Daily S2 refresh finished"
    exit 0
}
catch {
    Write-Log "FAILED $($_.Exception.Message)"
    if ($_.ScriptStackTrace) {
        Write-Log "STACK $($_.ScriptStackTrace)"
    }
    exit 1
}
