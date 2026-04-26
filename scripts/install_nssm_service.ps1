# Install the MLB Pipeline Scheduler as a Windows Service via NSSM.
# Run once from an elevated (Administrator) PowerShell prompt.
#
# Why NSSM over Task Scheduler:
#   - Runs as SYSTEM - no user session required (survives logoff, lock screen, RDP disconnect)
#   - Built-in daily log rotation (no separate launcher wrapper needed)
#   - Service Manager integration: sc.exe, Get-Service, Event Viewer
#   - Auto-restart on any crash, not just on launch
#
# After installation:
#   Start-Service   MLB-Pipeline-Scheduler
#   Stop-Service    MLB-Pipeline-Scheduler
#   Get-Service     MLB-Pipeline-Scheduler
#   nssm log        MLB-Pipeline-Scheduler   # tail live output
#   nssm remove     MLB-Pipeline-Scheduler confirm
#
# Env vars are stamped directly onto the service from your .env file.
# Update them with:  nssm set MLB-Pipeline-Scheduler AppEnvironmentExtra "KEY=value"

$ErrorActionPreference = "Stop"

$projectRoot = "C:\Users\metsy\dev\development\active-projects\mlb-data-db-fabric"
$uvExe       = "C:\Users\metsy\.local\bin\uv.exe"
$serviceName = "MLB-Pipeline-Scheduler"
$logDir      = Join-Path $projectRoot "logs"
$dotEnvPath  = Join-Path $projectRoot ".env"

# ── 1. Locate or install NSSM ─────────────────────────────────────────────────

function Find-Nssm {
    $cmd = Get-Command nssm -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    $candidate = "C:\ProgramData\nssm\nssm.exe"
    if (Test-Path $candidate) { return $candidate }

    return $null
}

$nssmExe = Find-Nssm

if (-not $nssmExe) {
    Write-Host "NSSM not found - attempting install via winget..." -ForegroundColor Yellow

    $wingetOk = $false
    try {
        winget install --id NSSM.NSSM --silent --accept-source-agreements --accept-package-agreements
        $nssmExe = Find-Nssm
        if ($nssmExe) { $wingetOk = $true }
    } catch { }

    if (-not $wingetOk) {
        Write-Host "winget install failed - downloading NSSM 2.24 directly..." -ForegroundColor Yellow

        $nssmZip  = Join-Path $env:TEMP "nssm-2.24.zip"
        $nssmDir  = Join-Path $env:TEMP "nssm-2.24"
        $nssmDest = "C:\ProgramData\nssm"

        Invoke-WebRequest -Uri "https://nssm.cc/release/nssm-2.24.zip" -OutFile $nssmZip -UseBasicParsing
        Expand-Archive -Path $nssmZip -DestinationPath $nssmDir -Force

        New-Item -ItemType Directory -Force $nssmDest | Out-Null
        Copy-Item "$nssmDir\nssm-2.24\win64\nssm.exe" $nssmDest -Force
        $nssmExe = Join-Path $nssmDest "nssm.exe"

        # Add to system PATH permanently
        $sysPath = [Environment]::GetEnvironmentVariable("Path", "Machine")
        if ($sysPath -notlike "*$nssmDest*") {
            [Environment]::SetEnvironmentVariable("Path", "$sysPath;$nssmDest", "Machine")
        }
        $env:Path += ";$nssmDest"
    }
}

if (-not $nssmExe) {
    Write-Error "Could not locate or install NSSM. Download it manually from https://nssm.cc and re-run this script."
    exit 1
}

Write-Host "Using NSSM: $nssmExe" -ForegroundColor Cyan

# ── 2. Remove existing service if present ─────────────────────────────────────

$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing service '$serviceName'..." -ForegroundColor Yellow
    if ($existing.Status -eq "Running") { Stop-Service $serviceName -Force }
    & $nssmExe remove $serviceName confirm | Out-Null
    Start-Sleep -Seconds 2
}

# ── 3. Create service ─────────────────────────────────────────────────────────

Write-Host "Registering service '$serviceName'..." -ForegroundColor Cyan

& $nssmExe install $serviceName $uvExe
& $nssmExe set     $serviceName AppParameters      "run python -m src.scheduler.jobs"
& $nssmExe set     $serviceName AppDirectory       $projectRoot
& $nssmExe set     $serviceName DisplayName        "MLB Pipeline Scheduler"
& $nssmExe set     $serviceName Description        "APScheduler daemon: nightly_incremental, roster_sync, standings_snapshot"
& $nssmExe set     $serviceName Start              SERVICE_AUTO_START

# ── 4. Logging with daily rotation ────────────────────────────────────────────

New-Item -ItemType Directory -Force $logDir | Out-Null

& $nssmExe set $serviceName AppStdout           (Join-Path $logDir "service_scheduler.log")
& $nssmExe set $serviceName AppStderr           (Join-Path $logDir "service_scheduler_err.log")
& $nssmExe set $serviceName AppRotateFiles      1
& $nssmExe set $serviceName AppRotateOnline     1   # rotate while service is running
& $nssmExe set $serviceName AppRotateSeconds    86400
& $nssmExe set $serviceName AppRotateBytes      10485760  # also rotate at 10 MB

# ── 5. Restart policy ─────────────────────────────────────────────────────────

& $nssmExe set $serviceName AppExit Default Restart
& $nssmExe set $serviceName AppRestartDelay 60000   # 60-second cool-down before restart

# ── 6. Stamp environment variables from .env ──────────────────────────────────
#
# Running as SYSTEM means the user's .env / Azure CLI session are not available.
# We read .env and pass every non-empty variable directly to the service so that
# DefaultAzureCredential picks up AZURE_CLIENT_ID/SECRET/TENANT_ID and the
# Fabric connection details are available at runtime.

if (Test-Path $dotEnvPath) {
    Write-Host "Loading environment variables from .env..." -ForegroundColor Cyan

    $envVars = @()
    foreach ($line in Get-Content $dotEnvPath) {
        if ($line -match "^\s*#" -or $line -notmatch "=") { continue }
        $parts = $line -split "=", 2
        $key   = $parts[0].Trim()
        $val   = $parts[1].Trim()
        if ($key -and $val -and $val -ne "") {
            $envVars += "$key=$val"
        }
    }

    if ($envVars.Count -gt 0) {
        $envString = $envVars -join "`n"
        & $nssmExe set $serviceName AppEnvironmentExtra $envString
        Write-Host "  Set $($envVars.Count) environment variable(s) on service." -ForegroundColor Gray
    } else {
        Write-Warning ".env file found but contained no non-empty variables - ensure credentials are filled in before starting the service."
    }
} else {
    Write-Warning ".env not found at $dotEnvPath - service will not have Fabric/Azure credentials. Copy .env.example to .env and fill it in, then rerun this script."
}

# ── 7. Start the service ──────────────────────────────────────────────────────

Write-Host ""
Write-Host "Starting service..." -ForegroundColor Cyan
Start-Service $serviceName
Start-Sleep -Seconds 3

$svc = Get-Service $serviceName
Write-Host ""
if ($svc.Status -eq "Running") {
    Write-Host "Service is RUNNING." -ForegroundColor Green
} else {
    Write-Host "Service status: $($svc.Status)" -ForegroundColor Yellow
    Write-Host "Check logs at: $logDir"
}

Write-Host ""
Write-Host "Log files:"
Write-Host "  $logDir\service_scheduler.log"
Write-Host "  $logDir\service_scheduler_err.log"
