# Deploy MLB Pipeline Scheduler as Azure Container Apps Jobs
#
# Creates three scheduled jobs that replace the local APScheduler daemon:
#   mlb-nightly        — 02:00 ET daily (Mar–Nov)  extract + transform + aggregate
#   mlb-roster-sync    — 06:00 ET daily             roster + player bio sync
#   mlb-standings-snap — 03:00 ET daily (Apr–Oct)   standings snapshot
#
# Prerequisites:
#   - deploy-to-azure.ps1 already run (ACR image + Container Apps environment exist)
#   - az CLI logged in
#   - .env populated with valid credentials

param(
    [string]$ResourceGroup  = "mlb-analytics-rg",
    [string]$AcrName        = "mlbanalyticsacr",
    [string]$EnvName        = "mlb-analytics-env",
    [string]$Location       = "eastus"
)

Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Deploying MLB Scheduler as Container Apps Jobs" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""

# ── Load .env ─────────────────────────────────────────────────────────────────
if (-not (Test-Path .env)) {
    Write-Host "[ERROR] .env file not found" -ForegroundColor Red
    exit 1
}
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)\s*=\s*(.*)$') {
        Set-Item -Path "env:$($matches[1].Trim())" -Value $matches[2].Trim()
    }
}

$required = @(
    'FABRIC_SERVER','FABRIC_DATABASE','FABRIC_AUTH',
    'AZURE_CLIENT_ID','AZURE_CLIENT_SECRET','AZURE_TENANT_ID',
    'ONELAKE_WORKSPACE_ID','ONELAKE_WORKSPACE_NAME','ONELAKE_LAKEHOUSE_NAME'
)
$missing = $required | Where-Object { -not (Test-Path "env:$_") -or (Get-Item "env:$_").Value -eq '' }
if ($missing) {
    Write-Host "[ERROR] Missing or empty .env vars: $($missing -join ', ')" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Configuration loaded" -ForegroundColor Green
Write-Host ""

$image = "$AcrName.azurecr.io/mlb-analytics:latest"

# Shared env-var + secret args passed to every job
# Secrets are referenced so the values never appear in CLI history
$secrets = @(
    "azure-client-secret=$env:AZURE_CLIENT_SECRET"
)

$envVars = @(
    "FABRIC_SERVER=$env:FABRIC_SERVER",
    "FABRIC_DATABASE=$env:FABRIC_DATABASE",
    "FABRIC_AUTH=$env:FABRIC_AUTH",
    "AZURE_CLIENT_ID=$env:AZURE_CLIENT_ID",
    "AZURE_TENANT_ID=$env:AZURE_TENANT_ID",
    "AZURE_CLIENT_SECRET=secretref:azure-client-secret",
    "ONELAKE_WORKSPACE_ID=$env:ONELAKE_WORKSPACE_ID",
    "ONELAKE_WORKSPACE_NAME=$env:ONELAKE_WORKSPACE_NAME",
    "ONELAKE_LAKEHOUSE_NAME=$env:ONELAKE_LAKEHOUSE_NAME",
    "MLB_API_BASE_URL=$env:MLB_API_BASE_URL",
    "MLB_API_RATE_LIMIT_RPS=$env:MLB_API_RATE_LIMIT_RPS",
    "MLB_API_MAX_RETRIES=$env:MLB_API_MAX_RETRIES",
    "LOG_LEVEL=$env:LOG_LEVEL"
)

# ── Helper: create or update a scheduled job ──────────────────────────────────
function Deploy-Job {
    param(
        [string]$JobName,
        [string]$CronExpression,   # UTC cron (Container Apps uses UTC)
        [string]$JobArg,           # --run argument for src.scheduler.jobs
        [string]$Description,
        [int]   $TimeoutSeconds = 3600
    )

    Write-Host "Deploying $JobName ($Description) ..." -ForegroundColor Cyan
    Write-Host "  Cron (UTC): $CronExpression"

    $exists = az containerapp job show --name $JobName --resource-group $ResourceGroup 2>$null
    $cmd = if ($exists) { "update" } else { "create" }

    if ($cmd -eq "create") {
        az containerapp job create `
            --name $JobName `
            --resource-group $ResourceGroup `
            --environment $EnvName `
            --trigger-type "Schedule" `
            --cron-expression $CronExpression `
            --replica-timeout $TimeoutSeconds `
            --replica-retry-limit 1 `
            --image $image `
            --registry-server "$AcrName.azurecr.io" `
            --cpu 1.0 --memory 2.0Gi `
            --secrets $secrets `
            --env-vars $envVars `
            --command "uv" "run" "python" "-m" "src.scheduler.jobs" "--run" $JobArg `
            --output table
    } else {
        az containerapp job update `
            --name $JobName `
            --resource-group $ResourceGroup `
            --image $image `
            --cpu 1.0 --memory 2.0Gi `
            --replica-timeout $TimeoutSeconds `
            --secrets $secrets `
            --env-vars $envVars `
            --output table
    }

    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Failed to deploy $JobName" -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] $JobName deployed" -ForegroundColor Green
    Write-Host ""
}

# ── Job 1: Nightly Incremental ─────────────────────────────────────────────────
# 02:00 ET = 06:00 UTC (EST) / 07:00 UTC (EDT)
# Using 07:00 UTC covers EDT (Mar–Nov); safe because the job handles yesterday's date
Deploy-Job `
    -JobName        "mlb-nightly" `
    -CronExpression "0 7 * * *" `
    -JobArg         "nightly_incremental" `
    -Description    "Extract + transform + aggregate prior-day games" `
    -TimeoutSeconds 3600

# ── Job 2: Roster Sync ────────────────────────────────────────────────────────
# 06:00 ET = 10:00 UTC
Deploy-Job `
    -JobName        "mlb-roster-sync" `
    -CronExpression "0 10 * * *" `
    -JobArg         "roster_sync" `
    -Description    "Sync 40-man rosters and player bios" `
    -TimeoutSeconds 1800

# ── Job 3: Standings Snapshot ─────────────────────────────────────────────────
# 03:00 ET = 07:00 UTC — run after nightly completes
Deploy-Job `
    -JobName        "mlb-standings-snap" `
    -CronExpression "30 7 * * *" `
    -JobArg         "standings_snapshot" `
    -Description    "Recompute standings snapshot" `
    -TimeoutSeconds 900

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "=================================================" -ForegroundColor Green
Write-Host "           SCHEDULER JOBS DEPLOYED!" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Jobs (all times ET):" -ForegroundColor Cyan
Write-Host "  mlb-nightly        — 02:00 ET daily  (nightly_incremental)"
Write-Host "  mlb-standings-snap — 02:30 ET daily  (standings_snapshot)"
Write-Host "  mlb-roster-sync    — 06:00 ET daily  (roster_sync)"
Write-Host ""
Write-Host "Monitor runs:" -ForegroundColor Cyan
Write-Host "  az containerapp job execution list --name mlb-nightly --resource-group $ResourceGroup --output table"
Write-Host "  az containerapp job execution list --name mlb-roster-sync --resource-group $ResourceGroup --output table"
Write-Host "  az containerapp job execution list --name mlb-standings-snap --resource-group $ResourceGroup --output table"
Write-Host ""
Write-Host "Trigger manually (e.g. backfill a missed run):" -ForegroundColor Cyan
Write-Host "  az containerapp job start --name mlb-nightly --resource-group $ResourceGroup"
Write-Host ""
Write-Host "View logs for latest run:" -ForegroundColor Cyan
Write-Host "  `$run = az containerapp job execution list --name mlb-nightly --resource-group $ResourceGroup --query '[0].name' -o tsv"
Write-Host "  az containerapp job execution logs show --name mlb-nightly --resource-group $ResourceGroup --execution `$run --follow"
Write-Host ""
