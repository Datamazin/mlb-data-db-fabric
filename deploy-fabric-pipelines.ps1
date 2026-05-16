# Deploy MLB pipeline notebooks + Data Pipelines to Microsoft Fabric
#
# What this does:
#   1. Uploads three notebooks to your Fabric workspace (Files/mlb_pipeline/)
#   2. Creates three Data Pipelines with Notebook activities
#   3. Sets schedules on each pipeline:
#        mlb-nightly        — 02:00 ET daily
#        mlb-standings-snap — 03:00 ET daily (Apr-Oct)
#        mlb-roster-sync    — 06:00 ET daily
#
# Prerequisites:
#   - az CLI logged in: az login
#   - .env populated (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID,
#                     ONELAKE_WORKSPACE_ID = your Fabric workspace ID)
#   - Repo synced to OneLake: upload src/ to
#     Files/mlb_pipeline/ in your mlb_bronze Lakehouse (one-time manual step)

param(
    [string]$WorkspaceId = "",   # Overrides ONELAKE_WORKSPACE_ID from .env
    [string]$EndDate     = "2099-12-31T23:59:00"
)

Write-Host "=================================================" -ForegroundColor Green
Write-Host "  Deploying MLB Fabric Data Pipelines" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""

# ── Load .env ─────────────────────────────────────────────────────────────────
if (-not (Test-Path .env)) { Write-Host "[ERROR] .env not found" -ForegroundColor Red; exit 1 }
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)\s*=\s*(.*)$') {
        $n = $matches[1].Trim(); $v = $matches[2].Trim()
        if (-not (Test-Path "env:$n") -or (Get-Item "env:$n").Value -eq '') {
            Set-Item -Path "env:$n" -Value $v
        }
    }
}

if ($WorkspaceId -eq '') { $WorkspaceId = $env:ONELAKE_WORKSPACE_ID }
if ($WorkspaceId -eq '') {
    Write-Host "[ERROR] WorkspaceId not set. Pass -WorkspaceId or set ONELAKE_WORKSPACE_ID in .env" -ForegroundColor Red
    exit 1
}

# ── Get Fabric bearer token via service principal ─────────────────────────────
Write-Host "Acquiring Fabric bearer token..." -ForegroundColor Cyan
$tokenResponse = Invoke-RestMethod `
    -Uri "https://login.microsoftonline.com/$env:AZURE_TENANT_ID/oauth2/v2.0/token" `
    -Method POST `
    -Body @{
        grant_type    = "client_credentials"
        client_id     = $env:AZURE_CLIENT_ID
        client_secret = $env:AZURE_CLIENT_SECRET
        scope         = "https://api.fabric.microsoft.com/.default"
    }
$token = $tokenResponse.access_token
if (-not $token) { Write-Host "[ERROR] Failed to acquire token" -ForegroundColor Red; exit 1 }
$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }
Write-Host "[OK] Token acquired" -ForegroundColor Green
Write-Host ""

$fabricApi = "https://api.fabric.microsoft.com/v1"

# ── Helper: upload a notebook ─────────────────────────────────────────────────
function Deploy-Notebook {
    param([string]$DisplayName, [string]$NotebookPath)

    Write-Host "Uploading notebook: $DisplayName ..." -ForegroundColor Cyan

    $content = Get-Content $NotebookPath -Raw
    $b64     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($content))

    # Check if notebook already exists
    $existing = Invoke-RestMethod `
        -Uri "$fabricApi/workspaces/$WorkspaceId/items?type=Notebook" `
        -Headers $headers -Method GET
    $found = $existing.value | Where-Object { $_.displayName -eq $DisplayName }

    if ($found) {
        Write-Host "  Updating existing notebook $($found.id)..."
        Invoke-RestMethod `
            -Uri "$fabricApi/workspaces/$WorkspaceId/items/$($found.id)/updateDefinition" `
            -Headers $headers -Method POST `
            -Body (@{
                definition = @{
                    parts = @(@{
                        path        = "notebook-content.ipynb"
                        payload     = $b64
                        payloadType = "InlineBase64"
                    })
                }
            } | ConvertTo-Json -Depth 10) | Out-Null
        Write-Host "[OK] Updated $DisplayName ($($found.id))" -ForegroundColor Green
        return $found.id
    } else {
        $resp = Invoke-RestMethod `
            -Uri "$fabricApi/workspaces/$WorkspaceId/items" `
            -Headers $headers -Method POST `
            -Body (@{
                displayName = $DisplayName
                type        = "Notebook"
                definition  = @{
                    parts = @(@{
                        path        = "notebook-content.ipynb"
                        payload     = $b64
                        payloadType = "InlineBase64"
                    })
                }
            } | ConvertTo-Json -Depth 10)
        Write-Host "[OK] Created $DisplayName ($($resp.id))" -ForegroundColor Green
        return $resp.id
    }
}

# ── Helper: create or update a pipeline ──────────────────────────────────────
function Deploy-Pipeline {
    param([string]$DisplayName, [string]$NotebookId)

    Write-Host "Deploying pipeline: $DisplayName ..." -ForegroundColor Cyan

    $pipelineDef = @{
        name       = $DisplayName
        properties = @{
            activities = @(@{
                name           = "RunNotebook"
                type           = "TridentNotebook"
                typeProperties = @{
                    notebookId  = $NotebookId
                    workspaceId = $WorkspaceId
                }
            })
        }
    }
    $defJson = $pipelineDef | ConvertTo-Json -Depth 10
    $b64     = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($defJson))

    $existing = Invoke-RestMethod `
        -Uri "$fabricApi/workspaces/$WorkspaceId/items?type=DataPipeline" `
        -Headers $headers -Method GET
    $found = $existing.value | Where-Object { $_.displayName -eq $DisplayName }

    if ($found) {
        Write-Host "  Updating existing pipeline $($found.id)..."
        Invoke-RestMethod `
            -Uri "$fabricApi/workspaces/$WorkspaceId/items/$($found.id)/updateDefinition" `
            -Headers $headers -Method POST `
            -Body (@{
                definition = @{
                    parts = @(@{
                        path        = "pipeline-content.json"
                        payload     = $b64
                        payloadType = "InlineBase64"
                    })
                }
            } | ConvertTo-Json -Depth 10) | Out-Null
        Write-Host "[OK] Updated $DisplayName ($($found.id))" -ForegroundColor Green
        return $found.id
    } else {
        $resp = Invoke-RestMethod `
            -Uri "$fabricApi/workspaces/$WorkspaceId/items" `
            -Headers $headers -Method POST `
            -Body (@{
                displayName = $DisplayName
                type        = "DataPipeline"
                definition  = @{
                    parts = @(@{
                        path        = "pipeline-content.json"
                        payload     = $b64
                        payloadType = "InlineBase64"
                    })
                }
            } | ConvertTo-Json -Depth 10)
        Write-Host "[OK] Created $DisplayName ($($resp.id))" -ForegroundColor Green
        return $resp.id
    }
}

# ── Helper: set a daily schedule on a pipeline ────────────────────────────────
function Set-PipelineSchedule {
    param(
        [string]$PipelineId,
        [string]$PipelineName,
        [string]$StartDateTime,   # ISO local time, e.g. "2026-05-11T02:00:00"
        [string]$TimeZone = "Eastern Standard Time"
    )

    Write-Host "  Scheduling $PipelineName at $StartDateTime $TimeZone ..." -ForegroundColor Cyan

    # Remove existing schedules first
    $schedules = Invoke-RestMethod `
        -Uri "$fabricApi/workspaces/$WorkspaceId/items/$PipelineId/jobs/DefaultJob/schedules" `
        -Headers $headers -Method GET
    foreach ($s in $schedules.value) {
        Invoke-RestMethod `
            -Uri "$fabricApi/workspaces/$WorkspaceId/items/$PipelineId/jobs/DefaultJob/schedules/$($s.id)" `
            -Headers $headers -Method DELETE | Out-Null
    }

    $body = @{
        enabled       = $true
        configuration = @{
            startDateTime    = $StartDateTime
            endDateTime      = $EndDate
            localTimeZoneId  = $TimeZone
            type             = "Daily"
            interval         = 1
        }
    } | ConvertTo-Json -Depth 5

    Invoke-RestMethod `
        -Uri "$fabricApi/workspaces/$WorkspaceId/items/$PipelineId/jobs/DefaultJob/schedules" `
        -Headers $headers -Method POST -Body $body | Out-Null

    Write-Host "  [OK] Schedule set" -ForegroundColor Green
}

# ── Deploy notebooks ──────────────────────────────────────────────────────────
Write-Host "── Notebooks ─────────────────────────────────────" -ForegroundColor White
$nightlyNbId    = Deploy-Notebook "mlb_nightly_incremental"  "fabric/notebooks/mlb_nightly_incremental.ipynb"
$rosterNbId     = Deploy-Notebook "mlb_roster_sync"          "fabric/notebooks/mlb_roster_sync.ipynb"
$standingsNbId  = Deploy-Notebook "mlb_standings_snapshot"   "fabric/notebooks/mlb_standings_snapshot.ipynb"
Write-Host ""

# ── Deploy pipelines ──────────────────────────────────────────────────────────
Write-Host "── Pipelines ─────────────────────────────────────" -ForegroundColor White
$nightlyPipeId    = Deploy-Pipeline "mlb-nightly"        $nightlyNbId
$rosterPipeId     = Deploy-Pipeline "mlb-roster-sync"    $rosterNbId
$standingsPipeId  = Deploy-Pipeline "mlb-standings-snap" $standingsNbId
Write-Host ""

# ── Set schedules ─────────────────────────────────────────────────────────────
Write-Host "── Schedules ─────────────────────────────────────" -ForegroundColor White
Set-PipelineSchedule -PipelineId $nightlyPipeId    -PipelineName "mlb-nightly"        -StartDateTime "2026-05-11T02:00:00"
Set-PipelineSchedule -PipelineId $standingsPipeId  -PipelineName "mlb-standings-snap" -StartDateTime "2026-05-11T03:00:00"
Set-PipelineSchedule -PipelineId $rosterPipeId     -PipelineName "mlb-roster-sync"    -StartDateTime "2026-05-11T06:00:00"
Write-Host ""

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "=================================================" -ForegroundColor Green
Write-Host "        FABRIC PIPELINES DEPLOYED!" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Pipelines (ET, runs daily):" -ForegroundColor Cyan
Write-Host "  mlb-nightly        — 02:00 AM  extract + transform + aggregate"
Write-Host "  mlb-standings-snap — 03:00 AM  standings snapshot"
Write-Host "  mlb-roster-sync    — 06:00 AM  roster + player sync"
Write-Host ""
Write-Host "Monitor in Fabric:" -ForegroundColor Cyan
Write-Host "  Fabric portal → MLB workspace → Data Factory → your pipeline → Run history"
Write-Host ""
Write-Host "IMPORTANT — one-time manual step required:" -ForegroundColor Yellow
Write-Host "  Upload your repo source code to OneLake so the notebooks can import it:"
Write-Host "  Fabric portal → mlb_bronze Lakehouse → Files → upload folder as 'mlb_pipeline/'"
Write-Host "  (drag-and-drop the src/ folder and key files from this repo)"
Write-Host ""
