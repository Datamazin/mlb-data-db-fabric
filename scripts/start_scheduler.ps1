# MLB Pipeline Scheduler launcher — invoked by Windows Task Scheduler.
# Starts the APScheduler daemon and tees stdout/stderr to a timestamped log file.

$ErrorActionPreference = "Stop"

$projectRoot = "C:\Users\metsy\dev\development\active-projects\mlb-data-db-fabric"
$uvExe       = "C:\Users\metsy\.local\bin\uv.exe"

Set-Location $projectRoot

$ts      = Get-Date -Format "yyyyMMdd_HHmmss"
$logDir  = Join-Path $projectRoot "logs"
$logFile = Join-Path $logDir "scheduler_$ts.log"

if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

& $uvExe run python -m src.scheduler.jobs 2>&1 | Tee-Object -FilePath $logFile -Append
