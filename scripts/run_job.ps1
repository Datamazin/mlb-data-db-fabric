# Job runner invoked by Windows Task Scheduler.
# Each scheduled task points here with -Job <name>.
#
# Usage:
#   run_job.ps1 -Job nightly_incremental
#   run_job.ps1 -Job nightly_incremental -Date 2026-05-02
#   run_job.ps1 -Job roster_sync
#   run_job.ps1 -Job standings_snapshot

param(
    [Parameter(Mandatory)][ValidateSet("nightly_incremental","roster_sync","standings_snapshot")]
    [string]$Job,

    [string]$Date = ""
)

$ErrorActionPreference = "Stop"

$projectRoot = "C:\Users\metsy\dev\development\active-projects\mlb-data-db-fabric"
$uvExe       = "C:\Users\metsy\.local\bin\uv.exe"

Set-Location $projectRoot

$logDir = Join-Path $projectRoot "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }

$ts      = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "${Job}_${ts}.log"

$jobArgs = @("run", "python", "-m", "src.scheduler.jobs", "--run", $Job)
if ($Date) { $jobArgs += @("--date", $Date) }

"[$(Get-Date -Format 'o')] Starting $Job" | Tee-Object -FilePath $logFile -Append

& $uvExe @jobArgs 2>&1 | Tee-Object -FilePath $logFile -Append
$exitCode = $LASTEXITCODE

"[$(Get-Date -Format 'o')] $Job finished — exit code $exitCode" | Tee-Object -FilePath $logFile -Append

exit $exitCode
