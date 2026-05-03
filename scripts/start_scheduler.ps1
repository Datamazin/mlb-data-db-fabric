# Manually trigger one or all MLB pipeline jobs.
# Run from any PowerShell prompt — no elevated rights needed.
#
# Usage:
#   .\start_scheduler.ps1                         # run all three jobs in sequence
#   .\start_scheduler.ps1 -Job nightly_incremental
#   .\start_scheduler.ps1 -Job nightly_incremental -Date 2026-05-02

param(
    [ValidateSet("nightly_incremental","roster_sync","standings_snapshot","all")]
    [string]$Job = "all",

    [string]$Date = ""
)

$projectRoot  = "C:\Users\metsy\dev\development\active-projects\mlb-data-db-fabric"
$runJobScript = Join-Path $projectRoot "scripts\run_job.ps1"

function Invoke-Job([string]$name) {
    $extraArgs = @()
    if ($Date) { $extraArgs = @("-Date", $Date) }
    & $runJobScript -Job $name @extraArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "$name exited with code $LASTEXITCODE"
    }
}

if ($Job -eq "all") {
    Invoke-Job "nightly_incremental"
    Invoke-Job "standings_snapshot"
    Invoke-Job "roster_sync"
} else {
    Invoke-Job $Job
}
