# =============================================================================
# setup_task_scheduler.ps1
#
# Registers three Windows Task Scheduler tasks for the MLB pipeline.
# Run once as Administrator; tasks survive reboots/sleep without a daemon.
#
# Tasks created:
#   MLB-Nightly-Incremental   02:00 ET daily (+ run on missed start)
#   MLB-Roster-Sync           06:00 ET daily (+ run on missed start)
#   MLB-Standings-Snapshot    03:00 ET daily, Apr-Oct (+ run on missed start)
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File scripts\setup_task_scheduler.ps1
# =============================================================================

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$UvExe       = (Get-Command uv -ErrorAction Stop).Source
$LogDir      = Join-Path $ProjectRoot "logs"

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory $LogDir | Out-Null }

function Register-MLBTask {
    param(
        [string]$TaskName,
        [string]$JobArg,
        [string]$Description,
        [object]$Trigger
    )

    $logFile = Join-Path $LogDir "task_$($TaskName.ToLower() -replace '-','_').log"

    # Wrap in cmd /c so stdout+stderr both land in the log file
    $action = New-ScheduledTaskAction `
        -Execute "cmd.exe" `
        -Argument "/c `"$UvExe run python -m src.scheduler.jobs --run $JobArg >> `"$logFile`" 2>&1`"" `
        -WorkingDirectory $ProjectRoot

    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
        -MultipleInstances IgnoreNew

    $principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME `
        -LogonType Interactive `
        -RunLevel Highest

    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }

    Register-ScheduledTask `
        -TaskName    $TaskName `
        -Description $Description `
        -Action      $action `
        -Trigger     $Trigger `
        -Settings    $settings `
        -Principal   $principal | Out-Null

    Write-Host "Registered: $TaskName"
}

# Nightly Incremental — 2:00 AM daily
Register-MLBTask `
    -TaskName   "MLB-Nightly-Incremental" `
    -JobArg     "nightly_incremental" `
    -Description "MLB pipeline: extract yesterday's games, transform, aggregate." `
    -Trigger    (New-ScheduledTaskTrigger -Daily -At "02:00")

# Roster Sync — 6:00 AM daily
Register-MLBTask `
    -TaskName   "MLB-Roster-Sync" `
    -JobArg     "roster_sync" `
    -Description "MLB pipeline: sync teams, players, venues, and seasons." `
    -Trigger    (New-ScheduledTaskTrigger -Daily -At "06:00")

# Standings Snapshot — 3:00 AM daily
Register-MLBTask `
    -TaskName   "MLB-Standings-Snapshot" `
    -JobArg     "standings_snapshot" `
    -Description "MLB pipeline: recompute gold standings snapshot." `
    -Trigger    (New-ScheduledTaskTrigger -Daily -At "03:00")

Write-Host ""
Write-Host "Done. Tasks registered:"
Get-ScheduledTask -TaskName "MLB-*" | Format-Table TaskName, State -AutoSize
Write-Host ""
Write-Host "Key setting: StartWhenAvailable = true"
Write-Host "If the machine was off at 2 AM, the task runs as soon as it wakes up."
