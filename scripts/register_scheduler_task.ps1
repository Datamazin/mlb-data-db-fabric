# Register the MLB Pipeline Scheduler as a Windows Scheduled Task.
# Run once from an elevated (Administrator) PowerShell prompt.
#
# What this sets up:
#   - Starts automatically 60 s after you log in (time for network/Azure auth)
#   - Daily 01:00 AM trigger restarts the daemon if it silently died
#   - Restarts up to 3 times on crash with a 1-minute gap between attempts
#   - Logs to logs\scheduler_<timestamp>.log (same as manual runs)
#
# Useful commands after registration:
#   Start-ScheduledTask  -TaskName "MLB-Pipeline-Scheduler"          # run now
#   Stop-ScheduledTask   -TaskName "MLB-Pipeline-Scheduler"          # stop daemon
#   Get-ScheduledTask    -TaskName "MLB-Pipeline-Scheduler"          # status
#   Unregister-ScheduledTask -TaskName "MLB-Pipeline-Scheduler" -Confirm:$false

$projectRoot = "C:\Users\metsy\dev\development\active-projects\mlb-data-db-fabric"
$uvExe       = "C:\Users\metsy\.local\bin\uv.exe"
$taskName    = "MLB-Pipeline-Scheduler"

# ── Action ────────────────────────────────────────────────────────────────────
$action = New-ScheduledTaskAction `
    -Execute          $uvExe `
    -Argument         "run python -m src.scheduler.jobs" `
    -WorkingDirectory $projectRoot

# ── Triggers ──────────────────────────────────────────────────────────────────
$triggerLogon       = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$triggerLogon.Delay = "PT60S"

$triggerDaily = New-ScheduledTaskTrigger -Daily -At "01:00AM"

# ── Settings ──────────────────────────────────────────────────────────────────
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit    (New-TimeSpan -Days 3650) `
    -RestartCount          3 `
    -RestartInterval       (New-TimeSpan -Minutes 1) `
    -RunOnlyIfNetworkAvailable `
    -StartWhenAvailable `
    -MultipleInstances     IgnoreNew

# ── Principal ─────────────────────────────────────────────────────────────────
$principal = New-ScheduledTaskPrincipal `
    -UserId    "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType S4U `
    -RunLevel  Highest

# ── Register ──────────────────────────────────────────────────────────────────
$task = Register-ScheduledTask `
    -TaskName    $taskName `
    -Description "MLB Data Pipeline APScheduler daemon (nightly_incremental, roster_sync, standings_snapshot)" `
    -Action      $action `
    -Trigger     @($triggerLogon, $triggerDaily) `
    -Settings    $settings `
    -Principal   $principal `
    -Force

Write-Host ""
Write-Host "Registered: $($task.TaskName)" -ForegroundColor Green
Write-Host "Next run:   $($task.NextRunTime)"
Write-Host ""
Write-Host "Start it now:"
Write-Host "  Start-ScheduledTask -TaskName '$taskName'"
