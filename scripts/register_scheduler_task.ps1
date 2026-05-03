# Register three discrete MLB pipeline jobs as Windows Scheduled Tasks.
# Run once from an elevated (Administrator) PowerShell prompt.
#
# Replaces the old APScheduler daemon approach. Each job is an independent
# one-shot task — no daemon to crash or keep alive overnight.
#
#   nightly_incremental   02:00 AM daily  — extract + transform + aggregate
#   standings_snapshot    03:00 AM daily  — recompute gold.standings_snap
#   roster_sync           06:00 AM daily  — pull rosters + player bios
#
# Useful commands after registration:
#   Start-ScheduledTask -TaskName "MLB-NightlyIncremental"      # run now
#   Get-ScheduledTaskInfo -TaskName "MLB-NightlyIncremental"    # last run / next run
#   Unregister-ScheduledTask -TaskName "MLB-NightlyIncremental" -Confirm:$false

$ErrorActionPreference = "Stop"

$projectRoot  = "C:\Users\metsy\dev\development\active-projects\mlb-data-db-fabric"
$runJobScript = Join-Path $projectRoot "scripts\run_job.ps1"
$psExe        = "powershell.exe"

# ── 0. Remove old NSSM daemon service if present ─────────────────────────────

$nssmServiceName = "MLB-Pipeline-Scheduler"
$existingSvc = Get-Service -Name $nssmServiceName -ErrorAction SilentlyContinue
if ($existingSvc) {
    Write-Host "Removing old NSSM daemon service '$nssmServiceName'..." -ForegroundColor Yellow
    if ($existingSvc.Status -eq "Running") { Stop-Service $nssmServiceName -Force }
    $nssmExe = (Get-Command nssm -ErrorAction SilentlyContinue)?.Source
    if ($nssmExe) {
        & $nssmExe remove $nssmServiceName confirm | Out-Null
    } else {
        sc.exe delete $nssmServiceName | Out-Null
    }
    Start-Sleep -Seconds 2
    Write-Host "  Removed." -ForegroundColor Gray
}

# ── Shared helpers ────────────────────────────────────────────────────────────

function Register-MlbTask {
    param(
        [string]$TaskName,
        [string]$JobName,
        [string]$TriggerTime,
        [string]$Description
    )

    $psArgs = "-NonInteractive -ExecutionPolicy Bypass -File `"$runJobScript`" -Job $JobName"

    $action = New-ScheduledTaskAction `
        -Execute          $psExe `
        -Argument         $psArgs `
        -WorkingDirectory $projectRoot

    $trigger = New-ScheduledTaskTrigger -Daily -At $TriggerTime

    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit        (New-TimeSpan -Hours 2) `
        -RestartCount              2 `
        -RestartInterval           (New-TimeSpan -Minutes 5) `
        -RunOnlyIfNetworkAvailable `
        -StartWhenAvailable `
        -MultipleInstances         IgnoreNew

    # S4U — runs as current user without storing password, works when logged off
    $principal = New-ScheduledTaskPrincipal `
        -UserId    "$env:USERDOMAIN\$env:USERNAME" `
        -LogonType S4U `
        -RunLevel  Highest

    $task = Register-ScheduledTask `
        -TaskName    $TaskName `
        -Description $Description `
        -Action      $action `
        -Trigger     $trigger `
        -Settings    $settings `
        -Principal   $principal `
        -Force

    Write-Host "  Registered '$TaskName' — fires daily at $TriggerTime" -ForegroundColor Green
    return $task
}

# ── 1. Nightly Incremental (02:00 AM) ─────────────────────────────────────────

Write-Host ""
Write-Host "Registering MLB pipeline scheduled tasks..." -ForegroundColor Cyan

Register-MlbTask `
    -TaskName    "MLB-NightlyIncremental" `
    -JobName     "nightly_incremental" `
    -TriggerTime "02:00AM" `
    -Description "MLB: extract prior-day games, transform bronze→silver, aggregate silver→gold"

# ── 2. Standings Snapshot (03:00 AM) ──────────────────────────────────────────

Register-MlbTask `
    -TaskName    "MLB-StandingsSnapshot" `
    -JobName     "standings_snapshot" `
    -TriggerTime "03:00AM" `
    -Description "MLB: recompute gold.standings_snap from all final regular-season games"

# ── 3. Roster Sync (06:00 AM) ─────────────────────────────────────────────────

Register-MlbTask `
    -TaskName    "MLB-RosterSync" `
    -JobName     "roster_sync" `
    -TriggerTime "06:00AM" `
    -Description "MLB: pull 40-man rosters + player bios, re-run silver teams/players transforms"

# ── Summary ───────────────────────────────────────────────────────────────────

Write-Host ""
Write-Host "Done. Three tasks registered:" -ForegroundColor Cyan
Get-ScheduledTask -TaskPath "\" | Where-Object { $_.TaskName -like "MLB-*" } |
    ForEach-Object {
        $info = Get-ScheduledTaskInfo -TaskName $_.TaskName
        Write-Host ("  {0,-30} next: {1}" -f $_.TaskName, $info.NextRunTime)
    }

Write-Host ""
Write-Host "To run nightly manually (e.g. to backfill a missed date):"
Write-Host "  Start-ScheduledTask -TaskName 'MLB-NightlyIncremental'"
Write-Host "  # or with a specific date:"
Write-Host "  & '$runJobScript' -Job nightly_incremental -Date 2026-05-02"
Write-Host ""
Write-Host "Log files land in: $projectRoot\logs\<job>_<timestamp>.log"
