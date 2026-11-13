<#
    install-autostart.ps1 -- register a Windows scheduled task that starts the
    AgentRouter Codex compatibility proxy at logon.

    Semantics: LOGON autostart, not boot autostart. The task uses an
    Interactive principal and an at-logon trigger, so the proxy starts a few
    seconds after you log in and nothing runs while you are logged off.
    No elevation is needed.

    Usage:
      .\install-autostart.ps1
      .\install-autostart.ps1 -TaskName AgentRouterCodexProxy -Port 8788
      .\install-autostart.ps1 -WhatIf_        # show the plan, change nothing

    Remove it again with .\uninstall-autostart.ps1
#>

[CmdletBinding()]
param(
    [string]$TaskName = 'AgentRouterCodexProxy',
    [int]$Port = 8788,
    [int]$DelaySeconds = 8,
    [int]$MaxLogKB = 1024,
    [switch]$NoAutoUpdate,
    [switch]$WhatIf_
)

$ErrorActionPreference = 'Stop'

$root   = Split-Path -Parent $MyInvocation.MyCommand.Path
$script = Join-Path $root 'service.py'

if (-not (Test-Path $script)) {
    Write-Host "[fail ] service.py not found next to this script ($root)" -ForegroundColor Red
    exit 1
}

$account = "$env:USERDOMAIN\$env:USERNAME"

# Resolve GUI-subsystem pythonw.exe so Windows Task Scheduler never opens or
# flashes a console / Windows Terminal window when starting at logon.
$launcher = Join-Path $root '.venv\Scripts\pythonw.exe'
if (-not (Test-Path $launcher)) {
    $launcher = (Get-Command pythonw.exe -ErrorAction SilentlyContinue).Source
}
if (-not $launcher) {
    $pyCmd = (Get-Command python.exe -ErrorAction SilentlyContinue).Source
    if (-not $pyCmd) { $pyCmd = (Get-Command py.exe -ErrorAction SilentlyContinue).Source }
    if ($pyCmd) {
        $candidate = Join-Path (Split-Path -Parent $pyCmd) 'pythonw.exe'
        if (Test-Path $candidate) { $launcher = $candidate }
    }
}
if (-not $launcher -or -not (Test-Path $launcher)) {
    Write-Host '[fail ] pythonw.exe not found in .venv\Scripts or on PATH. Install Python 3.10+ first.' -ForegroundColor Red
    exit 1
}

$arguments = '"{0}" --port {1} --max-log-kb {2}' -f $script, $Port, $MaxLogKB
if ($NoAutoUpdate) { $arguments += ' --no-auto-update' }

Write-Host ''
Write-Host '  Scheduled task to be registered' -ForegroundColor White
Write-Host "  name      : $TaskName"        -ForegroundColor DarkGray
Write-Host "  account   : $account"          -ForegroundColor DarkGray
Write-Host "  trigger   : at logon, +${DelaySeconds}s delay" -ForegroundColor DarkGray
Write-Host "  logon     : Interactive -- starts only while you are logged in" -ForegroundColor DarkGray
Write-Host "  window    : headless background (pythonw.exe GUI subsystem, no console)" -ForegroundColor DarkGray
Write-Host "  action    : $launcher"         -ForegroundColor DarkGray
Write-Host "  arguments : $arguments"        -ForegroundColor DarkGray
Write-Host "  workdir   : $root"             -ForegroundColor DarkGray
Write-Host "  listen    : 127.0.0.1:$Port (loopback only)" -ForegroundColor DarkGray
Write-Host ''

if ($WhatIf_) {
    Write-Host '[ ok  ] -WhatIf_ given: nothing was changed.' -ForegroundColor Yellow
    exit 0
}

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "[warn ] a task named '$TaskName' already exists -- it will be replaced." -ForegroundColor Yellow
}

$action = New-ScheduledTaskAction -Execute $launcher -Argument $arguments -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $account
$trigger.Delay = "PT${DelaySeconds}S"
$principal = New-ScheduledTaskPrincipal -UserId $account -LogonType Interactive -RunLevel Limited

$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries `
                                         -DontStopIfGoingOnBatteries `
                                         -StartWhenAvailable `
                                         -RestartCount 3 `
                                         -RestartInterval (New-TimeSpan -Minutes 1) `
                                         -ExecutionTimeLimit ([TimeSpan]::Zero) `
                                         -MultipleInstances IgnoreNew
$settings.Hidden = $true

Register-ScheduledTask -TaskName $TaskName `
                       -Action $action `
                       -Trigger $trigger `
                       -Principal $principal `
                       -Settings $settings `
                       -Force | Out-Null

$verify = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
if ($verify) {
    Write-Host "[ ok  ] Scheduled task '$TaskName' registered successfully." -ForegroundColor Green
    Write-Host "[info ] Starting task now..." -ForegroundColor Cyan
    Start-ScheduledTask -TaskName $TaskName
    Start-Sleep -Seconds 3

    # Check health
    try {
        $health = Invoke-RestMethod "http://127.0.0.1:$Port/_health" -TimeoutSec 5 -ErrorAction Stop
        if ($health.status -eq 'ok') {
            Write-Host "[ ok  ] Proxy is active and responding on http://127.0.0.1:$Port/_health" -ForegroundColor Green
        }
    } catch {
        Write-Host "[warn ] Proxy starting in background. Check 'logs/proxy.log' if not responding shortly." -ForegroundColor Yellow
    }
}
