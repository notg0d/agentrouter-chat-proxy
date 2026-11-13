<#
    uninstall-autostart.ps1 -- stop and remove the scheduled task created by
    install-autostart.ps1.

    Usage:
      .\uninstall-autostart.ps1
      .\uninstall-autostart.ps1 -Mode combined
      .\uninstall-autostart.ps1 -TaskName AgentRouterCodexProxy -Port 8788
      .\uninstall-autostart.ps1 -KeepRunning     # unregister but leave it up
#>

[CmdletBinding()]
param(
    [string]$TaskName = 'AgentRouterCodexProxy',
    [int]$Port = 8788,
    [ValidateSet('responses', 'combined')]
    [string]$Mode = 'responses',
    [switch]$KeepRunning
)

$ErrorActionPreference = 'Stop'
if ($Mode -eq 'combined') {
    if (-not $PSBoundParameters.ContainsKey('Port')) { $Port = 8789 }
    if (-not $PSBoundParameters.ContainsKey('TaskName')) { $TaskName = 'AgentRouterCombinedProxy' }
}

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $task) {
    Write-Host "[ ok  ] no scheduled task named '$TaskName' -- nothing to remove." -ForegroundColor Green
} else {
    try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue } catch { }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "[ ok  ] removed scheduled task '$TaskName'" -ForegroundColor Green
}

if ($KeepRunning) {
    Write-Host '[ ok  ] -KeepRunning given: the running proxy was left alone.' -ForegroundColor Green
    exit 0
}

# Stop the listener only after confirming it is this proxy.
$conns = $null
try { $conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue } catch { }

if (-not $conns) {
    Write-Host "[ ok  ] nothing listening on 127.0.0.1:$Port" -ForegroundColor Green
    exit 0
}

$isOurs = $false
try {
    $h = Invoke-RestMethod "http://127.0.0.1:$Port/_health" -TimeoutSec 5 -ErrorAction Stop
    $required = if ($Mode -eq 'combined') { @('responses', 'chat.completions') } else { @('responses') }
    $missing = @($required | Where-Object { $_ -notin $h.supported_apis })
    if ($h.proxy -in @('agentrouter-codex-proxy', 'agentrouter-chat-proxy') -and
        $h.status -eq 'ok' -and $h.listen -eq "127.0.0.1:$Port" -and $missing.Count -eq 0) { $isOurs = $true }
} catch { }

$ownerPids = @($conns | Select-Object -ExpandProperty OwningProcess -Unique)

if (-not $isOurs) {
    Write-Host "[warn ] port $Port is held by a process that did not answer /_health as this proxy (PID $($ownerPids -join ', '))." -ForegroundColor Yellow
    Write-Host '[warn ] leaving it running and untouched.' -ForegroundColor Yellow
    exit 0
}

foreach ($procId in $ownerPids) {
    try {
        $procInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $procId" -ErrorAction SilentlyContinue
        if ($procInfo -and $procInfo.ParentProcessId) {
            $parentInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $($procInfo.ParentProcessId)" -ErrorAction SilentlyContinue
            if ($parentInfo -and "$($parentInfo.CommandLine)" -match 'service\.py|start-proxy\.ps1') {
                Stop-Process -Id $parentInfo.ProcessId -Force -ErrorAction SilentlyContinue
                Write-Host "[ ok  ] stopped supervisor PID $($parentInfo.ProcessId)" -ForegroundColor Green
            }
        }
    } catch { }
    try {
        Stop-Process -Id $procId -Force -ErrorAction Stop
        Write-Host "[ ok  ] stopped proxy process PID $procId" -ForegroundColor Green
    } catch {
        Write-Host "[warn ] could not stop PID ${procId}: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}
