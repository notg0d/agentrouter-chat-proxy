<# Test launcher syntax and mode selection without starting or stopping services. #>

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot

function Assert-Equal {
    param($Actual, $Expected, [string]$Label)
    if ($Actual -ne $Expected) { throw "${Label}: expected '$Expected', got '$Actual'" }
}

$asts = @{}
foreach ($file in @('start-proxy.ps1', 'install-autostart.ps1', 'uninstall-autostart.ps1')) {
    $tokens = $null
    $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $repoRoot $file), [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw ($errors | Out-String) }
    $asts[$file] = $ast

    # Execute only the actual parameter declarations and mode-selection nodes.
    # Never execute launcher side effects, process commands, or scheduled tasks.
    $selection = @($ast.EndBlock.Statements | Where-Object {
        ($_ -is [System.Management.Automation.Language.IfStatementAst] -and
         $_.Extent.Text.Contains('$Mode -eq ''combined''') -and
         $_.Extent.Text.Contains('$PSBoundParameters.ContainsKey(''Port'')')) -or
        ($_ -is [System.Management.Automation.Language.AssignmentStatementAst] -and
         $_.Left.Extent.Text -eq '$appModule')
    })
    if (-not $selection.Count) { throw "No mode-selection nodes found in $file" }
    $resultCode = @'
[pscustomobject]@{
    Port = $Port
    Mode = $Mode
    App = (Get-Variable appModule -ValueOnly -ErrorAction SilentlyContinue)
    TaskName = (Get-Variable TaskName -ValueOnly -ErrorAction SilentlyContinue)
}
'@
    $code = $ast.ParamBlock.Extent.Text + "`n" + ($selection.Extent.Text -join "`n") + "`n" + $resultCode
    $selectMode = [scriptblock]::Create($code)
    $default = & $selectMode
    $combined = & $selectMode -Mode combined
    $custom = & $selectMode -Mode combined -Port 9001
    Assert-Equal $default.Port 8788 "$file default port"
    Assert-Equal $default.Mode 'responses' "$file default mode"
    Assert-Equal $combined.Port 8789 "$file combined port"
    Assert-Equal $custom.Port 9001 "$file custom port"
    if ($file -eq 'start-proxy.ps1') {
        Assert-Equal $default.App 'proxy:app' 'Responses worker'
        Assert-Equal $combined.App 'chat_proxy:app' 'Combined worker'
    } else {
        Assert-Equal $default.TaskName 'AgentRouterCodexProxy' "$file default task"
        Assert-Equal $combined.TaskName 'AgentRouterCombinedProxy' "$file combined task"
        $customTask = & $selectMode -Mode combined -TaskName 'CustomProxy'
        Assert-Equal $customTask.TaskName 'CustomProxy' "$file custom task"
    }
    Write-Output "$file syntax and mode selection OK"
}

# Inspect display messages only; do not execute the launchers.
foreach ($file in @('start-proxy.ps1', 'install-autostart.ps1')) {
    $messages = @($asts[$file].FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -in @('Write-Line', 'Write-Host', 'Write-Warn')
    }, $true))
    $text = $messages.Extent.Text -join "`n"
    if (-not $text.Contains('dev-api-proxy')) { throw "$file is missing the project display name" }
    if ($text -match '\bCodex\b') { throw "$file still has a Codex display message" }
    if ($file -eq 'start-proxy.ps1' -and
        -not $text.Contains('Configure OpenCode to send its AgentRouter API key in the Authorization header.')) {
        throw 'Missing OpenCode credential guidance'
    }
}
Write-Output 'Launcher OpenCode display messages OK'

# Load only the configuration and health helpers. Replace network access with a local fixture.
foreach ($name in @('Import-ProxyEnvironment', 'Get-ProbeHost', 'Get-OurProxyHealth', 'Test-OurProxy')) {
    $node = $asts['start-proxy.ps1'].Find({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq $name
    }, $false)
    if (-not $node) { throw "Missing health helper: $name" }
    Invoke-Expression $node.Extent.Text
}
function Invoke-RestMethod {
    param([string]$Uri, [int]$TimeoutSec, [string]$ErrorAction)
    return $script:HealthFixture
}
$Mode = 'combined'
$script:HealthFixture = [pscustomobject]@{
    status = 'ok'; proxy = 'dev-api-proxy'; listen = '127.0.0.1:8789'
    supported_apis = @('responses'); max_body_bytes = 1073741824
}
$expectedBodyLimit = 1073741824L
Assert-Equal (Test-OurProxy -OnPort 8789) $false 'Combined mode rejects a Responses-only listener'
$script:HealthFixture.supported_apis = @('responses', 'chat.completions')
Assert-Equal (Test-OurProxy -OnPort 8789) $true 'Combined capabilities accepted'
$script:HealthFixture.max_body_bytes = 20971520
Assert-Equal (Test-OurProxy -OnPort 8789) $false 'Stale body limit is rejected'
$script:HealthFixture.max_body_bytes = $expectedBodyLimit
$script:HealthFixture.proxy = 'unrelated-service'
Assert-Equal (Test-OurProxy -OnPort 8789) $false 'Unrelated service rejected'
$script:HealthFixture.proxy = 'dev-api-proxy'
$script:HealthFixture.listen = '0.0.0.0:8789'
Assert-Equal (Test-OurProxy -OnPort 8789) $false 'Different bind address rejected'
Write-Output 'Launcher health capability checks OK'

# Exercise the env loader with an isolated temporary file and restore the
# caller's process environment. The fixture contains no real credentials.
function Write-Ok { param([string]$Message) }
$envNames = @('PROXY_MAX_BODY_BYTES', 'XINGCHI_API_KEY', 'PROXY_AUTH_TOKEN', 'XINGCHI_UPSTREAM')
$savedEnvironment = @{}
foreach ($name in $envNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
}
$fixturePath = Join-Path ([IO.Path]::GetTempPath()) ("proxy-env-test-{0}.env" -f [guid]::NewGuid())
try {
    [Environment]::SetEnvironmentVariable('PROXY_MAX_BODY_BYTES', '20971520')
    [Environment]::SetEnvironmentVariable('XINGCHI_API_KEY', 'inherited-key-fixture')
    [Environment]::SetEnvironmentVariable('PROXY_AUTH_TOKEN', 'inherited-token-fixture')
    [Environment]::SetEnvironmentVariable('XINGCHI_UPSTREAM', 'https://inherited.invalid')
    [IO.File]::WriteAllLines($fixturePath, @(
        'PROXY_MAX_BODY_BYTES=1073741824',
        'XINGCHI_API_KEY=file-key-fixture',
        'PROXY_AUTH_TOKEN=file-token-fixture',
        'XINGCHI_UPSTREAM=https://file.invalid'
    ))
    Assert-Equal (& { Import-ProxyEnvironment -EnvFile $fixturePath }) 1073741824L 'Local body limit overrides inherited value'
    Assert-Equal $env:PROXY_MAX_BODY_BYTES '1073741824' 'Resolved body limit is exported to the worker'
    Assert-Equal $env:XINGCHI_API_KEY 'inherited-key-fixture' 'Inherited provider key is preserved'
    Assert-Equal $env:PROXY_AUTH_TOKEN 'inherited-token-fixture' 'Inherited local token is preserved'
    Assert-Equal $env:XINGCHI_UPSTREAM 'https://inherited.invalid' 'Inherited upstream is preserved'

    Remove-Variable bodyLimitValue -ErrorAction SilentlyContinue
    [Environment]::SetEnvironmentVariable('PROXY_MAX_BODY_BYTES', $null)
    $missingFixture = Join-Path ([IO.Path]::GetTempPath()) ("proxy-env-missing-{0}.env" -f [guid]::NewGuid())
    Assert-Equal (& { Import-ProxyEnvironment -EnvFile $missingFixture }) 20971520L 'Default body limit is used when unset'

    Remove-Variable bodyLimitValue -ErrorAction SilentlyContinue
    [Environment]::SetEnvironmentVariable('PROXY_MAX_BODY_BYTES', '123456')
    [IO.File]::WriteAllLines($fixturePath, @('# no body-limit setting'))
    Assert-Equal (& { Import-ProxyEnvironment -EnvFile $fixturePath }) 123456L 'Inherited body limit remains when file omits it'

    Remove-Variable bodyLimitValue -ErrorAction SilentlyContinue
    [Environment]::SetEnvironmentVariable('PROXY_MAX_BODY_BYTES', '20971520')
    [IO.File]::WriteAllLines($fixturePath, @('PROXY_MAX_BODY_BYTES=invalid'))
    $invalidRejected = $false
    try { & { Import-ProxyEnvironment -EnvFile $fixturePath } | Out-Null } catch { $invalidRejected = $true }
    Assert-Equal $invalidRejected $true 'Invalid local body limit is rejected'
    Write-Output 'Launcher environment precedence checks OK'
} finally {
    Remove-Item -LiteralPath $fixturePath -Force -ErrorAction SilentlyContinue
    foreach ($name in $envNames) {
        [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name])
    }
}
