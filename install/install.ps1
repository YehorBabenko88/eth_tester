# ETH Tester G Control: single owned startup task, no automatic DB creation or data deletion.
# Administrator provisions/migrates the dedicated DB using: python -m eth_tester.cli init
[CmdletBinding()]
param(
    [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path,
    [string]$Python = ''
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path -LiteralPath $Root).Path
if (-not $Python) { $Python = Join-Path $Root '.venv\Scripts\python.exe' }
$Python = (Resolve-Path -LiteralPath $Python -ErrorAction Stop).Path
$envFile = Join-Path $Root '.env'
$manifest = Join-Path $Root '.eth-tester-install.json'
$taskName = 'ETHTester-Owned-Supervisor'

if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw 'Configure protected .env before installation'
}
if (-not (Test-Path -LiteralPath (Join-Path $Root 'src\eth_tester\supervisor.py') -PathType Leaf)) {
    throw 'ETH Tester installation root is invalid'
}
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Administrator privileges required for startup registration'
}
if (Test-Path -LiteralPath $manifest) {
    throw 'Ownership manifest already exists; refusing overwrite'
}
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw 'Scheduled task name occupied; refusing overwrite'
}

# Locale-independent SIDs, fail closed if an ACL principal cannot be identified.
function Get-AllowedReadSids([string]$Path) {
    $sids = @()
    foreach ($ace in (Get-Acl -LiteralPath $Path).Access) {
        if ($ace.AccessControlType -ne 'Allow' -or
            (($ace.FileSystemRights -band [Security.AccessControl.FileSystemRights]::ReadData) -eq 0)) {
            continue
        }
        try {
            $sids += $ace.IdentityReference.Translate(
                [Security.Principal.SecurityIdentifier]).Value
        } catch { throw "Cannot validate ACL principal on $Path" }
    }
    return $sids
}
$readers = @(Get-AllowedReadSids $envFile)
if ($readers -notcontains 'S-1-5-19') {
    throw 'Grant LocalService (S-1-5-19) explicit read permission to .env'
}
foreach ($sid in @('S-1-1-0','S-1-5-11','S-1-5-32-545')) {
    if ($readers -contains $sid) { throw 'Broad read ACL on .env; installation refused' }
}
foreach ($target in @($Root, $Python)) {
    foreach ($ace in (Get-Acl -LiteralPath $target).Access) {
        if ($ace.AccessControlType -ne 'Allow' -or
            (($ace.FileSystemRights -band [Security.AccessControl.FileSystemRights]'Write,Modify,FullControl') -eq 0)) {
            continue
        }
        try {
            $sid = $ace.IdentityReference.Translate(
                [Security.Principal.SecurityIdentifier]).Value
        } catch { throw "Cannot validate ACL principal on $target" }
        if (@('S-1-1-0','S-1-5-11','S-1-5-32-545') -contains $sid) {
            throw "Broad writable installation component: $target"
        }
    }
}
$hasBotToken = [bool](Select-String -LiteralPath $envFile -Pattern '^\s*TG_BOT_TOKEN\s*=\s*\S+' -Quiet)
$ownerId = [Environment]::GetEnvironmentVariable('ETH_TESTER_TG_USER_ID', 'Machine')
if ($hasBotToken -and $ownerId -notmatch '^[0-9]+$') {
    throw 'Set machine-level ETH_TESTER_TG_USER_ID before installing Telegram worker'
}

# Read-only preflights. No child process or database DDL is started.
Push-Location $Root
try {
    & $Python -m eth_tester.cli --help *> $null
    if ($LASTEXITCODE -ne 0) { throw 'ETH Tester Python environment failed CLI import check' }
    & $Python -c 'from eth_tester.config import Settings; from eth_tester.database import verify_existing_schema; verify_existing_schema(Settings())'
    if ($LASTEXITCODE -ne 0) { throw 'ETH Tester database requires explicit administrator init/upgrade' }
} finally { Pop-Location }

# Never adopt, grant access to, or delete a pre-existing unowned ProgramData path.
$runtimeRoot = Join-Path $env:ProgramData 'ETHTester'
if (Test-Path -LiteralPath $runtimeRoot) {
    throw 'Existing runtime root without ownership manifest; refuse adoption'
}
New-Item -ItemType Directory -Path $runtimeRoot -ErrorAction Stop | Out-Null
$runtimeAcl = Get-Acl -LiteralPath $runtimeRoot
$runtimeAcl.SetAccessRuleProtection($true, $false)
foreach ($oldRule in @($runtimeAcl.Access)) {
    $runtimeAcl.RemoveAccessRuleSpecific($oldRule)
}
foreach ($grant in @(
    @{ Sid='S-1-5-18'; Rights='FullControl' }, # SYSTEM
    @{ Sid='S-1-5-32-544'; Rights='FullControl' }, # Administrators
    @{ Sid='S-1-5-19'; Rights='Modify' } # LOCAL SERVICE
)) {
    $sid = [Security.Principal.SecurityIdentifier]::new($grant.Sid)
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        $sid, [Security.AccessControl.FileSystemRights]$grant.Rights,
        [Security.AccessControl.InheritanceFlags]'ContainerInherit,ObjectInherit',
        [Security.AccessControl.PropagationFlags]::None,
        [Security.AccessControl.AccessControlType]::Allow
    )
    $runtimeAcl.AddAccessRule($rule)
}
Set-Acl -LiteralPath $runtimeRoot -AclObject $runtimeAcl
$runtime = Join-Path $runtimeRoot 'runtime'
New-Item -ItemType Directory -Path $runtime -ErrorAction Stop | Out-Null

$action = New-ScheduledTaskAction -Execute $Python -Argument '-m eth_tester.cli run' -WorkingDirectory $Root
$trigger = New-ScheduledTaskTrigger -AtStartup
$owner = New-ScheduledTaskPrincipal -UserId 'NT AUTHORITY\LOCAL SERVICE' -LogonType ServiceAccount -RunLevel Limited
$opts = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $owner -Settings $opts | Out-Null
try {
    [ordered]@{
        product='ETHTester'; version=1; task=$taskName; root=$Root; python=$Python
        arguments='-m eth_tester.cli run'; principal='S-1-5-19'
        created_utc=(Get-Date).ToUniversalTime().ToString('o')
    } | ConvertTo-Json | Set-Content -LiteralPath $manifest -Encoding UTF8 -ErrorAction Stop
} catch {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    throw
}
Write-Host "Installed owned startup task: $taskName; database unchanged"
