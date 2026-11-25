# Register a task at Windows startup as restricted LocalService. Never delete DB.
[CmdletBinding()]
param(
  [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path,
  [string]$Python = ''
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path -LiteralPath $Root).Path
if (-not $Python) { $Python = Join-Path $Root '.venv\Scripts\python.exe' }
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) { throw "Python missing: $Python" }
if (-not (Test-Path -LiteralPath (Join-Path $Root '.env') -PathType Leaf)) {
    throw 'Configure secure .env first'
}
if (-not (Test-Path -LiteralPath (Join-Path $Root 'src\eth_tester\supervisor.py'))) {
    throw 'ETH Tester installation root invalid'
}
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Administrator privileges required to register at startup'
}
$manifest = Join-Path $Root '.eth-tester-install.json'
$taskName = 'ETHTester-Owned-Supervisor'
if (Test-Path -LiteralPath $manifest) { throw 'Manifest already exists; uninstall first' }
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    throw 'Task name occupied; refusing to overwrite чужой task'
}
# Explicit, narrowly scoped ACL must grant LocalService access. Never grant Everyone read.
$access = (Get-Acl -LiteralPath (Join-Path $Root '.env')).Access
$canRead = $false
$broad = $false
foreach ($ace in $access) {
    if ($ace.AccessControlType -ne 'Allow' -or
        (($ace.FileSystemRights -band [Security.AccessControl.FileSystemRights]::ReadData) -eq 0)) {
        continue
    }
    # Well-known SIDs are independent of localized Windows account names.
    try { $sid = $ace.IdentityReference.Translate(
        [System.Security.Principal.SecurityIdentifier]).Value
    } catch { continue }
    if ($sid -eq 'S-1-5-19') { $canRead = $true }
    if (@('S-1-1-0','S-1-5-11','S-1-5-32-545') -contains $sid) { $broad = $true }
}
if (-not $canRead) { throw 'Grant LOCAL SERVICE (S-1-5-19) explicit read ACL to .env' }
if ($broad) { throw 'Insecure broad read ACL on .env; restrict it before installing' }
# Supervisor state is written outside the source/.env tree to a narrowly scoped runtime directory.
$runtime = Join-Path $env:ProgramData 'ETHTester\runtime'
New-Item -ItemType Directory -Path $runtime -Force | Out-Null
$runtimeAcl = Get-Acl -LiteralPath $runtime
$rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
    'NT AUTHORITY\LOCAL SERVICE', 'Modify',
    [System.Security.AccessControl.InheritanceFlags]'ContainerInherit,ObjectInherit',
    [System.Security.AccessControl.PropagationFlags]::None,
    [System.Security.AccessControl.AccessControlType]::Allow
)
$runtimeAcl.AddAccessRule($rule)
Set-Acl -LiteralPath $runtime -AclObject $runtimeAcl
$action = New-ScheduledTaskAction -Execute $Python -Argument '-m eth_tester.cli run' -WorkingDirectory $Root
$trigger = New-ScheduledTaskTrigger -AtStartup
$owner = New-ScheduledTaskPrincipal -UserId 'NT AUTHORITY\LOCAL SERVICE' -LogonType ServiceAccount -RunLevel Limited
$opts = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $owner -Settings $opts | Out-Null
try {
    [ordered]@{
        product='ETHTester'; version=1; task=$taskName; root=$Root; python=$Python
        arguments='-m eth_tester.cli run'; created_utc=(Get-Date).ToUniversalTime().ToString('o')
    } | ConvertTo-Json | Set-Content -LiteralPath $manifest -Encoding UTF8 -ErrorAction Stop
} catch {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    throw
}
Write-Host "Installed owned Windows startup task: $taskName"
