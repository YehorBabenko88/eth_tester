# Remove ONLY task proven owned by manifest and matching exact action.
[CmdletBinding()]
param([string]$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path -LiteralPath $Root).Path
$manifestPath = Join-Path $Root '.eth-tester-install.json'
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw 'Ownership manifest missing: refusing deletion'
}
$m = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
if ($m.product -cne 'ETHTester' -or $m.version -ne 1 -or
    $m.root -cne $Root -or $m.arguments -cne '-m eth_tester.cli run' -or
    $m.task -cne 'ETHTester-Owned-Supervisor') {
    throw 'Ownership mismatch: refusing deletion'
}
$t = Get-ScheduledTask -TaskName $m.task -ErrorAction SilentlyContinue
if ($t) {
    $actions = @($t.Actions)
    if ($actions.Count -ne 1 -or $actions[0].Execute -cne $m.python -or
        $actions[0].Arguments -cne $m.arguments -or
        $actions[0].WorkingDirectory -cne $m.root) {
        throw 'Task action does not match manifest: refusing deletion'
    }
    # Cooperative stop prevents leaving supervised child processes running.
    $runtime = Join-Path $env:ProgramData 'ETHTester\runtime'
    $marker = Join-Path $runtime 'shutdown.request'
    if (Test-Path -LiteralPath $runtime -PathType Container) {
        'Stop requested by verified uninstall' | Set-Content -LiteralPath $marker -Encoding Ascii
        for ($i = 0; $i -lt 20; $i++) {
            if ((Get-ScheduledTask -TaskName $m.task).State -ne 'Running') { break }
            Start-Sleep -Seconds 1
        }
    }
    if ((Get-ScheduledTask -TaskName $m.task).State -eq 'Running') {
        Stop-ScheduledTask -TaskName $m.task -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $marker -PathType Leaf) {
        Remove-Item -LiteralPath $marker -Force
    }
    Unregister-ScheduledTask -TaskName $m.task -Confirm:$false
}
Remove-Item -LiteralPath $manifestPath -Force
Write-Host 'Removed owned task and manifest; PostgreSQL, archives, source, tokens and user data preserved.'
