param([Parameter(ValueFromRemainingArguments=$true)][string[]]$TaskArguments)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$env:Path = "$env:LOCALAPPDATA\Chocolatey\bin;C:\Program Files\Git\usr\bin;$env:Path"
if (-not $TaskArguments) { $TaskArguments = @('--list') }
& mise exec -- task @TaskArguments
exit $LASTEXITCODE
