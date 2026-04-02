$ErrorActionPreference = 'Stop'

$base = "C:\Users\Admin\Python Projects\TradingBotMOEX\fix_engine\certs"
New-Item -ItemType Directory -Force -Path $base | Out-Null

$root = Get-ChildItem Cert:\CurrentUser\Root | Where-Object { $_.Subject -like "*CN=Russian Trusted Root CA*" } | Select-Object -First 1
if (-not $root) { throw "Root CA not found in CurrentUser\Root" }

$sub = Get-ChildItem Cert:\CurrentUser\CA | Where-Object { $_.Subject -like "*CN=Russian Trusted Sub CA*" } | Select-Object -First 1
if (-not $sub) { throw "Sub CA not found in CurrentUser\CA" }

Export-Certificate -Cert $root -FilePath (Join-Path $base "russian_trusted_root_ca.cer") -Type CERT | Out-Null
Export-Certificate -Cert $sub  -FilePath (Join-Path $base "russian_trusted_sub_ca.cer")  -Type CERT | Out-Null

certutil -encode (Join-Path $base "russian_trusted_root_ca.cer") (Join-Path $base "russian_trusted_root_ca.pem") | Out-Null
certutil -encode (Join-Path $base "russian_trusted_sub_ca.cer")  (Join-Path $base "russian_trusted_sub_ca.pem")  | Out-Null

Get-Content (Join-Path $base "russian_trusted_root_ca.pem"), (Join-Path $base "russian_trusted_sub_ca.pem") |
  Set-Content -Encoding ascii (Join-Path $base "russian_trusted_chain.pem")

Write-Host ("OK bundle: " + (Join-Path $base "russian_trusted_chain.pem"))
Write-Host ("root_thumb " + $root.Thumbprint)
Write-Host ("sub_thumb  " + $sub.Thumbprint)

