# PAVILOS dashboard launcher (PowerShell).
# Uses a Python that HAS the deps (3.13 with fastapi/uvicorn), not the bare 3.14
# that `py -3` / `python` may resolve to. Then open http://127.0.0.1:8800
Set-Location $PSScriptRoot
$py = Join-Path $env:LOCALAPPDATA "Programs\Python\Python313\python.exe"
if (Test-Path $py) { & $py -m pavilos } else { py -3.13 -m pavilos }
