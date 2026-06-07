@echo off
REM PAVILOS dashboard launcher (Windows). Uses a Python that HAS the deps
REM (3.13 with fastapi/uvicorn), not the bare 3.14 that "py -3" may resolve to.
cd /d "%~dp0"
set "PY313=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
if exist "%PY313%" (
  "%PY313%" -m pavilos
) else (
  py -3.13 -m pavilos
)
pause
