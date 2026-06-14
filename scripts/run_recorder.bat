@echo off
REM PAVILOS recorder supervisor launcher (reboot survival).
REM cd to the project root (this .bat lives in scripts\), then run the auto-restart
REM supervisor with the Python 3.13 that has the deps (NOT the py-launcher 3.14).
cd /d "%~dp0.."
"%LOCALAPPDATA%\Programs\Python\Python313\python.exe" -m scripts.run_recorder >> "recorder_supervisor.log" 2>&1
