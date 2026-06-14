' PAVILOS recorder supervisor - launched HIDDEN at logon for reboot survival.
' Runs scripts\run_recorder.bat with no console window; the supervisor then
' auto-restarts the recorder on any crash, so recording resumes automatically
' after a machine reboot (a copy of this .vbs lives in the user Startup folder).
Set sh = CreateObject("WScript.Shell")
sh.Run """C:\Users\FSOCIETY\Desktop\APPS\PAVILOS\scripts\run_recorder.bat""", 0, False
