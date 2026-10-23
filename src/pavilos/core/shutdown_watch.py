# src/pavilos/core/shutdown_watch.py
"""Run a callback when Windows ends the session (PC shutdown, restart, log-off).

Almost every past stop of the recorder was the PC shutting down: Windows terminated the
process (exit code 0x40010004) and the writer's buffer (up to one flush interval of
book updates) was lost. A console handler is not enough: this process loads user32.dll,
and Windows then does not send CTRL_SHUTDOWN_EVENT / CTRL_LOGOFF_EVENT to the console
handler; it sends WM_QUERYENDSESSION / WM_ENDSESSION to the process's top-level windows.
So a hidden top-level window (not a message-only one: those miss broadcasts) runs on its
own thread, and on WM_ENDSESSION calls ``callback`` synchronously -- Windows waits for it
(a few seconds) before terminating the process. No-op outside Windows."""
from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable

_log = logging.getLogger(__name__)

WM_DESTROY = 0x0002
WM_CLOSE = 0x0010
WM_QUERYENDSESSION = 0x0011
WM_ENDSESSION = 0x0016


class ShutdownWatcher:
    def __init__(self, callback: Callable[[str], None], *, class_name: str = "PavilosShutdownWatcher") -> None:
        self._callback = callback
        self._class_name = f"{class_name}{os.getpid()}"
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._fired = threading.Event()
        self.hwnd = None
        self.fired_with: str | None = None

    @property
    def class_name(self) -> str:
        return self._class_name

    def start(self, timeout: float = 5.0) -> bool:
        """True if the window is up and listening (False outside Windows or on failure)."""
        if os.name != "nt":
            return False
        self._thread = threading.Thread(target=self._run, name="shutdown-watch", daemon=True)
        self._thread.start()
        self._ready.wait(timeout)
        return self.hwnd is not None

    def stop(self, timeout: float = 2.0) -> None:
        if self.hwnd is None:
            return
        import ctypes
        ctypes.WinDLL("user32", use_last_error=True).PostMessageW(self.hwnd, WM_CLOSE, 0, 0)
        if self._thread is not None:
            self._thread.join(timeout)

    def _fire(self, reason: str) -> None:
        if self._fired.is_set():
            return                                  # once per session end
        self._fired.set()
        self.fired_with = reason
        try:
            self._callback(reason)
        except Exception:
            _log.exception("shutdown watcher: the save-on-shutdown callback failed")

    def _run(self) -> None:
        try:
            import ctypes
            from ctypes import wintypes
            user32 = ctypes.WinDLL("user32", use_last_error=True)
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            LRESULT = ctypes.c_ssize_t
            WNDPROC = ctypes.WINFUNCTYPE(LRESULT, wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)

            class WNDCLASSW(ctypes.Structure):
                _fields_ = [("style", wintypes.UINT), ("lpfnWndProc", WNDPROC), ("cbClsExtra", ctypes.c_int),
                            ("cbWndExtra", ctypes.c_int), ("hInstance", wintypes.HINSTANCE),
                            ("hIcon", wintypes.HICON), ("hCursor", wintypes.HANDLE),
                            ("hbrBackground", wintypes.HBRUSH), ("lpszMenuName", wintypes.LPCWSTR),
                            ("lpszClassName", wintypes.LPCWSTR)]

            user32.DefWindowProcW.argtypes = [wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM]
            user32.DefWindowProcW.restype = LRESULT
            user32.CreateWindowExW.argtypes = [wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD,
                                               ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                               wintypes.HWND, wintypes.HMENU, wintypes.HINSTANCE, wintypes.LPVOID]
            user32.CreateWindowExW.restype = wintypes.HWND
            user32.RegisterClassW.argtypes = [ctypes.POINTER(WNDCLASSW)]
            user32.RegisterClassW.restype = wintypes.ATOM
            user32.GetMessageW.argtypes = [ctypes.POINTER(wintypes.MSG), wintypes.HWND, wintypes.UINT, wintypes.UINT]
            user32.GetMessageW.restype = wintypes.BOOL
            kernel32.GetModuleHandleW.restype = wintypes.HMODULE

            def wndproc(hwnd, msg, wparam, lparam):
                if msg == WM_QUERYENDSESSION:
                    return 1                        # never block the shutdown
                if msg == WM_ENDSESSION:
                    if wparam:                      # the session really ends
                        self._fire("windows-endsession")
                    return 0
                if msg == WM_CLOSE:
                    user32.DestroyWindow(hwnd)
                    return 0
                if msg == WM_DESTROY:
                    user32.PostQuitMessage(0)
                    return 0
                return user32.DefWindowProcW(hwnd, msg, wparam, lparam)

            self._wndproc = WNDPROC(wndproc)        # keep a reference: ctypes must not free it
            hinst = kernel32.GetModuleHandleW(None)
            wc = WNDCLASSW()
            wc.lpfnWndProc = self._wndproc
            wc.hInstance = hinst
            wc.lpszClassName = self._class_name
            if not user32.RegisterClassW(ctypes.byref(wc)):
                raise OSError(ctypes.get_last_error(), "RegisterClassW failed")
            hwnd = user32.CreateWindowExW(0, self._class_name, "PAVILOS shutdown watcher", 0,
                                          0, 0, 0, 0, None, None, hinst, None)
            if not hwnd:
                raise OSError(ctypes.get_last_error(), "CreateWindowExW failed")
            self.hwnd = hwnd
        except Exception:
            _log.exception("shutdown watcher: cannot create its hidden window; a PC shutdown will "
                           "not flush the recorder")
            self._ready.set()
            return
        self._ready.set()
        msg = wintypes.MSG()
        while user32.GetMessageW(ctypes.byref(msg), None, 0, 0) > 0:
            user32.TranslateMessage(ctypes.byref(msg))
            user32.DispatchMessageW(ctypes.byref(msg))
        self.hwnd = None
