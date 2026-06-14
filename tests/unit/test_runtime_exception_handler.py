# tests/unit/test_runtime_exception_handler.py
import asyncio
import logging

from pavilos.core.runtime import _loop_exception_handler


def test_logs_callback_exception_and_does_not_raise(caplog):
    loop = asyncio.new_event_loop()
    try:
        with caplog.at_level(logging.ERROR):
            # must NOT raise, regardless of the context shape
            _loop_exception_handler(loop, {"message": "Exception in callback Client.receive_loop()",
                                           "exception": TypeError("'<' not supported between ... NoneType")})
        assert any("Client.receive_loop" in r.message or "loop exception" in r.message.lower()
                   for r in caplog.records)
    finally:
        loop.close()


def test_ignores_cancellation_and_handles_missing_exception(caplog):
    loop = asyncio.new_event_loop()
    try:
        with caplog.at_level(logging.ERROR):
            _loop_exception_handler(loop, {"exception": asyncio.CancelledError()})   # benign
            # benign cancellation/shutdown must be SILENT (regression guard: a handler
            # that starts logging on CancelledError would fail here)
            assert not caplog.records
            _loop_exception_handler(loop, {"message": "no exception object here"})    # must not raise
    finally:
        loop.close()
