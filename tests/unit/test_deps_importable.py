# tests/unit/test_deps_importable.py
def test_async_transport_deps_importable():
    import websockets  # noqa: F401
    import aiohttp     # noqa: F401
    assert websockets is not None
    assert aiohttp is not None


def test_web_deps_importable():
    import fastapi, uvicorn, httpx  # noqa: F401


def test_ccxt_importable():
    import ccxt, ccxt.pro  # noqa: F401
    assert ccxt.pro.gate().has.get("watchOrderBook") is True
