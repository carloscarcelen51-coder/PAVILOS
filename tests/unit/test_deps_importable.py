# tests/unit/test_deps_importable.py
def test_async_transport_deps_importable():
    import websockets  # noqa: F401
    import aiohttp     # noqa: F401
    assert websockets is not None
    assert aiohttp is not None
