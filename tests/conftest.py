"""
Session-scoped event loop for pytest-asyncio.

Using one event loop for the entire test session prevents aiosqlite's
background threads from hitting a closed loop between tests (the "Event
loop is closed" RuntimeError that shows up in teardown).  This is the
recommended workaround for pytest-asyncio < 0.24 and for apps that
spawn asyncio.create_task() inside request handlers.
"""
import asyncio
import pytest


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
