import asyncio
from unittest import mock

import pytest

from tests.conftest import EnrichedClient, create_spying_connection, get_read_frames_with_lifespan

pytestmark = pytest.mark.anyio


async def test_client_heartbeats_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_sleep(delay: float) -> None:
        await real_sleep(0)
        sleep_calls.append(delay)

    sleep_calls: list[float] = []
    real_sleep = asyncio.sleep
    monkeypatch.setattr("asyncio.sleep", mock_sleep)

    connection_class, _ = create_spying_connection(*get_read_frames_with_lifespan([]))
    connection_class.write_heartbeat = (write_heartbeat_mock := mock.Mock())  # type: ignore[method-assign]

    async with EnrichedClient(connection_class=connection_class):
        await real_sleep(0)
        await real_sleep(0)
        await real_sleep(0)

    assert sleep_calls == [0, 1, 1]
    assert write_heartbeat_mock.mock_calls == [mock.call(), mock.call(), mock.call()]
