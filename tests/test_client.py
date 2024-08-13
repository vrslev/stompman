import asyncio
from collections.abc import AsyncIterable, Iterable
from unittest import mock

import pytest

from stompman import (
    AnyServerFrame,
    Client,
    ConnectedFrame,
    ConnectFrame,
    ConnectionParameters,
    HeartbeatFrame,
    ReceiptFrame,
)
from tests.conftest import EnrichedClient, build_dataclass, make_async_iter

pytestmark = pytest.mark.anyio


async def test_client_heartbeats_and_lifespan_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls = []
    real_sleep = asyncio.sleep

    async def mock_sleep(delay: float) -> None:
        await real_sleep(0)
        sleep_calls.append(delay)

    monkeypatch.setattr("asyncio.sleep", mock_sleep)

    written_and_read_frames = []

    async def mock_read_frames(iterable: Iterable[AnyServerFrame]) -> AsyncIterable[AnyServerFrame]:
        async for frame in make_async_iter(iterable):
            written_and_read_frames.append(frame)
            yield frame
        await asyncio.Future()

    connected_frame = build_dataclass(
        ConnectedFrame, headers={"version": EnrichedClient.PROTOCOL_VERSION, "heart-beat": "1000,1000"}
    )
    receipt_frame = build_dataclass(ReceiptFrame)

    on_heartbeat = mock.Mock()
    write_heartbeat = mock.Mock()
    connection = mock.AsyncMock(
        read_frames=mock.Mock(
            side_effect=[
                mock_read_frames([connected_frame]),
                mock_read_frames([HeartbeatFrame(), HeartbeatFrame()]),
                mock_read_frames([receipt_frame]),
            ]
        ),
        write_frame=mock.AsyncMock(side_effect=written_and_read_frames.append),
        write_heartbeat=write_heartbeat,
    )
    connection_class = mock.Mock(connect=mock.AsyncMock(return_value=connection))
    connection_parameters = build_dataclass(ConnectionParameters)
    async with Client(servers=[connection_parameters], connection_class=connection_class, on_heartbeat=on_heartbeat):
        await real_sleep(0)
        await real_sleep(0)
        await real_sleep(0)

    assert written_and_read_frames == [
        ConnectFrame(
            headers={
                "accept-version": "1.2",
                "heart-beat": "1000,1000",
                "host": connection_parameters.host,
                "login": connection_parameters.login,
                "passcode": connection_parameters.unescaped_passcode,
            }
        ),
        connected_frame,
        HeartbeatFrame(),
        HeartbeatFrame(),
        written_and_read_frames[-2],
        receipt_frame,
    ]

    assert sleep_calls == [0, 1, 0, 1]
    assert write_heartbeat.mock_calls == [mock.call(), mock.call(), mock.call()]
    assert on_heartbeat.mock_calls == [mock.call(), mock.call()]
