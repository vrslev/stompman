import asyncio
import socket
from collections.abc import Awaitable
from functools import partial
from typing import Any
from unittest import mock

import pytest

from stompman import (
    AnyServerFrame,
    ConnectedFrame,
    Connection,
    ConnectionLostError,
    ConnectionParameters,
    HeartbeatFrame,
)
from stompman.frames import BeginFrame, CommitFrame


@pytest.fixture()
def connection() -> Connection:
    return Connection(
        connection_parameters=ConnectionParameters("localhost", 12345, login="login", passcode="passcode"),
        connect_timeout=2,
        read_timeout=2,
        read_max_chunk_size=2,
    )


def mock_wait_for(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_impl(future: Awaitable[Any], timeout: int) -> Any:  # noqa: ANN401, ARG001
        return await original_wait_for(future, timeout=0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", mock_impl)


def test_connection_parameters_from_pydantic_multihost_hosts() -> None:
    full_host: dict[str, Any] = {"username": "me", "password": "pass", "host": "localhost", "port": 1234}
    assert ConnectionParameters.from_pydantic_multihost_hosts([{**full_host, "port": index} for index in range(5)]) == [  # type: ignore[typeddict-item]
        ConnectionParameters(full_host["host"], index, full_host["username"], full_host["password"])
        for index in range(5)
    ]

    for key in ("username", "password", "host", "port"):
        with pytest.raises(ValueError, match=f"{key} must be set"):
            assert ConnectionParameters.from_pydantic_multihost_hosts([{**full_host, key: None}, full_host])  # type: ignore[typeddict-item, list-item]


async def test_connection_lifespan(connection: Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    class MockWriter:
        close = mock.Mock()
        wait_closed = mock.AsyncMock()
        write = mock.Mock()
        drain = mock.AsyncMock()

    read_bytes = [
        b"\n\n",
        b"\nC",
        b"ON",
        b"NE",
        b"CT",
        b"ED",
        b"\n",
        b"he",
        b"ar",
        b"t-",
        b"be",
        b"at",
        b":0",
        b",0",
        b"\nse",
        b"rv",
        b"er:",
        b"som",
        b"e server\nversion:1.2\n\n\x00",
    ]

    class MockReader:
        read = mock.AsyncMock(side_effect=read_bytes)

    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(return_value=(MockReader(), MockWriter())))

    await connection.connect()
    connection.write_heartbeat()
    await connection.write_frame(CommitFrame(headers={"transaction": "transaction"}))

    async def take_frames(count: int) -> list[AnyServerFrame]:
        frames = []
        async for frame in connection.read_frames():
            frames.append(frame)
            if len(frames) == count:
                break

        return frames

    expected_frames = [
        HeartbeatFrame(),
        HeartbeatFrame(),
        HeartbeatFrame(),
        ConnectedFrame(headers={"heart-beat": "0,0", "version": "1.2", "server": "some server"}),
    ]
    assert await take_frames(len(expected_frames)) == expected_frames
    await connection.close()

    MockWriter.close.assert_called_once_with()
    MockWriter.wait_closed.assert_called_once_with()
    MockWriter.drain.assert_called_once_with()
    MockReader.read.mock_calls = [mock.call(connection.read_timeout)] * len(read_bytes)  # type: ignore[assignment]
    assert MockWriter.write.mock_calls == [mock.call(b"\n"), mock.call(b"COMMIT\ntransaction:transaction\n\n\x00")]


async def test_connection_close_connection_error(connection: Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    class MockWriter:
        close = mock.Mock()
        wait_closed = mock.AsyncMock(side_effect=ConnectionError)

    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(return_value=(mock.Mock(), MockWriter())))
    await connection.connect()

    with pytest.raises(ConnectionLostError):
        await connection.close()


async def test_connection_write_frame_connection_error(connection: Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    class MockWriter:
        write = mock.Mock()
        drain = mock.AsyncMock(side_effect=ConnectionError)

    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(return_value=(mock.Mock(), MockWriter())))
    await connection.connect()

    with pytest.raises(ConnectionLostError):
        await connection.write_frame(BeginFrame(headers={"transaction": ""}))


async def test_connection_timeout(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    mock_wait_for(monkeypatch)
    assert not await connection.connect()


@pytest.mark.parametrize("exception", [BrokenPipeError, socket.gaierror])
async def test_connection_connect_connection_error(
    monkeypatch: pytest.MonkeyPatch, connection: Connection, exception: type[Exception]
) -> None:
    monkeypatch.setattr("asyncio.open_connection", mock.AsyncMock(side_effect=exception))
    assert not await connection.connect()


async def test_read_frames_timeout_error(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    monkeypatch.setattr(
        "asyncio.open_connection",
        mock.AsyncMock(return_value=(mock.AsyncMock(read=partial(asyncio.sleep, 5)), mock.AsyncMock())),
    )
    await connection.connect()
    mock_wait_for(monkeypatch)
    with pytest.raises(ConnectionLostError):
        [frame async for frame in connection.read_frames()]


async def test_read_frames_connection_error(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> None:
    monkeypatch.setattr(
        "asyncio.open_connection",
        mock.AsyncMock(
            return_value=(mock.AsyncMock(read=mock.AsyncMock(side_effect=BrokenPipeError)), mock.AsyncMock())
        ),
    )
    await connection.connect()
    with pytest.raises(ConnectionLostError):
        [frame async for frame in connection.read_frames()]
