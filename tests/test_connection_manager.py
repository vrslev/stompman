import asyncio
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Self
from unittest import mock

import pytest

import stompman
from stompman.connection_manager import ActiveConnectionState
from stompman.errors import ConnectionLostError, FailedAllConnectAttemptsError, RepeatedConnectionLostError
from stompman.frames import AnyServerFrame, ConnectFrame
from tests.conftest import BaseMockConnection, EnrichedConnectionManager, build_dataclass

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("mock_sleep")]


@pytest.mark.parametrize("ok_on_attempt", [1, 2, 3])
async def test_connect_to_one_server_ok(ok_on_attempt: int, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(  # noqa: PLR0913
            cls, host: str, port: int, timeout: int, read_max_chunk_size: int, read_timeout: int
        ) -> Self | None:
            assert (host, port) == (manager.servers[0].host, manager.servers[0].port)
            nonlocal attempts
            attempts += 1

            return (
                await super().connect(host, port, timeout, read_max_chunk_size, read_timeout)
                if attempts == ok_on_attempt
                else None
            )

    sleep_mock = mock.AsyncMock()
    monkeypatch.setattr("asyncio.sleep", sleep_mock)
    manager = EnrichedConnectionManager(connection_class=MockConnection)
    assert await manager._connect_to_one_server(manager.servers[0])
    assert attempts == ok_on_attempt == (len(sleep_mock.mock_calls) + 1)


async def test_connect_to_one_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        connect = mock.AsyncMock(return_value=None)

    manager = EnrichedConnectionManager(connection_class=MockConnection)
    assert await manager._connect_to_one_server(manager.servers[0]) is None


async def test_connect_to_any_server_ok() -> None:
    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(  # noqa: PLR0913
            cls, host: str, port: int, timeout: int, read_max_chunk_size: int, read_timeout: int
        ) -> Self | None:
            return (
                await super().connect(host, port, timeout, read_max_chunk_size, read_timeout)
                if port == successful_server.port
                else None
            )

    successful_server = build_dataclass(stompman.ConnectionParameters)
    manager = EnrichedConnectionManager(
        servers=[
            build_dataclass(stompman.ConnectionParameters),
            build_dataclass(stompman.ConnectionParameters),
            successful_server,
            build_dataclass(stompman.ConnectionParameters),
        ],
        connection_class=MockConnection,
    )
    connection, connection_parameters = await manager._connect_to_any_server()
    assert connection
    assert connection_parameters == successful_server


async def test_connect_to_any_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        connect = mock.AsyncMock(return_value=None)

    manager = EnrichedConnectionManager(
        servers=[
            build_dataclass(stompman.ConnectionParameters),
            build_dataclass(stompman.ConnectionParameters),
            build_dataclass(stompman.ConnectionParameters),
            build_dataclass(stompman.ConnectionParameters),
        ],
        connection_class=MockConnection,
    )

    with pytest.raises(stompman.FailedAllConnectAttemptsError):
        await manager._connect_to_any_server()


async def test_get_active_connection_state_lifespan_flaky_ok() -> None:
    aenter = mock.AsyncMock(side_effect=[ConnectionLostError, None])
    lifespan = mock.Mock(return_value=SimpleNamespace(__aenter__=aenter))
    manager = EnrichedConnectionManager(lifespan=lifespan, connection_class=BaseMockConnection)

    await manager._get_active_connection_state()

    assert aenter.mock_calls == [mock.call(), mock.call()]
    assert lifespan.mock_calls == [
        mock.call(BaseMockConnection(), manager.servers[0]),
        mock.call(BaseMockConnection(), manager.servers[0]),
    ]


async def test_get_active_connection_state_lifespan_flaky_fails() -> None:
    aenter = mock.AsyncMock(side_effect=ConnectionLostError)
    lifespan = mock.Mock(return_value=SimpleNamespace(__aenter__=aenter))
    manager = EnrichedConnectionManager(lifespan=lifespan, connection_class=BaseMockConnection)

    with pytest.raises(stompman.RepeatedConnectionLostError) as exc_info:
        await manager._get_active_connection_state()

    assert (
        exc_info.value.retry_attempts
        == len(lifespan.mock_calls)
        == len(aenter.mock_calls)
        == manager.connect_retry_attempts
    )


async def test_get_active_connection_state_fails_to_connect() -> None:
    class MockConnection(BaseMockConnection):
        connect = mock.AsyncMock(return_value=None)

    with pytest.raises(FailedAllConnectAttemptsError):
        await EnrichedConnectionManager(connection_class=MockConnection)._get_active_connection_state()


async def test_get_active_connection_state_ok_concurrent() -> None:
    aenter = mock.AsyncMock(side_effect=None)
    lifespan = mock.Mock(return_value=SimpleNamespace(__aenter__=aenter))
    manager = EnrichedConnectionManager(lifespan=lifespan, connection_class=BaseMockConnection)

    first_state, second_state, third_state = await asyncio.gather(
        manager._get_active_connection_state(),
        manager._get_active_connection_state(),
        manager._get_active_connection_state(),
    )
    fourth_state = await manager._get_active_connection_state()

    assert (
        first_state
        == second_state
        == third_state
        == fourth_state
        == ActiveConnectionState(connection=BaseMockConnection(), lifespan=lifespan.return_value)
    )
    assert first_state is second_state is third_state is fourth_state

    aenter.assert_called_once_with()
    lifespan.assert_called_once_with(BaseMockConnection(), manager.servers[0])


async def test_write_heartbeat_reconnecting_raises() -> None:
    write_heartbeat_mock = mock.Mock(side_effect=[ConnectionLostError, ConnectionLostError, ConnectionLostError])

    class MockConnection(BaseMockConnection):
        write_heartbeat = write_heartbeat_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    with pytest.raises(RepeatedConnectionLostError):
        await manager.write_heartbeat_reconnecting()


async def test_write_frame_reconnecting_raises() -> None:
    write_frame_mock = mock.AsyncMock(side_effect=[ConnectionLostError, ConnectionLostError, ConnectionLostError])

    class MockConnection(BaseMockConnection):
        write_frame = write_frame_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    with pytest.raises(RepeatedConnectionLostError):
        await manager.write_frame_reconnecting(build_dataclass(ConnectFrame))


async def test_read_frames_reconnecting_raises() -> None:
    async def read_frames_mock(self: object) -> AsyncGenerator[AnyServerFrame, None]:
        raise ConnectionLostError
        yield
        await asyncio.Future()

    class MockConnection(BaseMockConnection):
        read_frames = read_frames_mock  # type: ignore[assignment]

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    with pytest.raises(RepeatedConnectionLostError):  # noqa: PT012
        async for _ in manager.read_frames_reconnecting():
            pass


@pytest.mark.parametrize(
    "side_effect", [(None,), (ConnectionLostError, None), (ConnectionLostError, ConnectionLostError, None)]
)
async def test_write_heartbeat_reconnecting_ok(side_effect: tuple[None | ConnectionLostError, ...]) -> None:
    write_heartbeat_mock = mock.Mock(side_effect=side_effect)

    class MockConnection(BaseMockConnection):
        write_heartbeat = write_heartbeat_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    await manager.write_heartbeat_reconnecting()

    assert len(write_heartbeat_mock.mock_calls) == len(side_effect)
