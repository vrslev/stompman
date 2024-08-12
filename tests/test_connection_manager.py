import asyncio
from collections.abc import AsyncGenerator, AsyncIterable
from unittest import mock

import faker
import pytest

from stompman import (
    AnyServerFrame,
    ConnectedFrame,
    ConnectFrame,
    ConnectionLostError,
    ErrorFrame,
    FailedAllWriteAttemptsError,
    MessageFrame,
)
from stompman.connection_manager import ActiveConnectionState
from tests.conftest import BaseMockConnection, EnrichedConnectionManager, build_dataclass

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("mock_sleep")]
FAKER = faker.Faker()


# async def test_get_active_connection_state_lifespan_flaky_ok() -> None:
#     enter = mock.AsyncMock(side_effect=[ConnectionLostError, None])
#     lifespan_factory = mock.Mock(return_value=mock.Mock(enter=enter))
#     manager = EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=BaseMockConnection)

#     await manager._get_active_connection_state()

#     assert enter.mock_calls == [mock.call(), mock.call()]
#     assert lifespan_factory.mock_calls == [
#         mock.call(connection=BaseMockConnection(), connection_parameters=manager.servers[0]),
#         mock.call(connection=BaseMockConnection(), connection_parameters=manager.servers[0]),
#     ]


# async def test_get_active_connection_state_lifespan_flaky_fails() -> None:
#     enter = mock.AsyncMock(side_effect=ConnectionLostError)
#     lifespan_factory = mock.Mock(return_value=mock.Mock(enter=enter))
#     manager = EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=BaseMockConnection)

#     with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
#         await manager._get_active_connection_state()

#     assert (
#         exc_info.value.retry_attempts
#         == len(lifespan_factory.mock_calls)
#         == len(enter.mock_calls)
#         == manager.connect_retry_attempts
#     )


# async def test_get_active_connection_state_fails_to_connect() -> None:
#     class MockConnection(BaseMockConnection):
#         connect = mock.AsyncMock(return_value=None)

#     with pytest.raises(FailedAllConnectAttemptsError):
#         await EnrichedConnectionManager(connection_class=MockConnection)._get_active_connection_state()


async def test_get_active_connection_state_ok_concurrent() -> None:
    enter = mock.AsyncMock(side_effect=[None])
    lifespan = mock.Mock(enter=enter)
    lifespan_factory = mock.Mock(side_effect=[lifespan])

    connection = mock.Mock()
    connect = mock.AsyncMock(side_effect=[connection])
    connection_class = mock.Mock(connect=connect)

    manager = EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=connection_class)

    first_state, second_state, third_state = await asyncio.gather(
        manager._get_active_connection_state(),
        manager._get_active_connection_state(),
        manager._get_active_connection_state(),
    )
    fourth_state = await manager._get_active_connection_state()

    assert first_state == ActiveConnectionState(connection=connection, lifespan=lifespan)
    assert first_state is second_state is third_state is fourth_state

    enter.assert_called_once_with()
    lifespan_factory.assert_called_once_with(connection=connection, connection_parameters=manager.servers[0])
    connect.assert_called_once()


async def test_connection_manager_context_connection_lost() -> None:
    async with EnrichedConnectionManager(connection_class=BaseMockConnection) as manager:
        manager._clear_active_connection_state()


async def test_connection_manager_context_lifespan_aexit_raises_connection_lost() -> None:
    async with EnrichedConnectionManager(
        lifespan_factory=mock.Mock(
            return_value=mock.Mock(
                enter=mock.AsyncMock(return_value=None), exit=mock.AsyncMock(side_effect=[ConnectionLostError])
            )
        ),
        connection_class=BaseMockConnection,
    ):
        pass


async def test_connection_manager_context_exits_ok() -> None:
    close = mock.AsyncMock()
    connection_close = mock.AsyncMock()

    class MockConnection(BaseMockConnection):
        close = connection_close

    async with EnrichedConnectionManager(
        lifespan_factory=mock.Mock(return_value=mock.Mock(enter=mock.AsyncMock(return_value=None), exit=close)),
        connection_class=MockConnection,
    ):
        pass

    close.assert_called_once()
    connection_close.assert_called_once_with()


async def test_write_heartbeat_reconnecting_raises() -> None:
    write_heartbeat_mock = mock.Mock(side_effect=[ConnectionLostError, ConnectionLostError, ConnectionLostError])

    class MockConnection(BaseMockConnection):
        write_heartbeat = write_heartbeat_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    with pytest.raises(FailedAllWriteAttemptsError):
        await manager.write_heartbeat_reconnecting()


async def test_write_frame_reconnecting_raises() -> None:
    write_frame_mock = mock.AsyncMock(side_effect=[ConnectionLostError, ConnectionLostError, ConnectionLostError])

    class MockConnection(BaseMockConnection):
        write_frame = write_frame_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    with pytest.raises(FailedAllWriteAttemptsError):
        await manager.write_frame_reconnecting(build_dataclass(ConnectFrame))


SIDE_EFFECTS = [(None,), (ConnectionLostError(), None), (ConnectionLostError(), ConnectionLostError(), None)]


@pytest.mark.parametrize("side_effect", SIDE_EFFECTS)
async def test_write_heartbeat_reconnecting_ok(side_effect: tuple[None | ConnectionLostError, ...]) -> None:
    write_heartbeat_mock = mock.Mock(side_effect=side_effect)

    class MockConnection(BaseMockConnection):
        write_heartbeat = write_heartbeat_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    await manager.write_heartbeat_reconnecting()

    assert len(write_heartbeat_mock.mock_calls) == len(side_effect)


@pytest.mark.parametrize("side_effect", SIDE_EFFECTS)
async def test_write_frame_reconnecting_ok(side_effect: tuple[None | ConnectionLostError, ...]) -> None:
    write_frame_mock = mock.AsyncMock(side_effect=side_effect)

    class MockConnection(BaseMockConnection):
        write_frame = write_frame_mock

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    await manager.write_frame_reconnecting(frame := build_dataclass(ConnectFrame))

    assert write_frame_mock.mock_calls == [mock.call(frame)] * len(side_effect)


@pytest.mark.parametrize("side_effect", SIDE_EFFECTS)
async def test_read_frames_reconnecting_ok(side_effect: tuple[None | ConnectionLostError, ...]) -> None:
    frames: list[AnyServerFrame] = [
        build_dataclass(ConnectedFrame),
        build_dataclass(MessageFrame),
        build_dataclass(ErrorFrame),
    ]
    attempt = -1

    class MockConnection(BaseMockConnection):
        @staticmethod
        async def read_frames() -> AsyncGenerator[AnyServerFrame, None]:
            nonlocal attempt
            attempt += 1
            current_effect = side_effect[attempt]
            if isinstance(current_effect, ConnectionLostError):
                raise ConnectionLostError
            for frame in frames:
                yield frame

    manager = EnrichedConnectionManager(connection_class=MockConnection)

    async def take_all_frames() -> AsyncIterable[AnyServerFrame]:
        iterator = manager.read_frames_reconnecting()
        for _ in frames:
            yield await anext(iterator)

    assert frames == [frame async for frame in take_all_frames()]


async def test_maybe_write_frame_connection_already_lost() -> None:
    manager = EnrichedConnectionManager(connection_class=BaseMockConnection)
    assert not await manager.maybe_write_frame(build_dataclass(ConnectFrame))


async def test_maybe_write_frame_connection_now_lost() -> None:
    class MockConnection(BaseMockConnection):
        write_frame = mock.AsyncMock(side_effect=[ConnectionLostError])

    async with EnrichedConnectionManager(connection_class=MockConnection) as manager:
        assert not await manager.maybe_write_frame(build_dataclass(ConnectFrame))


async def test_maybe_write_frame_ok() -> None:
    async with EnrichedConnectionManager(connection_class=BaseMockConnection) as manager:
        assert await manager.maybe_write_frame(build_dataclass(ConnectFrame))
