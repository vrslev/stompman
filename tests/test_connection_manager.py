import asyncio
import itertools
from collections.abc import AsyncGenerator, AsyncIterable, Awaitable
from typing import TypeVar, get_args
from unittest import mock

import pytest
from faker import Faker

from stompman import (
    AllServersUnavailable,
    AnyConnectionIssue,
    AnyServerFrame,
    ConnectedFrame,
    ConnectFrame,
    ConnectionLost,
    ConnectionLostError,
    ErrorFrame,
    FailedAllConnectAttemptsError,
    FailedAllWriteAttemptsError,
    MessageFrame,
    StompProtocolConnectionIssue,
)
from stompman.config import ConnectionParameters
from stompman.connection_manager import (
    ActiveConnectionState,
    attempt_to_connect,
    connect_to_first_server,
    make_healthy_connection,
)
from tests.conftest import BaseMockConnection, EnrichedConnectionManager, build_dataclass

pytestmark = pytest.mark.anyio


class TestAttemptToConnect:
    async def test_ok(self) -> None:
        active_connection_state = ActiveConnectionState(connection=mock.Mock(), lifespan=mock.Mock())
        connection_issues = [build_dataclass(issue_type) for issue_type in get_args(AnyConnectionIssue)]
        sleep = mock.AsyncMock()

        result = await attempt_to_connect(
            connect=mock.AsyncMock(side_effect=[*connection_issues, active_connection_state, connection_issues[0]]),
            connect_retry_interval=5,
            connect_retry_attempts=len(connection_issues) + 1,
            sleep=sleep,
        )

        assert result is active_connection_state
        assert sleep.mock_calls == [mock.call(5), mock.call(10), mock.call(15), mock.call(20)]

    async def test_fails(self, faker: Faker) -> None:
        connection_issues_generator = (
            build_dataclass(issue_type) for issue_type in itertools.cycle(get_args(AnyConnectionIssue))
        )
        attempts = faker.pyint(min_value=1, max_value=10)
        connection_issues = [next(connection_issues_generator) for _ in range(attempts)]

        with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
            await attempt_to_connect(
                connect=mock.AsyncMock(side_effect=connection_issues),
                connect_retry_interval=faker.pyint(),
                connect_retry_attempts=attempts,
                sleep=mock.AsyncMock(),
            )

        assert exc_info.value.issues == connection_issues
        assert exc_info.value.retry_attempts == attempts


ReturnType = TypeVar("ReturnType")


def return_arg_async(arg: ReturnType) -> Awaitable[ReturnType]:
    async def inner() -> ReturnType:  # noqa: RUF029
        return arg

    return inner()


class TestConnectToFirstServer:
    async def test_ok(self) -> None:
        active_connection_state = mock.Mock()
        awaitables: list[Awaitable[ActiveConnectionState | None]] = [
            return_arg_async(None),
            return_arg_async(None),
            return_arg_async(active_connection_state),
            return_arg_async(None),
        ]

        assert await connect_to_first_server(awaitables) is active_connection_state

    async def test_fails(self) -> None:
        awaitables: list[Awaitable[ActiveConnectionState | None]] = [
            return_arg_async(None),
            return_arg_async(None),
            return_arg_async(None),
            return_arg_async(None),
        ]

        assert await connect_to_first_server(awaitables) is None


class TestMakeHealthyConnection:
    async def test_ok(self) -> None:
        active_connection_state = ActiveConnectionState(
            connection=mock.AsyncMock(), lifespan=mock.AsyncMock(enter=mock.AsyncMock(side_effect=[None]))
        )

        result = await make_healthy_connection(
            active_connection_state=active_connection_state, servers=[], connect_timeout=0
        )

        assert result is active_connection_state

    async def test_no_active_state(self, faker: Faker) -> None:
        servers = [build_dataclass(ConnectionParameters) for _ in range(faker.pyint(max_value=10))]
        connect_timeout = faker.pyint()

        result = await make_healthy_connection(
            active_connection_state=None, servers=servers, connect_timeout=connect_timeout
        )

        assert result == AllServersUnavailable(servers=servers, timeout=connect_timeout)

    async def test_connection_lost(self) -> None:
        active_connection_state = ActiveConnectionState(
            connection=mock.AsyncMock(), lifespan=mock.AsyncMock(enter=mock.AsyncMock(side_effect=ConnectionLostError))
        )

        result = await make_healthy_connection(
            active_connection_state=active_connection_state, servers=[], connect_timeout=0
        )

        assert result == ConnectionLost()

    @pytest.mark.parametrize("issue_type", get_args(StompProtocolConnectionIssue))
    async def test_stomp_protocol_issue(self, issue_type: type[StompProtocolConnectionIssue]) -> None:
        issue = build_dataclass(issue_type)
        active_connection_state = ActiveConnectionState(
            connection=mock.AsyncMock(), lifespan=mock.AsyncMock(enter=mock.AsyncMock(side_effect=[issue]))
        )

        result = await make_healthy_connection(
            active_connection_state=active_connection_state, servers=[], connect_timeout=0
        )

        assert result == issue


async def test_get_active_connection_state_concurrency() -> None:
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


class TestConnectionManagerContext:
    async def test_connection_lost(self) -> None:
        async with EnrichedConnectionManager(connection_class=mock.AsyncMock()) as manager:
            manager._clear_active_connection_state()

    async def test_lifespan_exit_raises_connection_lost(self) -> None:
        enter_mock = mock.AsyncMock(side_effect=[None])
        exit_mock = mock.AsyncMock(side_effect=[ConnectionLostError])
        lifespan_factory = mock.Mock(return_value=mock.Mock(enter=enter_mock, exit=exit_mock))

        async with EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=mock.AsyncMock()):
            pass

        enter_mock.assert_called_once_with()
        exit_mock.assert_called_once_with()

    async def test_exits_ok(self) -> None:
        lifespan_exit = mock.AsyncMock()
        lifespan_factory = mock.Mock(
            return_value=mock.Mock(enter=mock.AsyncMock(side_effect=[None]), exit=lifespan_exit)
        )

        connection_close = mock.AsyncMock()

        class MockConnection(BaseMockConnection):
            close = connection_close

        async with EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=MockConnection):
            pass

        lifespan_exit.assert_called_once()
        connection_close.assert_called_once_with()


RECONNECTING_SIDE_EFFECTS = [
    (None,),
    (ConnectionLostError(), None),
    (ConnectionLostError(), ConnectionLostError(), None),
]


class TestWriteHeartbeatReconnecting:
    @pytest.mark.parametrize("side_effect", RECONNECTING_SIDE_EFFECTS)
    async def test_ok(self, side_effect: tuple[None | ConnectionLostError, ...]) -> None:
        write_heartbeat_mock = mock.Mock(side_effect=side_effect)

        class MockConnection(BaseMockConnection):
            write_heartbeat = write_heartbeat_mock

        manager = EnrichedConnectionManager(connection_class=MockConnection)
        await manager.write_heartbeat_reconnecting()
        assert len(write_heartbeat_mock.mock_calls) == len(side_effect)

    async def test_raises(self) -> None:
        class MockConnection(BaseMockConnection):
            write_heartbeat = mock.Mock(side_effect=[ConnectionLostError, ConnectionLostError, ConnectionLostError])

        manager = EnrichedConnectionManager(connection_class=MockConnection)

        with pytest.raises(FailedAllWriteAttemptsError):
            await manager.write_heartbeat_reconnecting()


class TestWriteFrameReconnecting:
    @pytest.mark.parametrize("side_effect", RECONNECTING_SIDE_EFFECTS)
    async def test_ok(self, side_effect: tuple[None | ConnectionLostError, ...]) -> None:
        write_frame_mock = mock.AsyncMock(side_effect=side_effect)

        class MockConnection(BaseMockConnection):
            write_frame = write_frame_mock

        manager = EnrichedConnectionManager(connection_class=MockConnection)
        await manager.write_frame_reconnecting(frame := build_dataclass(ConnectFrame))
        assert write_frame_mock.mock_calls == [mock.call(frame)] * len(side_effect)

    async def test_raises(self) -> None:
        class MockConnection(BaseMockConnection):
            write_frame = mock.AsyncMock(side_effect=[ConnectionLostError, ConnectionLostError, ConnectionLostError])

        manager = EnrichedConnectionManager(connection_class=MockConnection)

        with pytest.raises(FailedAllWriteAttemptsError):
            await manager.write_frame_reconnecting(build_dataclass(ConnectFrame))


@pytest.mark.parametrize("side_effect", RECONNECTING_SIDE_EFFECTS)
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


class TestMaybeWriteFrame:
    async def test_ok(self) -> None:
        async with EnrichedConnectionManager(connection_class=mock.AsyncMock()) as manager:
            assert await manager.maybe_write_frame(build_dataclass(ConnectFrame))

    async def test_connection_now_lost(self) -> None:
        class MockConnection(BaseMockConnection):
            write_frame = mock.AsyncMock(side_effect=[ConnectionLostError])

        async with EnrichedConnectionManager(connection_class=MockConnection) as manager:
            assert not await manager.maybe_write_frame(build_dataclass(ConnectFrame))

    async def test_connection_already_lost(self) -> None:
        manager = EnrichedConnectionManager(connection_class=mock.AsyncMock())
        assert not await manager.maybe_write_frame(build_dataclass(ConnectFrame))
