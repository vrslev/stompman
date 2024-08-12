import asyncio
import itertools
from collections.abc import AsyncGenerator, AsyncIterable
from typing import Any, Self, get_args
from unittest import mock

import faker
import pytest

from stompman import (
    AnyServerFrame,
    ConnectedFrame,
    ConnectFrame,
    ConnectionLostError,
    ErrorFrame,
    FailedAllConnectAttemptsError,
    FailedAllWriteAttemptsError,
    MessageFrame,
)
from stompman.connection_manager import ActiveConnectionState, attempt_to_connect, connect_to_first_server
from stompman.errors import AnyConnectionIssue
from tests.conftest import BaseMockConnection, EnrichedConnectionManager, build_dataclass

pytestmark = [pytest.mark.anyio, pytest.mark.usefixtures("mock_sleep")]
FAKER = faker.Faker()


async def test_attempt_to_connect_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asyncio.sleep", (sleep_mock := mock.AsyncMock()))
    active_connection_state = ActiveConnectionState(connection=mock.Mock(), lifespan=mock.Mock())
    connection_issues = [build_dataclass(issue_type) for issue_type in get_args(AnyConnectionIssue)]

    result = await attempt_to_connect(
        connect=mock.AsyncMock(side_effect=[*connection_issues, active_connection_state, connection_issues[0]]),
        connect_retry_interval=5,
        connect_retry_attempts=len(connection_issues) + 1,
    )

    assert result is active_connection_state
    assert sleep_mock.mock_calls == [mock.call(5), mock.call(10), mock.call(15), mock.call(20)]


async def test_attempt_to_connect_fails() -> None:
    connection_issues_generator = (
        build_dataclass(issue_type) for issue_type in itertools.cycle(get_args(AnyConnectionIssue))
    )
    attempts = FAKER.pyint(min_value=1, max_value=10)
    connection_issues = [next(connection_issues_generator) for _ in range(attempts)]

    with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
        await attempt_to_connect(
            connect=mock.AsyncMock(side_effect=connection_issues),
            connect_retry_interval=FAKER.pyint(),
            connect_retry_attempts=attempts,
        )

    assert exc_info.value.issues == connection_issues
    assert exc_info.value.retry_attempts == attempts


@pytest.mark.parametrize("ok_on_attempt", [1, 2, 3])
async def test_connect_attempts_ok(ok_on_attempt: int, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(
            cls, *, host: str, port: int, timeout: int, read_max_chunk_size: int, read_timeout: int
        ) -> Self | None:
            assert (host, port) == (manager.servers[0].host, manager.servers[0].port)
            nonlocal attempts
            attempts += 1

            return (
                await super().connect(
                    host=host,
                    port=port,
                    timeout=timeout,
                    read_max_chunk_size=read_max_chunk_size,
                    read_timeout=read_timeout,
                )
                if attempts == ok_on_attempt
                else None
            )

    sleep_mock = mock.AsyncMock()
    monkeypatch.setattr("asyncio.sleep", sleep_mock)
    manager = EnrichedConnectionManager(connection_class=MockConnection)
    active_connection_state = await manager._get_active_connection_state()
    assert isinstance(active_connection_state, ActiveConnectionState)
    assert attempts == ok_on_attempt == (len(sleep_mock.mock_calls) + 1)


async def test_connect_to_one_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        connect = mock.AsyncMock(return_value=None)

    manager = EnrichedConnectionManager(connection_class=MockConnection)
    assert await manager._create_connection_to_one_server(manager.servers[0]) is None


async def return_argument_async(arg: Any) -> Any:  # noqa: ANN401, RUF029
    return arg


async def test_connect_to_first_server_ok() -> None:
    expected_active_connection_state = ActiveConnectionState(connection=mock.AsyncMock(), lifespan=mock.AsyncMock())
    active_connection_state = await connect_to_first_server(
        [
            return_argument_async(None),
            return_argument_async(None),
            return_argument_async(expected_active_connection_state),
            return_argument_async(None),
        ]
    )
    assert active_connection_state is expected_active_connection_state


async def test_connect_to_first_server_fails() -> None:
    active_connection_state = await connect_to_first_server(
        [
            return_argument_async(None),
            return_argument_async(None),
            return_argument_async(None),
            return_argument_async(None),
        ]
    )
    assert active_connection_state is None


async def test_get_active_connection_state_lifespan_flaky_ok() -> None:
    enter = mock.AsyncMock(side_effect=[ConnectionLostError, None])
    lifespan_factory = mock.Mock(return_value=mock.Mock(enter=enter))
    manager = EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=BaseMockConnection)

    await manager._get_active_connection_state()

    assert enter.mock_calls == [mock.call(), mock.call()]
    assert lifespan_factory.mock_calls == [
        mock.call(connection=BaseMockConnection(), connection_parameters=manager.servers[0]),
        mock.call(connection=BaseMockConnection(), connection_parameters=manager.servers[0]),
    ]


async def test_get_active_connection_state_lifespan_flaky_fails() -> None:
    enter = mock.AsyncMock(side_effect=ConnectionLostError)
    lifespan_factory = mock.Mock(return_value=mock.Mock(enter=enter))
    manager = EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=BaseMockConnection)

    with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
        await manager._get_active_connection_state()

    assert (
        exc_info.value.retry_attempts
        == len(lifespan_factory.mock_calls)
        == len(enter.mock_calls)
        == manager.connect_retry_attempts
    )


async def test_get_active_connection_state_fails_to_connect() -> None:
    class MockConnection(BaseMockConnection):
        connect = mock.AsyncMock(return_value=None)

    with pytest.raises(FailedAllConnectAttemptsError):
        await EnrichedConnectionManager(connection_class=MockConnection)._get_active_connection_state()


async def test_get_active_connection_state_ok_concurrent() -> None:
    enter = mock.AsyncMock(return_value=None)
    lifespan_factory = mock.Mock(return_value=mock.Mock(enter=enter))
    manager = EnrichedConnectionManager(lifespan_factory=lifespan_factory, connection_class=BaseMockConnection)

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
        == ActiveConnectionState(connection=BaseMockConnection(), lifespan=lifespan_factory.return_value)
    )
    assert first_state is second_state is third_state is fourth_state

    enter.assert_called_once_with()
    lifespan_factory.assert_called_once_with(connection=BaseMockConnection(), connection_parameters=manager.servers[0])


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
