import asyncio
from collections.abc import AsyncIterable, Awaitable, Iterable
from typing import Any, TypeVar, get_args
from unittest import mock

import pytest
from faker import Faker

from stompman import (
    AnyServerFrame,
    ConnectedFrame,
    ConnectionConfirmationTimeout,
    ReceiptFrame,
    UnsupportedProtocolVersion,
)
from stompman.config import ConnectionParameters, Heartbeat
from stompman.connection_lifespan import (
    ConnectionLifespan,
    WaitForFutureReturnType,
    calculate_heartbeat_interval,
    check_stomp_protocol_version,
    take_frame_of_type,
    wait_for_or_none,
)
from stompman.frames import (
    AckMode,
    AnyClientFrame,
    CommitFrame,
    ConnectFrame,
    DisconnectFrame,
    HeartbeatFrame,
    MessageFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)
from stompman.subscription import ActiveSubscriptions, Subscription
from stompman.transaction import Transaction
from tests.conftest import build_dataclass, noop_error_handler, noop_message_handler

pytestmark = pytest.mark.anyio

IterableItemT = TypeVar("IterableItemT")


async def make_async_iter(iterable: Iterable[IterableItemT]) -> AsyncIterable[IterableItemT]:
    for item in iterable:
        yield item
    await asyncio.sleep(0)


class TestWaitForOrNone:
    async def test_ok(self) -> None:
        async def return_foo_after_tick() -> str:
            await asyncio.sleep(0)
            return "foo"

        assert await wait_for_or_none(return_foo_after_tick(), timeout=1) == "foo"

    async def test_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("asyncio.wait_for", mock.AsyncMock(side_effect=TimeoutError))
        assert await wait_for_or_none(mock.AsyncMock(), timeout=1000) is None


class TestTakeFrameOfType:
    @pytest.mark.parametrize(
        "frame_types",
        [[ConnectedFrame], [MessageFrame, HeartbeatFrame, ConnectedFrame], [HeartbeatFrame, ConnectedFrame]],
    )
    async def test_ok(self, frame_types: list[type[Any]]) -> None:
        expected_timeout = 10

        async def wait_for_or_none(
            awaitable: Awaitable[WaitForFutureReturnType], timeout: float
        ) -> WaitForFutureReturnType:
            assert timeout == expected_timeout
            return await awaitable

        result = await take_frame_of_type(
            frame_type=ConnectedFrame,
            frames_iter=make_async_iter(build_dataclass(frame_type) for frame_type in frame_types),
            timeout=expected_timeout,
            wait_for_or_none=wait_for_or_none,
        )
        assert isinstance(result, ConnectedFrame)

    async def test_unreachable(self) -> None:
        with pytest.raises(AssertionError, match="unreachable"):
            await take_frame_of_type(
                frame_type=HeartbeatFrame,
                frames_iter=make_async_iter([]),
                timeout=1,
                wait_for_or_none=wait_for_or_none,
            )

    async def test_timeout(self) -> None:
        async def wait_for_or_none(awaitable: Awaitable[WaitForFutureReturnType], timeout: float) -> None:
            async def coro() -> None:
                await awaitable

            task = asyncio.create_task(coro())
            await asyncio.sleep(0)
            task.cancel()

        result = await take_frame_of_type(
            frame_type=ConnectedFrame,
            frames_iter=make_async_iter([HeartbeatFrame()]),
            timeout=1,
            wait_for_or_none=wait_for_or_none,
        )
        assert result == [HeartbeatFrame()]


class TestCheckStompProtocolVersion:
    def test_ok(self, faker: Faker) -> None:
        supported_version = faker.pystr()
        connected_frame = build_dataclass(ConnectedFrame, headers={"version": supported_version})

        result = check_stomp_protocol_version(connected_frame=connected_frame, supported_version=supported_version)
        assert result is None

    def test_unsupported(self, faker: Faker) -> None:
        supported_version = faker.pystr()
        given_version = faker.pystr()
        connected_frame = build_dataclass(ConnectedFrame, headers={"version": given_version})

        result = check_stomp_protocol_version(connected_frame=connected_frame, supported_version=supported_version)
        assert result == UnsupportedProtocolVersion(given_version=given_version, supported_version=supported_version)


@pytest.mark.parametrize(
    ("client_declares", "server_asks", "expected_result"), [(900, 1000, 1), (900, 800, 0.9), (900, 900, 0.9)]
)
def test_calculate_heartbeat_interval(
    faker: Faker, client_declares: int, server_asks: int, expected_result: float
) -> None:
    server_heartbeat = Heartbeat(will_send_interval_ms=faker.pyint(), want_to_receive_interval_ms=server_asks)
    connected_frame = build_dataclass(ConnectedFrame, headers={"heart-beat": server_heartbeat.to_header()})
    client_heartbeat = Heartbeat(will_send_interval_ms=client_declares, want_to_receive_interval_ms=faker.pyint())

    result = calculate_heartbeat_interval(connected_frame=connected_frame, client_heartbeat=client_heartbeat)
    assert result == expected_result


class TestConnectionLifespanEnter:
    async def test_ok(self, faker: Faker) -> None:
        protocol_version = faker.pystr()
        connected_frame = build_dataclass(ConnectedFrame, headers={"version": protocol_version, "heart-beat": "1,1"})
        client_heartbeat = Heartbeat.from_header("1,1")
        subscriptions_list = [
            Subscription(
                destination=faker.pystr(),
                handler=noop_message_handler,
                ack=faker.random_element(get_args(AckMode)),
                on_suppressed_exception=noop_error_handler,
                supressed_exception_classes=(),
                _connection_manager=mock.Mock(),
                _active_subscriptions=mock.Mock(),
            )
            for _ in range(4)
        ]
        active_subscriptions = {subscription.id: subscription for subscription in subscriptions_list}
        active_transactions = [
            Transaction(_connection_manager=mock.Mock(), _active_transactions=mock.Mock()) for _ in range(4)
        ]
        connection_parameters = build_dataclass(ConnectionParameters)
        written_and_read_frames = []
        set_heartbeat_interval = mock.Mock(side_effect=[None])

        async def mock_read_frames(iterable: Iterable[AnyServerFrame]) -> AsyncIterable[AnyServerFrame]:
            async for frame in make_async_iter(iterable):
                written_and_read_frames.append(frame)
                yield frame

        connection_lifespan = ConnectionLifespan(
            connection=mock.AsyncMock(
                write_frame=mock.AsyncMock(side_effect=written_and_read_frames.append),
                read_frames=mock.Mock(side_effect=[mock_read_frames([connected_frame])]),
            ),
            connection_parameters=connection_parameters,
            protocol_version=protocol_version,
            client_heartbeat=client_heartbeat,
            connection_confirmation_timeout=faker.pyint(min_value=1),
            disconnect_confirmation_timeout=faker.pyint(),
            active_subscriptions=active_subscriptions,
            active_transactions=active_transactions.copy(),  # type: ignore[arg-type]
            set_heartbeat_interval=set_heartbeat_interval,
        )

        await connection_lifespan.enter()
        set_heartbeat_interval.assert_called_once_with(0.001)
        assert written_and_read_frames == [
            ConnectFrame(
                headers={
                    "accept-version": protocol_version,
                    "heart-beat": client_heartbeat.to_header(),
                    "host": connection_parameters.host,
                    "login": connection_parameters.login,
                    "passcode": connection_parameters.unescaped_passcode,
                },
            ),
            connected_frame,
            *(
                SubscribeFrame(
                    headers={"ack": subscription.ack, "destination": subscription.destination, "id": subscription.id},
                )
                for subscription in active_subscriptions.values()
            ),
            *(CommitFrame(headers={"transaction": transaction.id}) for transaction in active_transactions),
        ]

    async def test_confirmation_timeout(self, faker: Faker) -> None:
        connection_lifespan = ConnectionLifespan(
            connection=mock.AsyncMock(read_frames=mock.Mock(side_effect=[make_async_iter([])])),
            connection_parameters=mock.Mock(),
            protocol_version=faker.pystr(),
            client_heartbeat=mock.Mock(),
            connection_confirmation_timeout=0,
            disconnect_confirmation_timeout=faker.pyint(),
            active_subscriptions={},
            active_transactions=set(),
            set_heartbeat_interval=mock.Mock(),
        )

        assert await connection_lifespan.enter() == ConnectionConfirmationTimeout(timeout=0, frames=[])

    async def test_unsupported_protocol(self, faker: Faker) -> None:
        supported_version = faker.pystr()
        given_version = faker.pystr()
        connected_frame = build_dataclass(ConnectedFrame, headers={"version": given_version})

        connection_lifespan = ConnectionLifespan(
            connection=mock.AsyncMock(read_frames=mock.Mock(side_effect=[make_async_iter([connected_frame])])),
            connection_parameters=mock.Mock(),
            protocol_version=supported_version,
            client_heartbeat=mock.Mock(),
            connection_confirmation_timeout=faker.pyint(min_value=1),
            disconnect_confirmation_timeout=faker.pyint(),
            active_subscriptions={},
            active_transactions=set(),
            set_heartbeat_interval=mock.Mock(),
        )

        assert await connection_lifespan.enter() == UnsupportedProtocolVersion(
            given_version=given_version, supported_version=supported_version
        )


async def test_connection_lifespan_exit(faker: Faker) -> None:
    written_and_read_frames: list[AnyServerFrame | AnyClientFrame] = []
    connection_manager = mock.Mock(maybe_write_frame=mock.AsyncMock(side_effect=written_and_read_frames.append))
    active_subscriptions: ActiveSubscriptions = {}
    subscriptions_list = [
        Subscription(
            destination=faker.pystr(),
            handler=noop_message_handler,
            ack=faker.random_element(get_args(AckMode)),
            on_suppressed_exception=noop_error_handler,
            supressed_exception_classes=(),
            _connection_manager=connection_manager,
            _active_subscriptions=active_subscriptions,
        )
        for _ in range(4)
    ]
    active_subscriptions |= {subscription.id: subscription for subscription in subscriptions_list}
    unsubscribe_frames = [
        UnsubscribeFrame(headers={"id": subscription.id}) for subscription in active_subscriptions.values()
    ]
    active_transactions_mock = mock.Mock()
    active_transactions = [
        Transaction(_connection_manager=connection_manager, _active_transactions=active_transactions_mock)
        for _ in range(4)
    ]
    receipt_id = faker.pystr()
    receipt_frame = build_dataclass(ReceiptFrame)

    async def mock_read_frames(iterable: Iterable[AnyServerFrame]) -> AsyncIterable[AnyServerFrame]:
        async for frame in make_async_iter(iterable):
            written_and_read_frames.append(frame)
            yield frame

    connection_lifespan = ConnectionLifespan(
        connection=mock.AsyncMock(
            write_frame=mock.AsyncMock(side_effect=written_and_read_frames.append),
            read_frames=mock.Mock(side_effect=[mock_read_frames([receipt_frame])]),
        ),
        connection_parameters=mock.Mock(),
        protocol_version=faker.pystr(),
        client_heartbeat=mock.Mock(),
        connection_confirmation_timeout=faker.pyint(min_value=1),
        disconnect_confirmation_timeout=faker.pyint(),
        active_subscriptions=active_subscriptions,
        active_transactions=active_transactions,  # type: ignore[arg-type]
        set_heartbeat_interval=mock.Mock(),
        _generate_receipt_id=lambda: receipt_id,
    )
    await connection_lifespan.exit()

    assert written_and_read_frames == [
        *unsubscribe_frames,
        DisconnectFrame(headers={"receipt": receipt_id}),
        receipt_frame,
    ]
