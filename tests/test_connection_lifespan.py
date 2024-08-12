import asyncio
from collections.abc import AsyncIterable, Coroutine, Iterable
from functools import partial
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
    _make_receipt_id,  # noqa: PLC2701
    calculate_heartbeat_interval,
    check_stomp_protocol_version,
    take_connected_frame,
    wait_for_receipt_frame,
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


class TestTakeConnectedFrame:
    @pytest.mark.parametrize(
        "frame_types",
        [[ConnectedFrame], [MessageFrame, HeartbeatFrame, ConnectedFrame], [HeartbeatFrame, ConnectedFrame]],
    )
    async def test_ok(self, monkeypatch: pytest.MonkeyPatch, faker: Faker, frame_types: list[type[Any]]) -> None:
        wait_for_mock = mock.AsyncMock(side_effect=partial(asyncio.wait_for, timeout=0))
        monkeypatch.setattr("asyncio.wait_for", wait_for_mock)
        timeout = faker.pyint()

        result = await take_connected_frame(
            frames_iter=make_async_iter(build_dataclass(frame_type) for frame_type in frame_types),
            connection_confirmation_timeout=timeout,
        )

        assert isinstance(result, ConnectedFrame)
        wait_for_mock.assert_called_once()
        assert wait_for_mock.mock_calls[0].kwargs["timeout"] == timeout

    async def test_unreachable(self) -> None:
        with pytest.raises(AssertionError, match="unreachable"):
            await take_connected_frame(frames_iter=make_async_iter([]), connection_confirmation_timeout=1)

    async def test_timeout(self, monkeypatch: pytest.MonkeyPatch, faker: Faker) -> None:
        original_wait_for = asyncio.wait_for

        async def mock_wait_for(future: Coroutine[Any, Any, Any], timeout: float) -> object:
            task = asyncio.create_task(future)
            await asyncio.sleep(0)
            return await original_wait_for(task, 0)

        monkeypatch.setattr("asyncio.wait_for", mock_wait_for)
        timeout = faker.pyint()

        result = await take_connected_frame(
            frames_iter=make_async_iter([HeartbeatFrame()]), connection_confirmation_timeout=timeout
        )

        assert result == ConnectionConfirmationTimeout(timeout=timeout, frames=[HeartbeatFrame()])


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


class TestWaitForReceiptFrame:
    @pytest.mark.parametrize(
        "frame_types",
        [[ReceiptFrame], [MessageFrame, HeartbeatFrame, ReceiptFrame], [HeartbeatFrame, ReceiptFrame]],
    )
    async def test_ok(self, monkeypatch: pytest.MonkeyPatch, faker: Faker, frame_types: list[type[Any]]) -> None:
        wait_for_mock = mock.AsyncMock(side_effect=partial(asyncio.wait_for, timeout=0))
        monkeypatch.setattr("asyncio.wait_for", wait_for_mock)
        timeout = faker.pyint()

        await wait_for_receipt_frame(
            frames_iter=make_async_iter(build_dataclass(frame_type) for frame_type in frame_types),
            disconnect_confirmation_timeout=timeout,
        )

        wait_for_mock.assert_called_once()
        assert wait_for_mock.mock_calls[0].kwargs["timeout"] == timeout

    @pytest.mark.parametrize("frame_types", [[HeartbeatFrame, HeartbeatFrame, HeartbeatFrame], []])
    async def test_timeout(self, monkeypatch: pytest.MonkeyPatch, faker: Faker, frame_types: list[type[Any]]) -> None:
        original_wait_for = asyncio.wait_for

        async def mock_wait_for(future: Coroutine[Any, Any, Any], timeout: float) -> object:
            task = asyncio.create_task(future)
            await asyncio.sleep(0)
            return await original_wait_for(task, 0)

        monkeypatch.setattr("asyncio.wait_for", mock_wait_for)

        await wait_for_receipt_frame(
            frames_iter=make_async_iter(build_dataclass(frame_type) for frame_type in frame_types),
            disconnect_confirmation_timeout=faker.pyint(),
        )


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


async def test_connection_lifespan_exit(faker: Faker, monkeypatch: pytest.MonkeyPatch) -> None:
    receipt_id = faker.pystr()
    monkeypatch.setattr("stompman.connection_lifespan._make_receipt_id", lambda: receipt_id)

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
    )
    await connection_lifespan.exit()

    assert written_and_read_frames == [
        *unsubscribe_frames,
        DisconnectFrame(headers={"receipt": receipt_id}),
        receipt_frame,
    ]


def test_make_receipt_id() -> None:
    _make_receipt_id()
