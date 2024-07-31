import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import suppress
from typing import Any
from unittest import mock

import faker
import pytest

import stompman.client
from stompman import (
    AbortFrame,
    AbstractConnection,
    AckFrame,
    AnyClientFrame,
    AnyServerFrame,
    BeginFrame,
    CommitFrame,
    ConnectedFrame,
    ConnectFrame,
    ConnectionConfirmationTimeoutError,
    DisconnectFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    NackFrame,
    ReceiptFrame,
    SendFrame,
    SubscribeFrame,
    UnsubscribeFrame,
    UnsupportedProtocolVersionError,
)
from stompman.client import AckMode, Client
from stompman.config import ConnectionParameters
from tests.conftest import (
    BaseMockConnection,
    EnrichedClient,
    SomeError,
    build_dataclass,
    noop_error_handler,
    noop_message_handler,
)

pytestmark = pytest.mark.anyio
FAKER = faker.Faker()


def create_spying_connection(
    *read_frames_yields: list[AnyServerFrame],
) -> tuple[type[AbstractConnection], list[AnyClientFrame | AnyServerFrame]]:
    class BaseCollectingConnection(BaseMockConnection):
        @staticmethod
        async def write_frame(frame: AnyClientFrame) -> None:
            collected_frames.append(frame)

        @staticmethod
        async def read_frames() -> AsyncGenerator[AnyServerFrame, None]:
            for frame in next(read_frames_iterator):
                collected_frames.append(frame)
                yield frame

    read_frames_iterator = iter(read_frames_yields)
    collected_frames: list[AnyClientFrame | AnyServerFrame] = []
    return BaseCollectingConnection, collected_frames


def get_read_frames_with_lifespan(*read_frames: list[AnyServerFrame]) -> list[list[AnyServerFrame]]:
    return [
        [ConnectedFrame(headers={"version": Client.PROTOCOL_VERSION, "heart-beat": "1,1"})],
        *read_frames,
        [ReceiptFrame(headers={"receipt-id": "receipt-id-1"})],
    ]


def enrich_expected_frames(*expected_frames: AnyClientFrame | AnyServerFrame) -> list[AnyClientFrame | AnyServerFrame]:
    return [
        ConnectFrame(
            headers={
                "accept-version": Client.PROTOCOL_VERSION,
                "heart-beat": "1000,1000",
                "host": "localhost",
                "login": "login",
                "passcode": "passcode",
            },
        ),
        ConnectedFrame(headers={"version": Client.PROTOCOL_VERSION, "heart-beat": "1,1"}),
        *expected_frames,
        DisconnectFrame(headers={"receipt": "receipt-id-1"}),
        ReceiptFrame(headers={"receipt-id": "receipt-id-1"}),
    ]


@pytest.fixture(autouse=True)
def _mock_receipt_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stompman.client, "_make_receipt_id", lambda: "receipt-id-1")


async def test_client_lifespan_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    connected_frame = build_dataclass(ConnectedFrame, headers={"version": Client.PROTOCOL_VERSION, "heart-beat": "1,1"})
    connection_class, collected_frames = create_spying_connection(
        [connected_frame], [], [(receipt_frame := build_dataclass(ReceiptFrame))]
    )

    disconnect_frame = DisconnectFrame(headers={"receipt": (receipt_id := FAKER.pystr())})
    monkeypatch.setattr(stompman.client, "_make_receipt_id", mock.Mock(return_value=receipt_id))

    async with EnrichedClient(
        [ConnectionParameters("localhost", 10, "login", "%3Dpasscode")], connection_class=connection_class
    ) as client:
        await asyncio.sleep(0)

    connect_frame = ConnectFrame(
        headers={
            "host": "localhost",
            "accept-version": Client.PROTOCOL_VERSION,
            "heart-beat": client.heartbeat.to_header(),
            "login": "login",
            "passcode": "=passcode",
        }
    )
    assert collected_frames == [connect_frame, connected_frame, disconnect_frame, receipt_frame]


async def test_client_lifespan_connection_not_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_wait_for(future: Coroutine[Any, Any, Any], timeout: float) -> object:
        assert timeout == connection_confirmation_timeout
        task = asyncio.create_task(future)
        await asyncio.sleep(0)
        return await original_wait_for(task, 0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", mock_wait_for)
    error_frame = build_dataclass(ErrorFrame)
    connection_confirmation_timeout = FAKER.pyint()

    class MockConnection(BaseMockConnection):
        @staticmethod
        async def read_frames() -> AsyncGenerator[AnyServerFrame, None]:
            yield error_frame
            await asyncio.sleep(0)

    with pytest.raises(ConnectionConfirmationTimeoutError) as exc_info:
        await EnrichedClient(  # noqa: PLC2801
            connection_class=MockConnection, connection_confirmation_timeout=connection_confirmation_timeout
        ).__aenter__()

    assert exc_info.value == ConnectionConfirmationTimeoutError(
        timeout=connection_confirmation_timeout, frames=[error_frame]
    )


async def test_client_lifespan_unsupported_protocol_version() -> None:
    given_version = FAKER.pystr()

    with pytest.raises(UnsupportedProtocolVersionError) as exc_info:
        await EnrichedClient(  # noqa: PLC2801
            connection_class=create_spying_connection(
                [build_dataclass(ConnectedFrame, headers={"version": given_version})]
            )[0]
        ).__aenter__()

    assert exc_info.value == UnsupportedProtocolVersionError(
        given_version=given_version, supported_version=Client.PROTOCOL_VERSION
    )


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


async def test_client_subscribe_lifespan_no_active_subs_in_aexit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        stompman.client,
        "_make_subscription_id",
        mock.Mock(side_effect=[(first_id := FAKER.pystr()), (second_id := FAKER.pystr())]),
    )
    first_destination, second_destination = FAKER.pystr(), FAKER.pystr()
    connection_class, collected_frames = create_spying_connection(*get_read_frames_with_lifespan([]))

    async with EnrichedClient(connection_class=connection_class) as client:
        first_subscription = await client.subscribe(
            first_destination, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
        )
        second_subscription = await client.subscribe(
            second_destination, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
        )
        await asyncio.sleep(0)
        await first_subscription.unsubscribe()
        await second_subscription.unsubscribe()

    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"id": first_id, "destination": first_destination, "ack": "client-individual"}),
        SubscribeFrame(headers={"id": second_id, "destination": second_destination, "ack": "client-individual"}),
        UnsubscribeFrame(headers={"id": first_id}),
        UnsubscribeFrame(headers={"id": second_id}),
    )


@pytest.mark.parametrize("direct_error", [True, False])
async def test_client_subscribe_lifespan_with_active_subs_in_aexit(
    monkeypatch: pytest.MonkeyPatch,
    direct_error: bool,  # noqa: FBT001
) -> None:
    subscription_id, destination = FAKER.pystr(), FAKER.pystr()
    monkeypatch.setattr(stompman.client, "_make_subscription_id", mock.Mock(return_value=subscription_id))
    connection_class, collected_frames = create_spying_connection(*get_read_frames_with_lifespan([]))

    if direct_error:
        with pytest.raises(SomeError):  # noqa: PT012
            async with EnrichedClient(connection_class=connection_class) as client:
                await client.subscribe(
                    destination, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
                )
                await SomeError.raise_after_tick()
    else:
        with pytest.raises(ExceptionGroup) as exc_info:  # noqa: PT012
            async with asyncio.TaskGroup() as task_group, EnrichedClient(connection_class=connection_class) as client:
                await client.subscribe(
                    destination, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
                )
                task_group.create_task(SomeError.raise_after_tick())

        assert exc_info.value.exceptions == (SomeError(),)

    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"id": subscription_id, "destination": destination, "ack": "client-individual"}),
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


async def test_client_listen_routing_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        stompman.client,
        "_make_subscription_id",
        mock.Mock(side_effect=[(first_sub_id := FAKER.pystr()), (second_sub_id := FAKER.pystr())]),
    )
    connection_class, _ = create_spying_connection(
        *get_read_frames_with_lifespan(
            [
                build_dataclass(ConnectedFrame),
                build_dataclass(ReceiptFrame),
                (first_message_frame := build_dataclass(MessageFrame, headers={"subscription": first_sub_id})),
                (error_frame := build_dataclass(ErrorFrame)),
                (second_message_frame := build_dataclass(MessageFrame)),
                (third_message_frame := build_dataclass(MessageFrame, headers={"subscription": second_sub_id})),
                HeartbeatFrame(),
            ]
        )
    )
    first_message_handler, first_error_handler = mock.AsyncMock(return_value=None), mock.Mock()
    second_message_handler, second_error_handler = mock.AsyncMock(side_effect=SomeError), mock.Mock()

    async with EnrichedClient(
        connection_class=connection_class,
        on_error_frame=(on_error_frame := mock.Mock()),
        on_heartbeat=(on_heartbeat := mock.Mock()),
        on_unhandled_message_frame=(on_unhandled_message_frame := mock.Mock()),
    ) as client:
        first_subscription = await client.subscribe(
            FAKER.pystr(), handler=first_message_handler, on_suppressed_exception=first_error_handler
        )
        second_subscription = await client.subscribe(
            FAKER.pystr(), handler=second_message_handler, on_suppressed_exception=second_error_handler
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await first_subscription.unsubscribe()
        await second_subscription.unsubscribe()

    first_message_handler.assert_called_once_with(first_message_frame)
    first_error_handler.assert_not_called()

    second_message_handler.assert_called_once_with(third_message_frame)
    second_error_handler.assert_called_once_with(SomeError(), third_message_frame)

    on_error_frame.assert_called_once_with(error_frame)
    on_heartbeat.assert_called_once_with()
    on_unhandled_message_frame.assert_called_once_with(second_message_frame)


@pytest.mark.parametrize("side_effect", [None, SomeError])
@pytest.mark.parametrize("ack", ["client", "client-individual"])
async def test_client_listen_unsubscribe_before_ack_or_nack(
    monkeypatch: pytest.MonkeyPatch, ack: AckMode, side_effect: object
) -> None:
    subscription_id, destination = FAKER.pystr(), FAKER.pystr()
    monkeypatch.setattr(stompman.client, "_make_subscription_id", mock.Mock(return_value=subscription_id))

    message_frame = build_dataclass(MessageFrame, headers={"subscription": subscription_id})
    connection_class, collected_frames = create_spying_connection(*get_read_frames_with_lifespan([message_frame]))
    message_handler = mock.AsyncMock(side_effect=side_effect)

    async with EnrichedClient(connection_class=connection_class) as client:
        subscription = await client.subscribe(
            destination, message_handler, on_suppressed_exception=noop_error_handler, ack=ack
        )
        await asyncio.sleep(0)
        await subscription.unsubscribe()
        await asyncio.sleep(0)

    message_handler.assert_called_once_with(message_frame)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"ack": ack, "destination": destination, "id": subscription_id}),
        message_frame,
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


@pytest.mark.parametrize("ok", [True, False])
@pytest.mark.parametrize("ack", ["client", "client-individual"])
async def test_client_listen_ack_nack_sent(monkeypatch: pytest.MonkeyPatch, ack: AckMode, ok: bool) -> None:  # noqa: FBT001
    subscription_id, destination, message_id = FAKER.pystr(), FAKER.pystr(), FAKER.pystr()
    monkeypatch.setattr(stompman.client, "_make_subscription_id", mock.Mock(return_value=subscription_id))

    message_frame = build_dataclass(
        MessageFrame, headers={"destination": destination, "message-id": message_id, "subscription": subscription_id}
    )
    connection_class, collected_frames = create_spying_connection(*get_read_frames_with_lifespan([message_frame]))
    message_handler = mock.AsyncMock(side_effect=None if ok else SomeError)

    async with EnrichedClient(connection_class=connection_class) as client:
        subscription = await client.subscribe(
            destination, message_handler, on_suppressed_exception=noop_error_handler, ack=ack
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await subscription.unsubscribe()

    message_handler.assert_called_once_with(message_frame)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"id": subscription_id, "destination": destination, "ack": ack}),
        message_frame,
        AckFrame(headers={"id": message_id, "subscription": subscription_id})
        if ok
        else NackFrame(headers={"id": message_id, "subscription": subscription_id}),
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


@pytest.mark.parametrize("ok", [True, False])
async def test_client_listen_auto_ack_nack(monkeypatch: pytest.MonkeyPatch, ok: bool) -> None:  # noqa: FBT001
    subscription_id, destination, message_id = FAKER.pystr(), FAKER.pystr(), FAKER.pystr()
    monkeypatch.setattr(stompman.client, "_make_subscription_id", mock.Mock(return_value=subscription_id))

    message_frame = build_dataclass(
        MessageFrame, headers={"destination": destination, "message-id": message_id, "subscription": subscription_id}
    )
    connection_class, collected_frames = create_spying_connection(*get_read_frames_with_lifespan([message_frame]))
    message_handler = mock.AsyncMock(side_effect=None if ok else SomeError)

    async with EnrichedClient(connection_class=connection_class) as client:
        subscription = await client.subscribe(
            destination, message_handler, on_suppressed_exception=noop_error_handler, ack="auto"
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await subscription.unsubscribe()

    message_handler.assert_called_once_with(message_frame)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"ack": "auto", "destination": destination, "id": subscription_id}),
        message_frame,
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


async def test_send_message_and_enter_transaction_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    body = FAKER.binary()
    destination = FAKER.pystr()
    expires = FAKER.pystr()
    content_type = FAKER.pystr()
    monkeypatch.setattr(
        stompman.client, "_make_transaction_id", mock.Mock(return_value=(transaction_id := FAKER.pystr()))
    )
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))

    async with EnrichedClient(connection_class=connection_class) as client, client.begin() as transaction:
        await transaction.send(
            body=body, destination=destination, content_type=content_type, headers={"expires": expires}
        )
        await client.send(body=body, destination=destination, content_type=content_type, headers={"expires": expires})

    assert collected_frames == enrich_expected_frames(
        BeginFrame(headers={"transaction": transaction_id}),
        SendFrame(
            headers={  # type: ignore[typeddict-unknown-key]
                "content-length": str(len(body)),
                "content-type": content_type,
                "destination": destination,
                "transaction": transaction_id,
                "expires": expires,
            },
            body=body,
        ),
        SendFrame(
            headers={  # type: ignore[typeddict-unknown-key]
                "content-length": str(len(body)),
                "content-type": content_type,
                "destination": destination,
                "expires": expires,
            },
            body=body,
        ),
        CommitFrame(headers={"transaction": transaction_id}),
    )


async def test_send_message_and_enter_transaction_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        stompman.client, "_make_transaction_id", mock.Mock(return_value=(transaction_id := FAKER.pystr()))
    )
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))

    async with EnrichedClient(connection_class=connection_class) as client:
        with suppress(AssertionError):
            async with client.begin():
                raise AssertionError

    assert collected_frames == enrich_expected_frames(
        BeginFrame(headers={"transaction": transaction_id}), AbortFrame(headers={"transaction": transaction_id})
    )


@pytest.mark.parametrize(
    "func",
    [
        stompman.client._make_receipt_id,
        stompman.client._make_subscription_id,
        stompman.client._make_transaction_id,
    ],
)
def test_generate_ids(func: Callable[[], str]) -> None:
    func()
