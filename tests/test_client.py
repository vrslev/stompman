import asyncio
from collections.abc import AsyncGenerator, Coroutine
from contextlib import suppress
from typing import Any
from unittest import mock

import pytest

import stompman.protocol
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
    ConnectionLostError,
    ConnectionParameters,
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
from stompman.protocol import StompProtocol
from tests.conftest import BaseMockConnection, EnrichedClient, noop_error_handler, noop_message_handler

pytestmark = pytest.mark.anyio


def create_spying_connection(
    read_frames_yields: list[list[AnyServerFrame]],
) -> tuple[type[AbstractConnection], list[AnyClientFrame | AnyServerFrame]]:
    class BaseCollectingConnection(BaseMockConnection):
        @staticmethod
        async def write_frame(frame: AnyClientFrame) -> None:
            collected_frames.append(frame)

        @staticmethod
        async def read_frames(max_chunk_size: int, timeout: int) -> AsyncGenerator[AnyServerFrame, None]:
            for frame in next(read_frames_iterator):
                collected_frames.append(frame)
                yield frame

    read_frames_iterator = iter(read_frames_yields)
    collected_frames: list[AnyClientFrame | AnyServerFrame] = []
    return BaseCollectingConnection, collected_frames


def get_read_frames_with_lifespan(read_frames: list[list[AnyServerFrame]]) -> list[list[AnyServerFrame]]:
    return [
        [ConnectedFrame(headers={"version": StompProtocol.PROTOCOL_VERSION, "heart-beat": "1,1"})],
        *read_frames,
        [ReceiptFrame(headers={"receipt-id": "whatever"})],
    ]


def assert_frames_between_lifespan_match(
    collected_frames: list[AnyClientFrame | AnyServerFrame], expected_frames: list[AnyClientFrame | AnyServerFrame]
) -> None:
    assert collected_frames[2:-2] == expected_frames


@pytest.fixture()
def mock_sleep(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: PT004
    monkeypatch.setattr("asyncio.sleep", mock.AsyncMock())


async def test_client_lifespan_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    connected_frame = ConnectedFrame(headers={"version": StompProtocol.PROTOCOL_VERSION, "heart-beat": "1,1"})
    receipt_frame = ReceiptFrame(headers={"receipt-id": "whatever"})
    connection_class, collected_frames = create_spying_connection([[connected_frame], [receipt_frame]])
    write_heartbeat_mock = mock.Mock()

    class MockConnection(connection_class):  # type: ignore[valid-type, misc]
        write_heartbeat = write_heartbeat_mock

    receipt_id = "myid"
    monkeypatch.setattr(stompman.protocol, "uuid4", lambda: receipt_id)

    login = "login"
    async with EnrichedClient(
        [ConnectionParameters("localhost", 10, login, "%3Dpasscode")], connection_class=MockConnection
    ) as client:
        await asyncio.sleep(0)

    assert collected_frames == [
        ConnectFrame(
            headers={
                "host": client._protocol.connection_parameters.host,
                "accept-version": StompProtocol.PROTOCOL_VERSION,
                "heart-beat": client.heartbeat.to_header(),
                "login": login,
                "passcode": "=passcode",
            }
        ),
        connected_frame,
        DisconnectFrame(headers={"receipt": receipt_id}),
        receipt_frame,
    ]
    write_heartbeat_mock.assert_called_once_with()


async def test_client_lifespan_connection_not_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def timeout(future: Coroutine[Any, Any, Any], timeout: float) -> object:
        assert timeout == client.connection_confirmation_timeout
        task = asyncio.create_task(future)
        await asyncio.sleep(0)
        return await original_wait_for(task, 0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", timeout)

    class MockConnection(BaseMockConnection):
        @staticmethod
        async def read_frames(max_chunk_size: int, timeout: int) -> AsyncGenerator[AnyServerFrame, None]:
            yield ErrorFrame(headers={"message": "hi"})
            await asyncio.sleep(0)

    client = EnrichedClient(connection_class=MockConnection)
    with pytest.raises(ConnectionConfirmationTimeoutError) as exc_info:
        await client.__aenter__()  # noqa: PLC2801

    assert exc_info.value == ConnectionConfirmationTimeoutError(
        timeout=client.connection_confirmation_timeout, frames=[ErrorFrame(headers={"message": "hi"})]
    )


async def test_client_lifespan_unsupported_protocol_version() -> None:
    given_version = "whatever"
    connection_class, _ = create_spying_connection(
        [[ConnectedFrame(headers={"version": given_version, "heart-beat": "1,1"})]]
    )

    client = EnrichedClient(connection_class=connection_class)
    with pytest.raises(UnsupportedProtocolVersionError) as exc_info:
        await client.__aenter__()  # noqa: PLC2801

    assert exc_info.value == UnsupportedProtocolVersionError(
        given_version=given_version, supported_version=StompProtocol.PROTOCOL_VERSION
    )


async def test_client_subscribe(monkeypatch: pytest.MonkeyPatch) -> None:
    destination_1 = "/topic/one"
    destination_2 = "/topic/two"
    subscription_id_1 = "id1"
    subscription_id_2 = "id2"
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[subscription_id_1, subscription_id_2, ""]))

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))
    async with EnrichedClient(connection_class=connection_class) as client:
        subscription_1 = await client.subscribe(
            destination_1, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
        )
        await client.subscribe(destination_2, handler=noop_message_handler, on_suppressed_exception=noop_error_handler)
        await subscription_1.unsubscribe()

    assert_frames_between_lifespan_match(
        collected_frames,
        [
            SubscribeFrame(
                headers={
                    "destination": destination_1,
                    "id": subscription_id_1,
                    "ack": "client-individual",
                }
            ),
            SubscribeFrame(
                headers={
                    "destination": destination_2,
                    "id": subscription_id_2,
                    "ack": "client-individual",
                }
            ),
            UnsubscribeFrame(headers={"id": subscription_id_1}),
            UnsubscribeFrame(headers={"id": subscription_id_2}),
        ],
    )


async def test_client_start_sendind_heartbeats(monkeypatch: pytest.MonkeyPatch) -> None:
    real_sleep = asyncio.sleep
    sleep_calls = []

    async def mock_sleep(delay: float) -> None:
        await real_sleep(0)
        sleep_calls.append(delay)

    monkeypatch.setattr("asyncio.sleep", mock_sleep)

    write_heartbeat_mock = mock.Mock()
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([]))

    class MockConnection(connection_class):  # type: ignore[valid-type, misc]
        write_heartbeat = write_heartbeat_mock

    async with EnrichedClient(connection_class=MockConnection):
        await real_sleep(0)
        await real_sleep(0)
        await real_sleep(0)

    assert sleep_calls == [1, 1]
    assert write_heartbeat_mock.mock_calls == [mock.call(), mock.call(), mock.call()]


async def test_client_heartbeat_not_raises_connection_lost() -> None:
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([]))

    class MockConnection(connection_class):  # type: ignore[valid-type, misc]
        write_heartbeat = mock.Mock(side_effect=ConnectionLostError)

    async with EnrichedClient(connection_class=MockConnection):
        await asyncio.sleep(0)


async def test_client_listen_ok() -> None:
    destination = "mydestination"
    message_frame = MessageFrame(
        headers={"destination": destination, "message-id": "", "subscription": ""}, body=b"hello"
    )
    error_frame = ErrorFrame(headers={"message": "short description"})
    heartbeat_frame = HeartbeatFrame()

    connection_class, _ = create_spying_connection(
        get_read_frames_with_lifespan(
            [
                [
                    message_frame,
                    error_frame,
                    heartbeat_frame,  # type: ignore[list-item]
                ]
            ]
        )
    )
    on_error_frame = mock.Mock()
    on_heartbeat = mock.Mock()
    handle_message = mock.AsyncMock()
    async with EnrichedClient(
        connection_class=connection_class, on_error_frame=on_error_frame, on_heartbeat=on_heartbeat
    ) as client:
        subscription = await client.subscribe(
            destination, handler=handle_message, on_suppressed_exception=noop_error_handler
        )
        await asyncio.sleep(0)
        await subscription.unsubscribe()

    on_error_frame.assert_called_once_with(error_frame)
    on_heartbeat.assert_called_once_with()
    handle_message.assert_called_once_with(message_frame)


@pytest.mark.parametrize("frame", [ConnectedFrame(headers={"version": ""}), ReceiptFrame(headers={"receipt-id": ""})])
async def test_client_listen_to_events_unreachable(frame: ConnectedFrame | ReceiptFrame) -> None:
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([[frame]]))

    async with EnrichedClient(connection_class=connection_class) as client:
        with pytest.raises(AssertionError, match="unreachable"):
            [event async for event in client.listen()]


async def test_ack_nack_ok() -> None:
    subscription = "subscription-id"
    message_id = "message-id"

    message_frame = MessageFrame(
        headers={"subscription": subscription, "message-id": message_id, "destination": "whatever"}, body=b"hello"
    )
    nack_frame = NackFrame(headers={"id": message_id, "subscription": subscription})
    ack_frame = AckFrame(headers={"id": message_id, "subscription": subscription})

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[message_frame]]))
    async with EnrichedClient(connection_class=connection_class) as client:
        events = [event async for event in client.listen()]

        assert len(events) == 1
        event = events[0]
        # assert isinstance(event, MessageEvent)
        await event.nack()
        await event.ack()

    assert_frames_between_lifespan_match(collected_frames, [message_frame, nack_frame, ack_frame])


async def test_ack_nack_connection_lost_error() -> None:
    message_frame = MessageFrame(headers={"subscription": "", "message-id": "", "destination": ""}, body=b"")
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([[message_frame]]))

    class MockConnection(connection_class):  # type: ignore[valid-type, misc]
        @staticmethod
        async def write_frame(frame: AnyClientFrame) -> None:
            if isinstance(frame, AckFrame | NackFrame):
                raise ConnectionLostError

    async with EnrichedClient(connection_class=MockConnection) as client:
        events = [event async for event in client.listen()]
        event = events[0]
        # assert isinstance(event, MessageEvent)

        await event.nack()
        await event.ack()


def get_mocked_message_event() -> tuple["MessageEvent", mock.AsyncMock, mock.AsyncMock, mock.Mock]:
    ack_mock, nack_mock, on_suppressed_exception_mock = mock.AsyncMock(), mock.AsyncMock(), mock.Mock()

    class CustomMessageEvent(MessageEvent):
        ack = ack_mock
        nack = nack_mock

    return (
        CustomMessageEvent(
            _frame=MessageFrame(
                headers={"destination": "destination", "message-id": "message-id", "subscription": "subscription"},
                body=b"",
            ),
            _client=mock.Mock(),
        ),
        ack_mock,
        nack_mock,
        on_suppressed_exception_mock,
    )


async def test_message_event_with_auto_ack_nack() -> None:
    event, ack, nack, on_suppressed_exception = get_mocked_message_event()
    exception = RuntimeError()

    async def raises_runtime_error() -> None:  # noqa: RUF029
        raise exception

    await event.with_auto_ack(
        raises_runtime_error(),
        supressed_exception_classes=(Exception,),
        on_suppressed_exception=on_suppressed_exception,
    )

    ack.assert_not_called()
    nack.assert_called_once_with()
    on_suppressed_exception.assert_called_once_with(exception, event)


async def test_message_event_with_auto_ack_ack_raises() -> None:
    event, ack, nack, on_suppressed_exception = get_mocked_message_event()

    async def func() -> None:  # noqa: RUF029
        raise ImportError

    with suppress(ImportError):
        await event.with_auto_ack(
            func(), supressed_exception_classes=(ModuleNotFoundError,), on_suppressed_exception=on_suppressed_exception
        )

    ack.assert_called_once_with()
    nack.assert_not_called()
    on_suppressed_exception.assert_not_called()


async def test_message_event_with_auto_ack_ack_ok() -> None:
    event, ack, nack, on_suppressed_exception = get_mocked_message_event()
    await event.with_auto_ack(mock.AsyncMock()(), on_suppressed_exception=on_suppressed_exception)
    ack.assert_called_once_with()
    nack.assert_not_called()
    on_suppressed_exception.assert_not_called()


async def test_send_message_and_enter_transaction_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    body = b"hello"
    destination = "/queue/test"
    expires = "whatever"
    transaction_id = "myid"
    content_type = "my-content-type"
    monkeypatch.setattr(stompman.protocol, "uuid4", lambda: transaction_id)

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))
    async with (
        EnrichedClient(connection_class=connection_class) as client,
        client.begin() as transaction,
    ):
        await transaction.send(
            body=body, destination=destination, content_type=content_type, headers={"expires": expires}
        )
        await client.send(body=body, destination=destination, content_type=content_type, headers={"expires": expires})

    assert_frames_between_lifespan_match(
        collected_frames,
        [
            BeginFrame(headers={"transaction": transaction_id}),
            SendFrame(
                headers={  # type: ignore[typeddict-unknown-key]
                    "content-length": str(len(body)),
                    "content-type": content_type,
                    "destination": destination,
                    "transaction": transaction_id,
                    "expires": expires,
                },
                body=b"hello",
            ),
            SendFrame(
                headers={  # type: ignore[typeddict-unknown-key]
                    "content-length": str(len(body)),
                    "content-type": content_type,
                    "destination": destination,
                    "expires": expires,
                },
                body=b"hello",
            ),
            CommitFrame(headers={"transaction": transaction_id}),
        ],
    )


async def test_send_message_and_enter_transaction_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    transaction_id = "myid"
    monkeypatch.setattr(stompman.protocol, "uuid4", lambda: transaction_id)

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))
    async with EnrichedClient(connection_class=connection_class) as client:
        with suppress(AssertionError):
            async with client.begin():
                raise AssertionError

    assert_frames_between_lifespan_match(
        collected_frames,
        [BeginFrame(headers={"transaction": transaction_id}), AbortFrame(headers={"transaction": transaction_id})],
    )
