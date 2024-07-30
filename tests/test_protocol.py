import asyncio
from collections.abc import AsyncGenerator, Coroutine
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Literal
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
    read_frames_yields: list[list[AnyServerFrame | HeartbeatFrame]],
) -> tuple[type[AbstractConnection], list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]:
    class BaseCollectingConnection(BaseMockConnection):
        @staticmethod
        async def write_frame(frame: AnyClientFrame) -> None:
            collected_frames.append(frame)

        @staticmethod
        async def read_frames(
            max_chunk_size: int, timeout: int
        ) -> AsyncGenerator[AnyServerFrame | HeartbeatFrame, None]:
            for frame in next(read_frames_iterator):
                collected_frames.append(frame)
                yield frame

    read_frames_iterator = iter(read_frames_yields)
    collected_frames: list[AnyClientFrame | AnyServerFrame | HeartbeatFrame] = []
    return BaseCollectingConnection, collected_frames


def get_read_frames_with_lifespan(
    read_frames: list[list[AnyServerFrame | HeartbeatFrame]],
) -> list[list[AnyServerFrame | HeartbeatFrame]]:
    return [
        [ConnectedFrame(headers={"version": StompProtocol.PROTOCOL_VERSION, "heart-beat": "1,1"})],
        *read_frames,
        [ReceiptFrame(headers={"receipt-id": "whatever"})],
    ]


def enrich_expected_frames(
    *expected_frames: AnyClientFrame | AnyServerFrame | HeartbeatFrame,
) -> list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]:
    return [
        ConnectFrame(
            headers={
                "accept-version": StompProtocol.PROTOCOL_VERSION,
                "heart-beat": "1000,1000",
                "host": "localhost",
                "login": "login",
                "passcode": "passcode",
            },
        ),
        ConnectedFrame(headers={"version": StompProtocol.PROTOCOL_VERSION, "heart-beat": "1,1"}),
        *expected_frames,
        DisconnectFrame(headers={"receipt": ""}),
        ReceiptFrame(headers={"receipt-id": "whatever"}),
    ]


async def test_client_lifespan_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    connection_class, collected_frames = create_spying_connection(
        [
            [
                (
                    connected_frame := ConnectedFrame(
                        headers={"version": StompProtocol.PROTOCOL_VERSION, "heart-beat": "1,1"}
                    )
                )
            ],
            [],
            [(receipt_frame := ReceiptFrame(headers={"receipt-id": "whatever"}))],
        ]
    )
    connection_class.write_heartbeat = (write_heartbeat_mock := mock.Mock())  # type: ignore[method-assign]
    monkeypatch.setattr(stompman.protocol, "_make_receipt_id", mock.Mock(return_value=(receipt_id := "myid")))

    async with EnrichedClient(
        [ConnectionParameters("localhost", 10, "login", "%3Dpasscode")], connection_class=connection_class
    ) as client:
        await asyncio.sleep(0)

    assert collected_frames == [
        ConnectFrame(
            headers={
                "host": "localhost",
                "accept-version": StompProtocol.PROTOCOL_VERSION,
                "heart-beat": client.heartbeat.to_header(),
                "login": "login",
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

    class MockConnection(BaseMockConnection):
        @staticmethod
        async def read_frames(max_chunk_size: int, timeout: int) -> AsyncGenerator[AnyServerFrame, None]:
            yield error_frame
            await asyncio.sleep(0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", timeout)
    error_frame = ErrorFrame(headers={"message": "hi"})
    client = EnrichedClient(connection_class=MockConnection)

    with pytest.raises(ConnectionConfirmationTimeoutError) as exc_info:
        await client.__aenter__()  # noqa: PLC2801

    assert exc_info.value == ConnectionConfirmationTimeoutError(
        timeout=client.connection_confirmation_timeout, frames=[error_frame]
    )


async def test_client_lifespan_unsupported_protocol_version() -> None:
    connection_class, _ = create_spying_connection(
        [[ConnectedFrame(headers={"version": (given_version := "whatever"), "heart-beat": "1,1"})]]
    )
    client = EnrichedClient(connection_class=connection_class)

    with pytest.raises(UnsupportedProtocolVersionError) as exc_info:
        await client.__aenter__()  # noqa: PLC2801

    assert exc_info.value == UnsupportedProtocolVersionError(
        given_version=given_version, supported_version=StompProtocol.PROTOCOL_VERSION
    )


async def test_client_subscribe_lifespan_no_active_subs_in_aexit(monkeypatch: pytest.MonkeyPatch) -> None:
    destination_1, subscription_id_1 = "/topic/one", "id1"
    destination_2, subscription_id_2 = "/topic/two", "id2"
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[subscription_id_1, subscription_id_2, ""]))
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[]]))

    async with EnrichedClient(connection_class=connection_class) as client:
        subscription_1 = await client.subscribe(
            destination_1, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
        )
        subscription_2 = await client.subscribe(
            destination_2, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
        )
        await asyncio.sleep(0)
        await subscription_1.unsubscribe()
        await subscription_2.unsubscribe()

    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"destination": destination_1, "id": subscription_id_1, "ack": "client-individual"}),
        SubscribeFrame(headers={"destination": destination_2, "id": subscription_id_2, "ack": "client-individual"}),
        UnsubscribeFrame(headers={"id": subscription_id_1}),
        UnsubscribeFrame(headers={"id": subscription_id_2}),
    )


async def test_client_start_sending_heartbeats(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_sleep(delay: float) -> None:
        await real_sleep(0)
        sleep_calls.append(delay)

    real_sleep = asyncio.sleep
    sleep_calls: list[float] = []
    monkeypatch.setattr("asyncio.sleep", mock_sleep)
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([[]]))
    connection_class.write_heartbeat = (write_heartbeat_mock := mock.Mock())  # type: ignore[method-assign]

    async with EnrichedClient(connection_class=connection_class):
        await real_sleep(0)
        await real_sleep(0)
        await real_sleep(0)

    assert sleep_calls == [1, 1]
    assert write_heartbeat_mock.mock_calls == [mock.call(), mock.call(), mock.call()]


async def test_client_heartbeat_not_raises_connection_lost() -> None:
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([[]]))
    connection_class.write_heartbeat = mock.Mock(side_effect=ConnectionLostError)  # type: ignore[method-assign]
    async with EnrichedClient(connection_class=connection_class):
        await asyncio.sleep(0)


@dataclass
class SomeError(Exception): ...


async def test_client_subscribe_lifespan_with_active_subs_in_aexit_indirect_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, subscription_id = "/topic/one", "id1"
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[subscription_id, ""]))
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[]]))

    async def raise_soon() -> None:
        await asyncio.sleep(0)
        raise SomeError

    with pytest.raises(ExceptionGroup) as exc_info:  # noqa: PT012
        async with asyncio.TaskGroup() as task_group, EnrichedClient(connection_class=connection_class) as client:
            await client.subscribe(
                destination, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
            )
            task_group.create_task(raise_soon())

    assert exc_info.value.exceptions == (SomeError(),)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"destination": destination, "id": subscription_id, "ack": "client-individual"}),
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


async def test_client_subscribe_lifespan_with_active_subs_in_aexit_direct_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, subscription_id = "/topic/one", "id1"
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[subscription_id, ""]))
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[]]))

    with pytest.raises(SomeError):  # noqa: PT012
        async with EnrichedClient(connection_class=connection_class) as client:
            await client.subscribe(
                destination, handler=noop_message_handler, on_suppressed_exception=noop_error_handler
            )
            await asyncio.sleep(0)
            raise SomeError

    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"destination": destination, "id": subscription_id, "ack": "client-individual"}),
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


async def test_client_listen_routing_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    message_frame_1 = MessageFrame(
        headers={"destination": "whatever-1", "message-id": "", "subscription": (subscription_id_1 := "sub-id-1")},
        body=b"hello",
    )
    message_frame_2 = MessageFrame(
        headers={"destination": "whatever-2", "message-id": "", "subscription": (subscription_id_2 := "sub-id-2")},
        body=b"hello again",
    )
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[subscription_id_1, subscription_id_2, ""]))
    error_frame = ErrorFrame(headers={"message": "short description here"})
    heartbeat_frame = HeartbeatFrame()

    connection_class, _ = create_spying_connection(
        get_read_frames_with_lifespan(
            [
                [
                    ConnectedFrame(headers={"version": ""}),
                    ReceiptFrame(headers={"receipt-id": ""}),
                    message_frame_1,
                    error_frame,
                    message_frame_2,
                    heartbeat_frame,
                ]
            ]
        )
    )

    on_error_frame = mock.Mock()
    on_heartbeat = mock.Mock()
    handle_message_1 = mock.AsyncMock(return_value=None)
    handle_message_2 = mock.AsyncMock(side_effect=SomeError)
    on_suppressed_exception_1 = mock.Mock()
    on_suppressed_exception_2 = mock.Mock()

    async with EnrichedClient(
        connection_class=connection_class, on_error_frame=on_error_frame, on_heartbeat=on_heartbeat
    ) as client:
        subscription_1 = await client.subscribe(
            "whatev", handler=handle_message_1, on_suppressed_exception=on_suppressed_exception_1
        )
        subscription_2 = await client.subscribe(
            "whatev", handler=handle_message_2, on_suppressed_exception=on_suppressed_exception_2
        )
        await asyncio.sleep(0)
        await subscription_1.unsubscribe()
        await subscription_2.unsubscribe()

    on_error_frame.assert_called_once_with(error_frame)
    on_heartbeat.assert_called_once_with()
    handle_message_1.assert_called_once_with(message_frame_1)
    handle_message_2.assert_called_once_with(message_frame_2)
    on_suppressed_exception_1.assert_not_called()
    on_suppressed_exception_2.assert_called_once_with(SomeError(), message_frame_2)


@pytest.mark.parametrize("side_effect", [None, SomeError])
@pytest.mark.parametrize("ack", ["client", "client-individual"])
async def test_client_listen_unsubscribe_before_ack_or_nack(
    monkeypatch: pytest.MonkeyPatch, ack: Literal["client", "client-individual"], side_effect: object
) -> None:
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[(subscription_id := "id1"), ""]))
    message_frame = MessageFrame(
        headers={"destination": "", "message-id": "", "subscription": subscription_id}, body=b""
    )
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[message_frame]]))
    handle_message = mock.AsyncMock(side_effect=side_effect)

    async with EnrichedClient(connection_class=connection_class) as client:
        subscription = await client.subscribe(
            "", handler=handle_message, on_suppressed_exception=noop_error_handler, ack=ack
        )
        await asyncio.sleep(0)
        await subscription.unsubscribe()

    handle_message.assert_called_once_with(message_frame)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"ack": ack, "destination": "", "id": subscription_id}),
        message_frame,
        UnsubscribeFrame(headers={"id": "id1"}),
    )


@pytest.mark.parametrize("ok", [True, False])
@pytest.mark.parametrize("ack", ["client", "client-individual"])
async def test_client_listen_ack_nack(
    monkeypatch: pytest.MonkeyPatch,
    ack: Literal["client", "client-individual"],
    ok: bool,  # noqa: FBT001
) -> None:
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[(subscription_id := "id1"), ""]))
    message_frame = MessageFrame(
        headers={"destination": "", "message-id": "", "subscription": subscription_id}, body=b""
    )

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[message_frame]]))
    handle_message = mock.AsyncMock(side_effect=None if ok else SomeError)
    async with EnrichedClient(connection_class=connection_class) as client:
        subscription = await client.subscribe(
            "", handler=handle_message, on_suppressed_exception=noop_error_handler, ack=ack
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await subscription.unsubscribe()

    handle_message.assert_called_once_with(message_frame)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"ack": ack, "destination": "", "id": subscription_id}),
        message_frame,
        AckFrame(headers={"id": "", "subscription": subscription_id})
        if ok
        else NackFrame(headers={"id": "", "subscription": subscription_id}),
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


@pytest.mark.parametrize("ok", [True, False])
async def test_client_listen_auto_ack_nack(
    monkeypatch: pytest.MonkeyPatch,
    ok: bool,  # noqa: FBT001
) -> None:
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[(subscription_id := "id1"), ""]))
    message_frame = MessageFrame(
        headers={"destination": "", "message-id": "", "subscription": subscription_id}, body=b""
    )

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[message_frame]]))
    handle_message = mock.AsyncMock(side_effect=None if ok else SomeError)
    async with EnrichedClient(connection_class=connection_class) as client:
        subscription = await client.subscribe(
            "", handler=handle_message, on_suppressed_exception=noop_error_handler, ack="auto"
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await subscription.unsubscribe()

    handle_message.assert_called_once_with(message_frame)
    assert collected_frames == enrich_expected_frames(
        SubscribeFrame(headers={"ack": "auto", "destination": "", "id": subscription_id}),
        message_frame,
        UnsubscribeFrame(headers={"id": subscription_id}),
    )


async def test_send_message_and_enter_transaction_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    body = b"hello"
    destination = "/queue/test"
    expires = "whatever"
    content_type = "my-content-type"
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[(transaction_id := "myid"), ""]))

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
    )


async def test_send_message_and_enter_transaction_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stompman.protocol, "uuid4", mock.Mock(side_effect=[(transaction_id := "myid"), ""]))
    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))

    async with EnrichedClient(connection_class=connection_class) as client:
        with suppress(AssertionError):
            async with client.begin():
                raise AssertionError

    assert collected_frames == enrich_expected_frames(
        BeginFrame(headers={"transaction": transaction_id}),
        AbortFrame(headers={"transaction": transaction_id}),
    )
