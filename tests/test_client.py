import asyncio
from collections.abc import AsyncGenerator, Awaitable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any
from unittest import mock

import pytest

import stompman.client
from stompman import (
    AbortFrame,
    AbstractConnection,
    AckFrame,
    BeginFrame,
    Client,
    ClientFrame,
    CommitFrame,
    ConnectedFrame,
    ConnectFrame,
    ConnectionConfirmationTimeoutError,
    ConnectionParameters,
    DisconnectFrame,
    ErrorEvent,
    ErrorFrame,
    FailedAllConnectAttemptsError,
    HeartbeatEvent,
    HeartbeatFrame,
    MessageEvent,
    MessageFrame,
    NackFrame,
    ReceiptFrame,
    SendFrame,
    ServerFrame,
    SubscribeFrame,
    UnsubscribeFrame,
    UnsupportedProtocolVersionError,
)
from stompman.protocol import PROTOCOL_VERSION


@dataclass
class BaseMockConnection(AbstractConnection):
    connection_parameters: ConnectionParameters
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int

    async def connect(self) -> bool:  # noqa: PLR6301
        return True

    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: ClientFrame) -> None: ...
    async def read_frames(self) -> AsyncGenerator[ServerFrame, None]:  # pragma: no cover  # noqa: PLR6301
        await asyncio.Future()
        yield  # type: ignore[misc]


def create_spying_connection(
    read_frames_yields: list[list[ServerFrame]],
) -> tuple[type[AbstractConnection], list[ClientFrame | ServerFrame | HeartbeatFrame]]:
    @dataclass
    class BaseCollectingConnection(BaseMockConnection):
        async def write_frame(self, frame: ClientFrame) -> None:  # noqa: PLR6301
            collected_frames.append(frame)

        async def read_frames(self) -> AsyncGenerator[ServerFrame, None]:  # noqa: PLR6301
            for frame in next(read_frames_iterator):
                collected_frames.append(frame)
                yield frame

    read_frames_iterator = iter(read_frames_yields)
    collected_frames: list[ClientFrame | ServerFrame | HeartbeatFrame] = []
    return BaseCollectingConnection, collected_frames


def get_read_frames_with_lifespan(read_frames: list[list[ServerFrame]]) -> list[list[ServerFrame]]:
    return [
        [ConnectedFrame(headers={"version": PROTOCOL_VERSION, "heart-beat": "1,1"})],
        *read_frames,
        [ReceiptFrame(headers={"receipt-id": "whatever"})],
    ]


def assert_frames_between_lifespan_match(
    collected_frames: list[ClientFrame | ServerFrame | HeartbeatFrame],
    expected_frames: list[ClientFrame | ServerFrame | HeartbeatFrame],
) -> None:
    assert collected_frames[2:-2] == expected_frames


@dataclass
class EnrichedClient(Client):
    servers: list[ConnectionParameters] = field(
        default_factory=lambda: [ConnectionParameters("localhost", 12345, "login", "passcode")]
    )


@pytest.fixture()
def mock_sleep(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: PT004
    monkeypatch.setattr("asyncio.sleep", mock.AsyncMock())


@pytest.mark.parametrize("ok_on_attempt", [1, 2, 3])
async def test_client_connect_to_one_server_ok(ok_on_attempt: int, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    class MockConnection(BaseMockConnection):
        async def connect(self) -> bool:
            assert self.connection_parameters == client.servers[0]
            nonlocal attempts
            attempts += 1
            return attempts == ok_on_attempt

    sleep_mock = mock.AsyncMock()
    monkeypatch.setattr("asyncio.sleep", sleep_mock)
    client = EnrichedClient(connection_class=MockConnection)
    assert await client._connect_to_one_server(client.servers[0])
    assert attempts == ok_on_attempt == (len(sleep_mock.mock_calls) + 1)


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connect_to_one_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        async def connect(self) -> bool:  # noqa: PLR6301
            return False

    client = EnrichedClient(connection_class=MockConnection)
    assert await client._connect_to_one_server(client.servers[0]) is None


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connect_to_any_server_ok() -> None:
    class MockConnection(BaseMockConnection):
        async def connect(self) -> bool:
            return self.connection_parameters.port == successful_server.port

    successful_server = ConnectionParameters("localhost", 10, "login", "pass")
    client = EnrichedClient(
        servers=[
            ConnectionParameters("localhost", 0, "login", "pass"),
            ConnectionParameters("localhost", 1, "login", "pass"),
            successful_server,
            ConnectionParameters("localhost", 3, "login", "pass"),
        ],
        connection_class=MockConnection,
    )
    connection = await client._connect_to_any_server()
    assert connection.connection_parameters == successful_server


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connect_to_any_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        async def connect(self) -> bool:  # noqa: PLR6301
            return False

    client = EnrichedClient(
        servers=[
            ConnectionParameters("", 0, "", ""),
            ConnectionParameters("", 1, "", ""),
            ConnectionParameters("", 2, "", ""),
            ConnectionParameters("", 3, "", ""),
        ],
        connection_class=MockConnection,
    )

    with pytest.raises(FailedAllConnectAttemptsError):
        await client._connect_to_any_server()


async def test_client_lifespan_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    connected_frame = ConnectedFrame(headers={"version": PROTOCOL_VERSION, "heart-beat": "1,1"})
    receipt_frame = ReceiptFrame(headers={"receipt-id": "whatever"})
    connection_class, collected_frames = create_spying_connection([[connected_frame], [receipt_frame]])
    write_heartbeat_mock = mock.Mock()

    class MockConnection(connection_class):  # type: ignore[valid-type, misc]
        write_heartbeat = write_heartbeat_mock

    receipt_id = "myid"
    monkeypatch.setattr(stompman.client, "uuid4", lambda: receipt_id)

    login = "login"
    passcode = "passcode"
    async with EnrichedClient(
        [ConnectionParameters("localhost", 10, login, passcode)], connection_class=MockConnection
    ) as client:
        await asyncio.sleep(0)

    assert collected_frames == [
        ConnectFrame(
            headers={
                "host": client._connection.connection_parameters.host,
                "accept-version": PROTOCOL_VERSION,
                "heart-beat": client.heartbeat.to_header(),
                "login": login,
                "passcode": passcode,
            }
        ),
        connected_frame,
        DisconnectFrame(headers={"receipt": receipt_id}),
        receipt_frame,
    ]
    write_heartbeat_mock.assert_called_once_with()


async def test_client_lifespan_connection_not_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def timeout(future: Awaitable[Any], timeout: float) -> Any:  # noqa: ANN401
        assert timeout == client.connection_confirmation_timeout
        return await original_wait_for(future, 0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", timeout)

    client = EnrichedClient(connection_class=BaseMockConnection)
    with pytest.raises(ConnectionConfirmationTimeoutError) as exc_info:
        await client.__aenter__()  # noqa: PLC2801

    assert exc_info.value == ConnectionConfirmationTimeoutError(client.connection_confirmation_timeout)


async def test_client_lifespan_unsupported_protocol_version() -> None:
    given_version = "whatever"
    connection_class, _ = create_spying_connection(
        [[ConnectedFrame(headers={"version": given_version, "heart-beat": "1,1"})]]
    )

    client = EnrichedClient(connection_class=connection_class)
    with pytest.raises(UnsupportedProtocolVersionError) as exc_info:
        await client.__aenter__()  # noqa: PLC2801

    assert exc_info.value == UnsupportedProtocolVersionError(
        given_version=given_version, supported_version=PROTOCOL_VERSION
    )


async def test_client_subscribe(monkeypatch: pytest.MonkeyPatch) -> None:
    destination = "/topic/test"
    subscription_id = "myid"
    monkeypatch.setattr(stompman.client, "uuid4", lambda: subscription_id)

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))
    async with EnrichedClient(connection_class=connection_class) as client, client.subscribe(destination):
        pass

    assert_frames_between_lifespan_match(
        collected_frames,
        [
            SubscribeFrame(
                headers={
                    "destination": destination,
                    "id": subscription_id,
                    "ack": "client-individual",
                }
            ),
            UnsubscribeFrame(headers={"id": subscription_id}),
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


async def test_client_listen_to_events_ok() -> None:
    message_frame = MessageFrame(headers={"destination": "", "message-id": "", "subscription": ""}, body=b"hello")
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
    async with EnrichedClient(connection_class=connection_class) as client:
        events = [event async for event in client.listen_to_events()]

    assert events == [
        MessageEvent(_client=client, _frame=message_frame),
        ErrorEvent(_client=client, _frame=error_frame),
        HeartbeatEvent(_client=client, _frame=heartbeat_frame),
    ]
    assert events[0].body == message_frame.body  # type: ignore[union-attr]
    assert events[1].message_header == error_frame.headers["message"]  # type: ignore[union-attr]
    assert events[1].body == error_frame.body  # type: ignore[union-attr]


@pytest.mark.parametrize("frame", [ConnectedFrame(headers={"version": ""}), ReceiptFrame(headers={"receipt-id": ""})])
async def test_client_listen_to_events_unreachable(frame: ConnectedFrame | ReceiptFrame) -> None:
    connection_class, _ = create_spying_connection(get_read_frames_with_lifespan([[frame]]))

    async with EnrichedClient(connection_class=connection_class) as client:
        with pytest.raises(AssertionError, match="unreachable"):
            [event async for event in client.listen_to_events()]


async def test_ack_nack() -> None:
    subscription = "subscription-id"
    message_id = "message-id"

    message_frame = MessageFrame(
        headers={"subscription": subscription, "message-id": message_id, "destination": "whatever"}, body=b"hello"
    )
    nack_frame = NackFrame(headers={"id": message_id, "subscription": subscription})
    ack_frame = AckFrame(headers={"id": message_id, "subscription": subscription})

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([[message_frame]]))
    async with EnrichedClient(connection_class=connection_class) as client:
        events = [event async for event in client.listen_to_events()]

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, MessageEvent)
        await event.nack()
        await event.ack()

    assert_frames_between_lifespan_match(collected_frames, [message_frame, nack_frame, ack_frame])


async def test_send_message_and_enter_transaction_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    body = b"hello"
    destination = "/queue/test"
    expires = "whatever"
    transaction = "myid"
    content_type = "my-content-type"
    monkeypatch.setattr(stompman.client, "uuid4", lambda: transaction)

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))
    async with (
        EnrichedClient(connection_class=connection_class) as client,
        client.enter_transaction() as transaction,
    ):
        await client.send(
            body=body,
            destination=destination,
            transaction=transaction,
            content_type=content_type,
            headers={"expires": expires},
        )

    assert_frames_between_lifespan_match(
        collected_frames,
        [
            BeginFrame(headers={"transaction": transaction}),
            SendFrame(
                headers={  # type: ignore[typeddict-unknown-key]
                    "content-length": str(len(body)),
                    "content-type": content_type,
                    "destination": destination,
                    "transaction": transaction,
                    "expires": expires,
                },
                body=b"hello",
            ),
            CommitFrame(headers={"transaction": transaction}),
        ],
    )


async def test_send_message_and_enter_transaction_abort(monkeypatch: pytest.MonkeyPatch) -> None:
    transaction = "myid"
    monkeypatch.setattr(stompman.client, "uuid4", lambda: transaction)

    connection_class, collected_frames = create_spying_connection(get_read_frames_with_lifespan([]))
    async with EnrichedClient(connection_class=connection_class) as client:
        with suppress(AssertionError):
            async with client.enter_transaction() as transaction:
                raise AssertionError

    assert_frames_between_lifespan_match(
        collected_frames,
        [BeginFrame(headers={"transaction": transaction}), AbortFrame(headers={"transaction": transaction})],
    )
