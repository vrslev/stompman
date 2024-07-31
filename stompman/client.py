import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import ClassVar, Literal, Self
from uuid import uuid4

from stompman.connection import AbstractConnection, Connection
from stompman.connection_manager import ConnectionManager, ConnectionParameters, Heartbeat
from stompman.errors import (
    ConnectionConfirmationTimeoutError,
    ConnectionLostError,
    UnsupportedProtocolVersionError,
)
from stompman.frames import (
    AbortFrame,
    AckFrame,
    BeginFrame,
    CommitFrame,
    ConnectedFrame,
    ConnectFrame,
    DisconnectFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    NackFrame,
    ReceiptFrame,
    SendFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)


@dataclass(kw_only=True, slots=True)
class Transaction:
    id: str
    _connection: ConnectionManager

    async def send(
        self,
        body: bytes,
        destination: str,
        content_type: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        await self._connection.write_frame(
            SendFrame.build(
                body=body, destination=destination, transaction=self.id, content_type=content_type, headers=headers
            )
        )


AckMode = Literal["client", "client-individual", "auto"]


@dataclass(kw_only=True, slots=True)
class Subscription:
    id: str
    destination: str
    handler: Callable[[MessageFrame], Coroutine[None, None, None]]
    ack: AckMode
    on_suppressed_exception: Callable[[Exception, MessageFrame], None]
    supressed_exception_classes: tuple[type[Exception], ...]

    _connection: ConnectionManager
    _active_subscriptions: dict[str, "Subscription"]
    _should_handle_ack_nack: bool = field(init=False)

    def __post_init__(self) -> None:
        self._should_handle_ack_nack = self.ack in {"client", "client-individual"}

    async def unsubscribe(self) -> None:
        del self._active_subscriptions[self.id]
        if self._connection.active:
            await self._connection.write_frame(UnsubscribeFrame(headers={"id": self.id}))

    async def _run_handler(self, frame: MessageFrame) -> None:
        called_nack = False
        try:
            await self.handler(frame)
        except self.supressed_exception_classes as exception:
            if self._should_handle_ack_nack and self._connection.active and self.id in self._active_subscriptions:
                with suppress(ConnectionLostError):
                    await self._connection.write_frame(
                        NackFrame(
                            headers={"id": frame.headers["message-id"], "subscription": frame.headers["subscription"]}
                        )
                    )
            called_nack = True
            self.on_suppressed_exception(exception, frame)
        finally:
            if (
                not called_nack
                and self._should_handle_ack_nack
                and self._connection.active
                and self.id in self._active_subscriptions
            ):
                with suppress(ConnectionLostError):
                    await self._connection.write_frame(
                        AckFrame(
                            headers={"id": frame.headers["message-id"], "subscription": frame.headers["subscription"]},
                        )
                    )


@dataclass(kw_only=True, slots=True)
class Client:
    PROTOCOL_VERSION: ClassVar = "1.2"  # https://stomp.github.io/stomp-specification-1.2.html

    servers: list[ConnectionParameters] = field(kw_only=False)
    on_error_frame: Callable[[ErrorFrame], None] | None = None
    on_unhandled_message_frame: Callable[[MessageFrame], None] | None = None
    on_heartbeat: Callable[[], None] | None = None

    heartbeat: Heartbeat = field(default=Heartbeat(1000, 1000))
    connect_retry_attempts: int = 3
    connect_retry_interval: int = 1
    connect_timeout: int = 2
    connection_confirmation_timeout: int = 2

    read_timeout: int = 2
    read_max_chunk_size: int = 1024 * 1024

    connection_class: type[AbstractConnection] = Connection
    _connection: ConnectionManager = field(init=False)
    _active_subscriptions: dict[str, "Subscription"] = field(default_factory=dict, init=False)
    _exit_stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False)
    _heartbeat_task: asyncio.Task[None] | None = field(default=None, init=False)
    _listen_task: asyncio.Task[None] = field(init=False)

    def __post_init__(self) -> None:
        self._connection = ConnectionManager(
            servers=self.servers,
            lifespan=self._lifespan,
            connection_class=self.connection_class,
            connect_retry_attempts=self.connect_retry_attempts,
            connect_retry_interval=self.connect_retry_interval,
            connect_timeout=self.connect_timeout,
            read_timeout=self.read_timeout,
            read_max_chunk_size=self.read_max_chunk_size,
        )

    async def __aenter__(self) -> Self:
        await self._exit_stack.enter_async_context(self._connection)
        self._listen_task = asyncio.create_task(self._listen_to_frames())
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        try:
            if self._active_subscriptions and not exc_value:
                await asyncio.Future()
        finally:
            self._listen_task.cancel()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
            await self._exit_stack.aclose()

    def _restart_heartbeat_task(self, heartbeat_interval: float) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._send_heartbeats_forever(heartbeat_interval))

    async def _wait_for_connected_frame(self) -> ConnectedFrame:
        collected_frames = []

        async def inner() -> ConnectedFrame:
            async for frame in self._connection.read_frames():
                if isinstance(frame, ConnectedFrame):
                    return frame
                collected_frames.append(frame)
            msg = "unreachable"  # pragma: no cover
            raise AssertionError(msg)  # pragma: no cover

        try:
            return await asyncio.wait_for(inner(), timeout=self.connection_confirmation_timeout)
        except TimeoutError as exception:
            raise ConnectionConfirmationTimeoutError(
                timeout=self.connection_confirmation_timeout, frames=collected_frames
            ) from exception

    @asynccontextmanager
    async def _lifespan(
        self, connection: AbstractConnection, connection_parameters: ConnectionParameters
    ) -> AsyncGenerator[None, None]:
        await connection.write_frame(
            ConnectFrame(
                headers={
                    "accept-version": self.PROTOCOL_VERSION,
                    "heart-beat": self.heartbeat.to_header(),
                    "host": connection_parameters.host,
                    "login": connection_parameters.login,
                    "passcode": connection_parameters.unescaped_passcode,
                },
            )
        )
        connected_frame = await self._wait_for_connected_frame()

        if connected_frame.headers["version"] != self.PROTOCOL_VERSION:
            raise UnsupportedProtocolVersionError(
                given_version=connected_frame.headers["version"], supported_version=self.PROTOCOL_VERSION
            )

        server_heartbeat = Heartbeat.from_header(connected_frame.headers["heart-beat"])
        heartbeat_interval = (
            max(self.heartbeat.will_send_interval_ms, server_heartbeat.want_to_receive_interval_ms) / 1000
        )
        self._restart_heartbeat_task(heartbeat_interval)
        yield

        for subscription in self._active_subscriptions.copy().values():
            await subscription.unsubscribe()
        await connection.write_frame(DisconnectFrame(headers={"receipt": _make_receipt_id()}))
        async for frame in connection.read_frames():
            if isinstance(frame, ReceiptFrame):
                break

    async def _send_heartbeats_forever(self, interval: float) -> None:
        while True:
            await self._connection.write_heartbeat()
            await asyncio.sleep(interval)

    async def _listen_to_frames(self) -> None:
        async with asyncio.TaskGroup() as task_group:
            async for frame in self._connection.read_frames():
                match frame:
                    case MessageFrame():
                        if subscription := self._active_subscriptions.get(frame.headers["subscription"]):
                            task_group.create_task(subscription._run_handler(frame))  # noqa: SLF001
                        elif self.on_unhandled_message_frame:
                            self.on_unhandled_message_frame(frame)
                    case ErrorFrame():
                        if self.on_error_frame:
                            self.on_error_frame(frame)
                    case HeartbeatFrame():
                        if self.on_heartbeat:
                            self.on_heartbeat()
                    case ConnectedFrame() | ReceiptFrame():
                        pass

    @asynccontextmanager
    async def begin(self) -> AsyncGenerator[Transaction, None]:
        transaction_id = _make_transaction_id()
        await self._connection.write_frame(BeginFrame(headers={"transaction": transaction_id}))

        try:
            yield Transaction(id=transaction_id, _connection=self._connection)
        except Exception:
            if self._connection.active:
                await self._connection.write_frame(AbortFrame(headers={"transaction": transaction_id}))
            raise
        else:
            if self._connection.active:
                await self._connection.write_frame(CommitFrame(headers={"transaction": transaction_id}))

    async def send(
        self, body: bytes, destination: str, content_type: str | None = None, headers: dict[str, str] | None = None
    ) -> None:
        await self._connection.write_frame(
            SendFrame.build(
                body=body, destination=destination, transaction=None, content_type=content_type, headers=headers
            )
        )

    async def subscribe(  # noqa: PLR0913
        self,
        destination: str,
        handler: Callable[[MessageFrame], Coroutine[None, None, None]],
        *,
        ack: AckMode = "client-individual",
        on_suppressed_exception: Callable[[Exception, MessageFrame], None],
        supressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
    ) -> "Subscription":
        subscription_id = _make_subscription_id()
        await self._connection.write_frame(
            SubscribeFrame(headers={"id": subscription_id, "destination": destination, "ack": ack})
        )
        subscription = Subscription(
            id=subscription_id,
            destination=destination,
            handler=handler,
            ack=ack,
            on_suppressed_exception=on_suppressed_exception,
            supressed_exception_classes=supressed_exception_classes,
            _connection=self._connection,
            _active_subscriptions=self._active_subscriptions,
        )
        self._active_subscriptions[subscription_id] = subscription
        return subscription


def _make_receipt_id() -> str:
    return str(uuid4())


def _make_transaction_id() -> str:
    return str(uuid4())


def _make_subscription_id() -> str:
    return str(uuid4())
