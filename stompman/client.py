import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import ClassVar, Self
from uuid import uuid4

from stompman.config import ConnectionParameters, Heartbeat
from stompman.connection import AbstractConnection, Connection
from stompman.connection_manager import ConnectionManager
from stompman.errors import ConnectionConfirmationTimeoutError, UnsupportedProtocolVersionError
from stompman.frames import (
    AbortFrame,
    AckFrame,
    AckMode,
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


@asynccontextmanager
async def connection_lifespan(
    *,
    connection: AbstractConnection,
    connection_parameters: ConnectionParameters,
    protocol_version: str,
    client_heartbeat: Heartbeat,
    connection_confirmation_timeout: int,
    disconnect_confirmation_timeout: int,
) -> AsyncIterator[float]:
    await connection.write_frame(
        ConnectFrame(
            headers={
                "accept-version": protocol_version,
                "heart-beat": client_heartbeat.to_header(),
                "host": connection_parameters.host,
                "login": connection_parameters.login,
                "passcode": connection_parameters.unescaped_passcode,
            },
        )
    )
    collected_frames = []

    async def take_connected_frame_and_collect_other_frames() -> ConnectedFrame:
        async for frame in connection.read_frames():
            if isinstance(frame, ConnectedFrame):
                return frame
            collected_frames.append(frame)
        msg = "unreachable"  # pragma: no cover
        raise AssertionError(msg)  # pragma: no cover

    try:
        connected_frame = await asyncio.wait_for(
            take_connected_frame_and_collect_other_frames(), timeout=connection_confirmation_timeout
        )
    except TimeoutError as exception:
        raise ConnectionConfirmationTimeoutError(
            timeout=connection_confirmation_timeout, frames=collected_frames
        ) from exception

    if connected_frame.headers["version"] != protocol_version:
        raise UnsupportedProtocolVersionError(
            given_version=connected_frame.headers["version"], supported_version=protocol_version
        )

    server_heartbeat = Heartbeat.from_header(connected_frame.headers["heart-beat"])
    heartbeat_interval = (
        max(client_heartbeat.will_send_interval_ms, server_heartbeat.want_to_receive_interval_ms) / 1000
    )
    yield heartbeat_interval

    await connection.write_frame(DisconnectFrame(headers={"receipt": _make_receipt_id()}))

    async def take_receipt_frame() -> None:
        async for frame in connection.read_frames():
            if isinstance(frame, ReceiptFrame):
                break

    with suppress(TimeoutError):
        await asyncio.wait_for(take_receipt_frame(), timeout=disconnect_confirmation_timeout)


def _make_receipt_id() -> str:
    return str(uuid4())


@dataclass(kw_only=True, slots=True)
class Subscription:
    id: str = field(default_factory=lambda: _make_subscription_id(), init=False)  # noqa: PLW0108
    destination: str
    handler: Callable[[MessageFrame], Coroutine[None, None, None]]
    ack: AckMode
    on_suppressed_exception: Callable[[Exception, MessageFrame], None]
    supressed_exception_classes: tuple[type[Exception], ...]
    _connection_manager: ConnectionManager
    _active_subscriptions: dict[str, "Subscription"]

    _should_handle_ack_nack: bool = field(init=False)

    def __post_init__(self) -> None:
        self._should_handle_ack_nack = self.ack in {"client", "client-individual"}

    async def _subscribe(self) -> None:
        await self._connection_manager.write_frame_reconnecting(
            SubscribeFrame(headers={"id": self.id, "destination": self.destination, "ack": self.ack})
        )
        self._active_subscriptions[self.id] = self

    async def unsubscribe(self) -> None:
        del self._active_subscriptions[self.id]
        await self._connection_manager.maybe_write_frame(UnsubscribeFrame(headers={"id": self.id}))

    async def _run_handler(self, *, frame: MessageFrame) -> None:
        try:
            await self.handler(frame)
        except self.supressed_exception_classes as exception:
            if self._should_handle_ack_nack and self.id in self._active_subscriptions:
                await self._connection_manager.maybe_write_frame(
                    NackFrame(
                        headers={"id": frame.headers["message-id"], "subscription": frame.headers["subscription"]}
                    )
                )
            self.on_suppressed_exception(exception, frame)
        else:
            if self._should_handle_ack_nack and self.id in self._active_subscriptions:
                await self._connection_manager.maybe_write_frame(
                    AckFrame(
                        headers={"id": frame.headers["message-id"], "subscription": frame.headers["subscription"]},
                    )
                )


def _make_subscription_id() -> str:
    return str(uuid4())


@asynccontextmanager
async def subscriptions_lifespan(
    *, connection: AbstractConnection, active_subscriptions: dict[str, Subscription]
) -> AsyncIterator[None]:
    for subscription in active_subscriptions.values():
        await connection.write_frame(
            SubscribeFrame(
                headers={"id": subscription.id, "destination": subscription.destination, "ack": subscription.ack}
            )
        )
    yield
    for subscription in active_subscriptions.copy().values():
        await subscription.unsubscribe()


@dataclass(kw_only=True, slots=True, unsafe_hash=True)
class Transaction:
    id: str = field(default_factory=lambda: _make_transaction_id(), init=False)  # noqa: PLW0108
    _connection_manager: ConnectionManager = field(hash=False)
    _active_transactions: set["Transaction"] = field(hash=False)
    sent_frames: list[SendFrame] = field(default_factory=list, init=False, hash=False)

    async def __aenter__(self) -> Self:
        await self._connection_manager.write_frame_reconnecting(BeginFrame(headers={"transaction": self.id}))
        self._active_transactions.add(self)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        if exc_value:
            await self._connection_manager.maybe_write_frame(AbortFrame(headers={"transaction": self.id}))
            self._active_transactions.remove(self)
        else:
            commited = await self._connection_manager.maybe_write_frame(CommitFrame(headers={"transaction": self.id}))
            if commited:
                self._active_transactions.remove(self)

    async def send(
        self, body: bytes, destination: str, *, content_type: str | None = None, headers: dict[str, str] | None = None
    ) -> None:
        frame = SendFrame.build(
            body=body, destination=destination, transaction=self.id, content_type=content_type, headers=headers
        )
        self.sent_frames.append(frame)
        await self._connection_manager.write_frame_reconnecting(frame)


def _make_transaction_id() -> str:
    return str(uuid4())


async def commit_pending_transactions(*, connection: AbstractConnection, active_transactions: set[Transaction]) -> None:
    for transaction in active_transactions:
        for frame in transaction.sent_frames:
            await connection.write_frame(frame)
        await connection.write_frame(CommitFrame(headers={"transaction": transaction.id}))
    active_transactions.clear()


@dataclass(kw_only=True, slots=True)
class Client:
    PROTOCOL_VERSION: ClassVar = "1.2"  # https://stomp.github.io/stomp-specification-1.2.html

    servers: list[ConnectionParameters] = field(kw_only=False)
    on_error_frame: Callable[[ErrorFrame], None] | None = None
    on_heartbeat: Callable[[], None] | None = None

    heartbeat: Heartbeat = field(default=Heartbeat(1000, 1000))
    connect_retry_attempts: int = 3
    connect_retry_interval: int = 1
    connect_timeout: int = 2
    read_timeout: int = 2
    read_max_chunk_size: int = 1024 * 1024
    connection_confirmation_timeout: int = 2
    disconnect_confirmation_timeout: int = 2

    connection_class: type[AbstractConnection] = Connection

    _connection_manager: ConnectionManager = field(init=False)
    _active_subscriptions: dict[str, "Subscription"] = field(default_factory=dict, init=False)
    _active_transactions: set[Transaction] = field(default_factory=set, init=False)
    _exit_stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False)
    _heartbeat_task: asyncio.Task[None] = field(init=False)
    _listen_task: asyncio.Task[None] = field(init=False)
    _task_group: asyncio.TaskGroup = field(init=False)

    def __post_init__(self) -> None:
        self._connection_manager = ConnectionManager(
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
        self._task_group = await self._exit_stack.enter_async_context(asyncio.TaskGroup())
        self._heartbeat_task = self._task_group.create_task(asyncio.sleep(0))
        await self._exit_stack.enter_async_context(self._connection_manager)
        self._listen_task = self._task_group.create_task(self._listen_to_frames())
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        try:
            if self._active_subscriptions and not exc_value:
                await asyncio.Future()
        finally:
            self._listen_task.cancel()
            self._heartbeat_task.cancel()
            await asyncio.wait([self._listen_task, self._heartbeat_task])
            await self._exit_stack.aclose()

    @asynccontextmanager
    async def _lifespan(
        self, connection: AbstractConnection, connection_parameters: ConnectionParameters
    ) -> AsyncGenerator[None, None]:
        async with connection_lifespan(
            connection=connection,
            connection_parameters=connection_parameters,
            protocol_version=self.PROTOCOL_VERSION,
            client_heartbeat=self.heartbeat,
            connection_confirmation_timeout=self.connection_confirmation_timeout,
            disconnect_confirmation_timeout=self.disconnect_confirmation_timeout,
        ) as heartbeat_interval:
            self._restart_heartbeat_task(heartbeat_interval)
            async with subscriptions_lifespan(connection=connection, active_subscriptions=self._active_subscriptions):
                await commit_pending_transactions(connection=connection, active_transactions=self._active_transactions)
                yield

    def _restart_heartbeat_task(self, interval: float) -> None:
        self._heartbeat_task.cancel()
        self._heartbeat_task = self._task_group.create_task(self._send_heartbeats_forever(interval))

    async def _send_heartbeats_forever(self, interval: float) -> None:
        while True:
            await self._connection_manager.write_heartbeat_reconnecting()
            await asyncio.sleep(interval)

    async def _listen_to_frames(self) -> None:
        async with asyncio.TaskGroup() as task_group:
            async for frame in self._connection_manager.read_frames_reconnecting():
                match frame:
                    case MessageFrame():
                        if subscription := self._active_subscriptions.get(frame.headers["subscription"]):
                            task_group.create_task(subscription._run_handler(frame=frame))  # noqa: SLF001
                    case ErrorFrame():
                        if self.on_error_frame:
                            self.on_error_frame(frame)
                    case HeartbeatFrame():
                        if self.on_heartbeat:
                            self.on_heartbeat()
                    case ConnectedFrame() | ReceiptFrame():
                        pass

    async def send(
        self, body: bytes, destination: str, *, content_type: str | None = None, headers: dict[str, str] | None = None
    ) -> None:
        await self._connection_manager.write_frame_reconnecting(
            SendFrame.build(
                body=body, destination=destination, transaction=None, content_type=content_type, headers=headers
            )
        )

    @asynccontextmanager
    async def begin(self) -> AsyncGenerator[Transaction, None]:
        async with Transaction(
            _connection_manager=self._connection_manager, _active_transactions=self._active_transactions
        ) as transaction:
            yield transaction

    async def subscribe(
        self,
        destination: str,
        handler: Callable[[MessageFrame], Coroutine[None, None, None]],
        *,
        ack: AckMode = "client-individual",
        on_suppressed_exception: Callable[[Exception, MessageFrame], None],
        supressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
    ) -> "Subscription":
        subscription = Subscription(
            destination=destination,
            handler=handler,
            ack=ack,
            on_suppressed_exception=on_suppressed_exception,
            supressed_exception_classes=supressed_exception_classes,
            _connection_manager=self._connection_manager,
            _active_subscriptions=self._active_subscriptions,
        )
        await subscription._subscribe()  # noqa: SLF001
        return subscription
