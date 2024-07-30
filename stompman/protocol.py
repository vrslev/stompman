import asyncio
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, ClassVar, Literal, NamedTuple, Self, TypedDict
from urllib.parse import unquote
from uuid import uuid4

from stompman.connection import AbstractConnection
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


class Heartbeat(NamedTuple):
    will_send_interval_ms: int
    want_to_receive_interval_ms: int

    def to_header(self) -> str:
        return f"{self.will_send_interval_ms},{self.want_to_receive_interval_ms}"

    @classmethod
    def from_header(cls, header: str) -> Self:
        first, second = header.split(",", maxsplit=1)
        return cls(int(first), int(second))


class MultiHostHostLike(TypedDict):
    username: str | None
    password: str | None
    host: str | None
    port: int | None


@dataclass(frozen=True, slots=True)
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)

    @property
    def unescaped_passcode(self) -> str:
        return unquote(self.passcode)

    @classmethod
    def from_pydantic_multihost_hosts(cls, hosts: list[MultiHostHostLike]) -> list[Self]:
        """Create connection parameters from a list of `MultiHostUrl` objects.

        .. code-block:: python
        import stompman.

        ArtemisDsn = typing.Annotated[
            pydantic_core.MultiHostUrl,
            pydantic.UrlConstraints(
                host_required=True,
                allowed_schemes=["tcp"],
            ),
        ]

        async with stompman.Client(
            servers=stompman.ConnectionParameters.from_pydantic_multihost_hosts(
                ArtemisDsn("tcp://lev:pass@host1:61616,lev:pass@host1:61617,lev:pass@host2:61616").hosts()
            ),
        ):
            ...
        """
        servers: list[Self] = []
        for host in hosts:
            if host["host"] is None:
                msg = "host must be set"
                raise ValueError(msg)
            if host["port"] is None:
                msg = "port must be set"
                raise ValueError(msg)
            if host["username"] is None:
                msg = "username must be set"
                raise ValueError(msg)
            if host["password"] is None:
                msg = "password must be set"
                raise ValueError(msg)

            servers.append(cls(host=host["host"], port=host["port"], login=host["username"], passcode=host["password"]))
        return servers


@dataclass(kw_only=True, slots=True)
class StompProtocol:
    connection: AbstractConnection
    connection_parameters: ConnectionParameters
    heartbeat: Heartbeat
    connection_confirmation_timeout: int
    on_error_frame: Callable[[ErrorFrame], None] | None
    on_heartbeat: Callable[[], None] | None
    read_timeout: int
    read_max_chunk_size: int

    PROTOCOL_VERSION: ClassVar = "1.2"  # https://stomp.github.io/stomp-specification-1.2.html

    _active_subscriptions: dict[str, "Subscription"] = field(default_factory=dict, init=False)
    _exit_stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False)

    async def __aenter__(self) -> Self:
        await self._exit_stack.enter_async_context(self._lifespan())
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        try:
            if self._active_subscriptions:
                await asyncio.Future()
        finally:
            await self._exit_stack.aclose()
            await self.connection.close()

    async def _wait_for_connected_frame(self) -> ConnectedFrame:
        collected_frames = []

        async def inner() -> ConnectedFrame:
            async for frame in self.connection.read_frames(
                max_chunk_size=self.read_max_chunk_size, timeout=self.read_timeout
            ):
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
    async def _lifespan(self) -> AsyncGenerator[None, None]:
        await self.connection.write_frame(
            ConnectFrame(
                headers={
                    "accept-version": self.PROTOCOL_VERSION,
                    "heart-beat": self.heartbeat.to_header(),
                    "host": self.connection_parameters.host,
                    "login": self.connection_parameters.login,
                    "passcode": self.connection_parameters.unescaped_passcode,
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

        async with asyncio.TaskGroup() as task_group:
            heartbeat_task = task_group.create_task(self._send_heartbeats_forever(heartbeat_interval))
            listen_task = task_group.create_task(self._listen_to_frames())
            try:
                yield
            finally:
                heartbeat_task.cancel()
                listen_task.cancel()

        if self.connection.active:
            for subscription in self._active_subscriptions.copy().values():
                await subscription.unsubscribe()
        if self.connection.active:
            await self.connection.write_frame(DisconnectFrame(headers={"receipt": str(uuid4())}))
        if self.connection.active:
            async for frame in self.connection.read_frames(
                max_chunk_size=self.read_max_chunk_size, timeout=self.read_timeout
            ):
                if isinstance(frame, ReceiptFrame):
                    break

    async def _send_heartbeats_forever(self, interval: float) -> None:
        while self.connection.active:
            try:
                self.connection.write_heartbeat()
            except ConnectionLostError:
                # Avoid raising the error in an exception group.
                # ConnectionLostError should be raised in a way that user expects it.
                return
            await asyncio.sleep(interval)

    async def _listen_to_frames(self) -> None:
        async with asyncio.TaskGroup() as task_group:
            async for frame in self.connection.read_frames(
                max_chunk_size=self.read_max_chunk_size, timeout=self.read_timeout
            ):
                match frame:
                    case MessageFrame():
                        if subscription := self._active_subscriptions.get(frame.headers["subscription"]):
                            task_group.create_task(subscription._run_handler(frame))  # noqa: SLF001
                    case ErrorFrame():
                        if self.on_error_frame:
                            self.on_error_frame(frame)
                    case HeartbeatFrame():
                        if self.on_heartbeat:
                            self.on_heartbeat()
                    case ConnectedFrame() | ReceiptFrame():
                        pass

    @asynccontextmanager
    async def begin(self) -> AsyncGenerator["Transaction", None]:
        transaction_id = str(uuid4())
        await self.connection.write_frame(BeginFrame(headers={"transaction": transaction_id}))

        try:
            yield Transaction(id=transaction_id, _connection=self.connection)
        except Exception:
            if self.connection.active:
                await self.connection.write_frame(AbortFrame(headers={"transaction": transaction_id}))
            raise
        else:
            if self.connection.active:
                await self.connection.write_frame(CommitFrame(headers={"transaction": transaction_id}))

    async def send(
        self, body: bytes, destination: str, content_type: str | None = None, headers: dict[str, str] | None = None
    ) -> None:
        await self.connection.write_frame(
            SendFrame.build(
                body=body, destination=destination, transaction=None, content_type=content_type, headers=headers
            )
        )

    async def subscribe(  # noqa: PLR0913
        self,
        destination: str,
        handler: Callable[[MessageFrame], Coroutine[None, None, None]],
        on_suppressed_exception: Callable[[Exception, MessageFrame], Any],
        supressed_exception_classes: tuple[type[Exception], ...],
        ack: Literal["client", "client-individual", "auto"],
    ) -> "Subscription":
        subscription_id = str(uuid4())
        await self.connection.write_frame(
            SubscribeFrame(headers={"id": subscription_id, "destination": destination, "ack": ack})
        )
        subscription = Subscription(
            id=subscription_id,
            destination=destination,
            handler=handler,
            on_suppressed_exception=on_suppressed_exception,
            supressed_exception_classes=supressed_exception_classes,
            ack=ack,
            _connection=self.connection,
            _active_subscriptions=self._active_subscriptions,
        )
        self._active_subscriptions[subscription_id] = subscription
        return subscription


@dataclass(kw_only=True, slots=True)
class Transaction:
    id: str
    _connection: AbstractConnection

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


@dataclass(kw_only=True, slots=True)
class Subscription:
    id: str
    destination: str
    handler: Callable[[MessageFrame], Coroutine[None, None, None]]
    ack: Literal["client", "client-individual", "auto"]
    on_suppressed_exception: Callable[[Exception, MessageFrame], Any]
    supressed_exception_classes: tuple[type[Exception], ...]
    _connection: AbstractConnection
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
            if self._should_handle_ack_nack and self._connection.active:
                with suppress(ConnectionLostError):
                    await self._connection.write_frame(
                        NackFrame(
                            headers={"id": frame.headers["message-id"], "subscription": frame.headers["subscription"]}
                        )
                    )
            called_nack = True
            self.on_suppressed_exception(exception, frame)
        finally:
            if not called_nack and self._should_handle_ack_nack and self._connection.active:
                with suppress(ConnectionLostError):
                    await self._connection.write_frame(
                        AckFrame(
                            headers={"id": frame.headers["message-id"], "subscription": frame.headers["subscription"]},
                        )
                    )
