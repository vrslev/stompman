import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar
from uuid import uuid4

from stompman.config import ConnectionParameters, Heartbeat
from stompman.connection import AbstractConnection
from stompman.errors import ConnectionConfirmationTimeout, StompProtocolConnectionIssue, UnsupportedProtocolVersion
from stompman.frames import (
    AnyServerFrame,
    ConnectedFrame,
    ConnectFrame,
    DisconnectFrame,
    ReceiptFrame,
)
from stompman.subscription import (
    ActiveSubscriptions,
    resubscribe_to_active_subscriptions,
    unsubscribe_from_all_active_subscriptions,
)
from stompman.transaction import ActiveTransactions, commit_pending_transactions

FrameType = TypeVar("FrameType", bound=AnyServerFrame)
WaitForFutureReturnType = TypeVar("WaitForFutureReturnType")


async def wait_for_or_none(
    awaitable: Awaitable[WaitForFutureReturnType], timeout: float
) -> WaitForFutureReturnType | None:
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except TimeoutError:
        return None


WaitForOrNone = Callable[[Awaitable[WaitForFutureReturnType], float], Awaitable[WaitForFutureReturnType | None]]


async def take_frame_of_type(
    *,
    frame_type: type[FrameType],
    frames_iter: AsyncIterable[AnyServerFrame],
    timeout: int,
    wait_for_or_none: WaitForOrNone[FrameType],
) -> FrameType | list[Any]:
    collected_frames = []

    async def inner() -> FrameType:
        async for frame in frames_iter:
            if isinstance(frame, frame_type):
                return frame
            collected_frames.append(frame)
        msg = "unreachable"
        raise AssertionError(msg)

    return await wait_for_or_none(inner(), timeout) or collected_frames


def check_stomp_protocol_version(
    *, connected_frame: ConnectedFrame, supported_version: str
) -> UnsupportedProtocolVersion | None:
    if connected_frame.headers["version"] == supported_version:
        return None
    return UnsupportedProtocolVersion(
        given_version=connected_frame.headers["version"], supported_version=supported_version
    )


def calculate_heartbeat_interval(*, connected_frame: ConnectedFrame, client_heartbeat: Heartbeat) -> float:
    server_heartbeat = Heartbeat.from_header(connected_frame.headers["heart-beat"])
    return max(client_heartbeat.will_send_interval_ms, server_heartbeat.want_to_receive_interval_ms) / 1000


class AbstractConnectionLifespan(Protocol):
    async def enter(self) -> StompProtocolConnectionIssue | None: ...
    async def exit(self) -> None: ...


@dataclass(kw_only=True, slots=True)
class ConnectionLifespan(AbstractConnectionLifespan):
    connection: AbstractConnection
    connection_parameters: ConnectionParameters
    protocol_version: str
    client_heartbeat: Heartbeat
    connection_confirmation_timeout: int
    disconnect_confirmation_timeout: int
    active_subscriptions: ActiveSubscriptions
    active_transactions: ActiveTransactions
    set_heartbeat_interval: Callable[[float], None]
    _generate_receipt_id: Callable[[], str] = field(default=lambda: _make_receipt_id())  # noqa: PLW0108

    async def _establish_connection(self) -> StompProtocolConnectionIssue | None:
        await self.connection.write_frame(
            ConnectFrame(
                headers={
                    "accept-version": self.protocol_version,
                    "heart-beat": self.client_heartbeat.to_header(),
                    "host": self.connection_parameters.host,
                    "login": self.connection_parameters.login,
                    "passcode": self.connection_parameters.unescaped_passcode,
                },
            )
        )
        connected_frame_or_collected_frames = await take_frame_of_type(
            frame_type=ConnectedFrame,
            frames_iter=self.connection.read_frames(),
            timeout=self.connection_confirmation_timeout,
            wait_for_or_none=wait_for_or_none,
        )
        if not isinstance(connected_frame_or_collected_frames, ConnectedFrame):
            return ConnectionConfirmationTimeout(
                timeout=self.connection_confirmation_timeout, frames=connected_frame_or_collected_frames
            )
        connected_frame = connected_frame_or_collected_frames

        if unsupported_protocol_version_error := check_stomp_protocol_version(
            connected_frame=connected_frame, supported_version=self.protocol_version
        ):
            return unsupported_protocol_version_error

        self.set_heartbeat_interval(
            calculate_heartbeat_interval(connected_frame=connected_frame, client_heartbeat=self.client_heartbeat)
        )
        return None

    async def enter(self) -> StompProtocolConnectionIssue | None:
        if protocol_connection_issue := await self._establish_connection():
            return protocol_connection_issue

        await resubscribe_to_active_subscriptions(
            connection=self.connection, active_subscriptions=self.active_subscriptions
        )
        await commit_pending_transactions(connection=self.connection, active_transactions=self.active_transactions)
        return None

    async def exit(self) -> None:
        await unsubscribe_from_all_active_subscriptions(active_subscriptions=self.active_subscriptions)
        await self.connection.write_frame(DisconnectFrame(headers={"receipt": self._generate_receipt_id()}))
        await take_frame_of_type(
            frame_type=ReceiptFrame,
            frames_iter=self.connection.read_frames(),
            timeout=self.disconnect_confirmation_timeout,
            wait_for_or_none=wait_for_or_none,
        )


class ConnectionLifespanFactory(Protocol):
    def __call__(
        self, *, connection: AbstractConnection, connection_parameters: ConnectionParameters
    ) -> AbstractConnectionLifespan: ...


def _make_receipt_id() -> str:
    return str(uuid4())
