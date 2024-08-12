import asyncio
from collections.abc import AsyncIterable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol
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


class AbstractConnectionLifespan(Protocol):
    async def enter(self) -> StompProtocolConnectionIssue | None: ...
    async def exit(self) -> None: ...


async def take_connected_frame(
    *, frames_iter: AsyncIterable[AnyServerFrame], connection_confirmation_timeout: int
) -> ConnectedFrame | ConnectionConfirmationTimeout:
    collected_frames = []

    async def take_connected_frame_and_collect_other_frames() -> ConnectedFrame:
        async for frame in frames_iter:
            if isinstance(frame, ConnectedFrame):
                return frame
            collected_frames.append(frame)
        msg = "unreachable"
        raise AssertionError(msg)

    try:
        return await asyncio.wait_for(
            take_connected_frame_and_collect_other_frames(), timeout=connection_confirmation_timeout
        )
    except TimeoutError:
        return ConnectionConfirmationTimeout(timeout=connection_confirmation_timeout, frames=collected_frames)


async def wait_for_receipt_frame(
    *, frames_iter: AsyncIterable[AnyServerFrame], disconnect_confirmation_timeout: int
) -> None:
    async def inner() -> None:
        async for frame in frames_iter:
            if isinstance(frame, ReceiptFrame):
                break

    with suppress(TimeoutError):
        await asyncio.wait_for(inner(), timeout=disconnect_confirmation_timeout)


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
        connected_frame_or_error = await take_connected_frame(
            frames_iter=self.connection.read_frames(),
            connection_confirmation_timeout=self.connection_confirmation_timeout,
        )
        if isinstance(connected_frame_or_error, ConnectionConfirmationTimeout):
            return connected_frame_or_error
        connected_frame = connected_frame_or_error

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
        await self.connection.write_frame(DisconnectFrame(headers={"receipt": _make_receipt_id()}))
        await wait_for_receipt_frame(
            frames_iter=self.connection.read_frames(),
            disconnect_confirmation_timeout=self.disconnect_confirmation_timeout,
        )


def _make_receipt_id() -> str:
    return str(uuid4())


class ConnectionLifespanFactory(Protocol):
    def __call__(
        self, *, connection: AbstractConnection, connection_parameters: ConnectionParameters
    ) -> AbstractConnectionLifespan: ...
