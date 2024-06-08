import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

from stompman.client import Heartbeat
from stompman.connection import AbstractConnection
from stompman.errors import (
    ConnectionConfirmationTimeoutError,
    UnsupportedProtocolVersionError,
)
from stompman.frames import (
    ConnectedFrame,
    ConnectFrame,
    DisconnectFrame,
    ReceiptFrame,
)
from stompman.protocol import PROTOCOL_VERSION


async def _read_connected_frame(connection: AbstractConnection) -> ConnectedFrame:
    while True:
        async for frame in connection.read_frames():
            if isinstance(frame, ConnectedFrame):
                return frame


@asynccontextmanager
async def enter_connection_context(
    connection: AbstractConnection, client_heartbeat: Heartbeat, connection_confirmation_timeout: int
) -> AsyncGenerator[None, None]:
    await connection.write_frame(
        ConnectFrame(
            headers={
                "accept-version": PROTOCOL_VERSION,
                "heart-beat": client_heartbeat.dump(),
                "login": connection.connection_parameters.login,
                "passcode": connection.connection_parameters.passcode,
            },
        )
    )
    try:
        async with asyncio.timeout(connection_confirmation_timeout):
            connected_frame = await _read_connected_frame(connection)
    except TimeoutError as exception:
        raise ConnectionConfirmationTimeoutError(connection_confirmation_timeout) from exception

    if connected_frame.headers["version"] != PROTOCOL_VERSION:
        raise UnsupportedProtocolVersionError(
            given_version=connected_frame.headers["version"], supported_version=PROTOCOL_VERSION
        )

    server_heartbeat = Heartbeat.load(connected_frame.headers["heart-beat"])
    heartbeat_interval = (
        max(client_heartbeat.will_send_interval_ms, server_heartbeat.want_to_receive_interval_ms) / 1000
    )
    async with asyncio.TaskGroup() as task_group:
        task = task_group.create_task(connection.send_heartbeats_forever(heartbeat_interval))
        yield
        task.cancel()

    await connection.write_frame(DisconnectFrame(headers={"receipt": str(uuid4())}))
    async for frame in connection.read_frames():
        match frame:
            case ReceiptFrame():
                await connection.close()
