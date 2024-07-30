import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Self

import pytest

import stompman
from stompman import AbstractConnection, Client, ConnectionParameters
from stompman.frames import AnyClientFrame, AnyServerFrame, HeartbeatFrame


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
    ],
)
def anyio_backend(request: pytest.FixtureRequest) -> object:
    return request.param


async def noop_message_handler(frame: stompman.MessageFrame) -> None: ...


def noop_error_handler(exception: Exception, frame: stompman.MessageFrame) -> None: ...


class BaseMockConnection(AbstractConnection):
    @classmethod
    async def connect(cls, host: str, port: int, timeout: int) -> Self | None:
        return cls()

    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: AnyClientFrame) -> None: ...
    @staticmethod
    async def read_frames(
        max_chunk_size: int, timeout: int
    ) -> AsyncGenerator[AnyServerFrame | HeartbeatFrame, None]:  # pragma: no cover
        await asyncio.Future()
        yield  # type: ignore[misc]


@dataclass(kw_only=True, slots=True)
class EnrichedClient(Client):
    servers: list[ConnectionParameters] = field(
        default_factory=lambda: [ConnectionParameters("localhost", 12345, "login", "passcode")], kw_only=False
    )
