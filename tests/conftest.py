import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Self, TypeVar

import pytest
from polyfactory.factories.dataclass_factory import DataclassFactory

import stompman
from stompman.connection import AbstractConnection


@pytest.fixture
def anyio_backend() -> object:
    return "asyncio"


async def noop_message_handler(frame: stompman.MessageFrame) -> None: ...


def noop_error_handler(exception: Exception, frame: stompman.MessageFrame) -> None: ...


class BaseMockConnection(AbstractConnection):
    @classmethod
    async def connect(
        cls, *, host: str, port: int, timeout: int, read_max_chunk_size: int, read_timeout: int
    ) -> Self | None:
        return cls()

    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: stompman.AnyClientFrame) -> None: ...
    @staticmethod
    async def read_frames() -> AsyncGenerator[stompman.AnyServerFrame, None]:  # pragma: no cover
        await asyncio.Future()
        yield stompman.HeartbeatFrame()


@dataclass(kw_only=True, slots=True)
class EnrichedClient(stompman.Client):
    servers: list[stompman.ConnectionParameters] = field(
        default_factory=lambda: [stompman.ConnectionParameters("localhost", 12345, "login", "passcode")], kw_only=False
    )


DataclassType = TypeVar("DataclassType")


def build_dataclass(dataclass: type[DataclassType], **kwargs: Any) -> DataclassType:  # noqa: ANN401
    return DataclassFactory.create_factory(dataclass).build(**kwargs)


@dataclass
class SomeError(Exception):
    @classmethod
    async def raise_after_tick(cls) -> None:
        await asyncio.sleep(0)
        raise cls


def create_spying_connection(
    *read_frames_yields: list[stompman.AnyServerFrame],
) -> tuple[type[AbstractConnection], list[stompman.AnyClientFrame | stompman.AnyServerFrame]]:
    class BaseCollectingConnection(BaseMockConnection):
        @staticmethod
        async def write_frame(frame: stompman.AnyClientFrame) -> None:
            collected_frames.append(frame)

        @staticmethod
        async def read_frames() -> AsyncGenerator[stompman.AnyServerFrame, None]:
            for frame in next(read_frames_iterator):
                collected_frames.append(frame)
                yield frame
            await asyncio.Future()

    read_frames_iterator = iter(read_frames_yields)
    collected_frames: list[stompman.AnyClientFrame | stompman.AnyServerFrame] = []
    return BaseCollectingConnection, collected_frames


CONNECT_FRAME = stompman.ConnectFrame(
    headers={
        "accept-version": stompman.Client.PROTOCOL_VERSION,
        "heart-beat": "1000,1000",
        "host": "localhost",
        "login": "login",
        "passcode": "passcode",
    },
)
CONNECTED_FRAME = stompman.ConnectedFrame(headers={"version": stompman.Client.PROTOCOL_VERSION, "heart-beat": "1,1"})


def get_read_frames_with_lifespan(*read_frames: list[stompman.AnyServerFrame]) -> list[list[stompman.AnyServerFrame]]:
    return [
        [CONNECTED_FRAME],
        *read_frames,
        [stompman.ReceiptFrame(headers={"receipt-id": "receipt-id-1"})],
    ]


def enrich_expected_frames(
    *expected_frames: stompman.AnyClientFrame | stompman.AnyServerFrame,
) -> list[stompman.AnyClientFrame | stompman.AnyServerFrame]:
    return [
        CONNECT_FRAME,
        CONNECTED_FRAME,
        *expected_frames,
        stompman.DisconnectFrame(headers={"receipt": "receipt-id-1"}),
        stompman.ReceiptFrame(headers={"receipt-id": "receipt-id-1"}),
    ]
