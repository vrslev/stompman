import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Self, TypeVar
from unittest import mock

import pytest
from polyfactory.factories.dataclass_factory import DataclassFactory

import stompman


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
    ],
)
def anyio_backend(request: pytest.FixtureRequest) -> object:
    return request.param


@pytest.fixture()
def mock_sleep(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: PT004
    monkeypatch.setattr("asyncio.sleep", mock.AsyncMock())


async def noop_message_handler(frame: stompman.MessageFrame) -> None: ...


def noop_error_handler(exception: Exception, frame: stompman.MessageFrame) -> None: ...


class BaseMockConnection(stompman.AbstractConnection):
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


@dataclass(frozen=True, kw_only=True, slots=True)
class NoopLifespan(stompman.AbstractConnectionLifespan):
    connection: stompman.AbstractConnection
    connection_parameters: stompman.ConnectionParameters

    async def enter(self) -> stompman.StompProtocolConnectionIssue | None: ...
    async def exit(self) -> None: ...


@dataclass(kw_only=True, slots=True)
class EnrichedConnectionManager(stompman.ConnectionManager):
    servers: list[stompman.ConnectionParameters] = field(
        default_factory=lambda: [stompman.ConnectionParameters("localhost", 12345, "login", "passcode")]
    )
    lifespan_factory: stompman.ConnectionLifespanFactory = field(default=NoopLifespan)
    connect_retry_attempts: int = 3
    connect_retry_interval: int = 1
    connect_timeout: int = 3
    read_timeout: int = 4
    read_max_chunk_size: int = 5
    write_retry_attempts: int = 3


DataclassType = TypeVar("DataclassType")


def build_dataclass(dataclass: type[DataclassType], **kwargs: Any) -> DataclassType:  # noqa: ANN401
    return DataclassFactory.create_factory(dataclass).build(**kwargs)


@dataclass
class SomeError(Exception):
    @classmethod
    async def raise_after_tick(cls) -> None:
        await asyncio.sleep(0)
        raise cls
