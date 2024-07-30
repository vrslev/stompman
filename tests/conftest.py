import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any, Self, TypeVar
from unittest import mock

import pytest
from polyfactory.factories.dataclass_factory import DataclassFactory

import stompman
from stompman import AbstractConnection, Client, ConnectionParameters
from stompman.frames import AnyClientFrame, AnyServerFrame


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
    async def connect(  # noqa: PLR0913
        cls, host: str, port: int, timeout: int, read_max_chunk_size: int, read_timeout: int
    ) -> Self | None:
        return cls()

    async def close(self) -> None: ...
    def write_heartbeat(self) -> None: ...
    async def write_frame(self, frame: AnyClientFrame) -> None: ...
    @staticmethod
    async def read_frames() -> AsyncGenerator[AnyServerFrame, None]:  # pragma: no cover
        await asyncio.Future()
        yield  # type: ignore[misc]


@dataclass(kw_only=True, slots=True)
class EnrichedClient(Client):
    servers: list[ConnectionParameters] = field(
        default_factory=lambda: [ConnectionParameters("localhost", 12345, "login", "passcode")], kw_only=False
    )


@pytest.fixture()
def mock_sleep(monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: PT004
    monkeypatch.setattr("asyncio.sleep", mock.AsyncMock())


DataclassType = TypeVar("DataclassType")


def build_dataclass(dataclass: type[DataclassType], **kwargs: Any) -> DataclassType:  # noqa: ANN401
    return DataclassFactory.create_factory(dataclass).build(**kwargs)
