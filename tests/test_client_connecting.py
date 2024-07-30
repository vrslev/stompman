from typing import Any, Self
from unittest import mock

import pytest

import stompman
from tests.conftest import BaseMockConnection, EnrichedClient


def test_connection_parameters_from_pydantic_multihost_hosts() -> None:
    full_host: dict[str, Any] = {"username": "me", "password": "pass", "host": "localhost", "port": 1234}
    assert stompman.ConnectionParameters.from_pydantic_multihost_hosts(
        [{**full_host, "port": index} for index in range(5)]  # type: ignore[typeddict-item]
    ) == [
        stompman.ConnectionParameters(full_host["host"], index, full_host["username"], full_host["password"])
        for index in range(5)
    ]

    for key in ("username", "password", "host", "port"):
        with pytest.raises(ValueError, match=f"{key} must be set"):
            assert stompman.ConnectionParameters.from_pydantic_multihost_hosts([{**full_host, key: None}, full_host])  # type: ignore[typeddict-item, list-item]


@pytest.mark.parametrize("ok_on_attempt", [1, 2, 3])
async def test_client_connect_to_one_server_ok(ok_on_attempt: int, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = 0

    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(cls, host: str, port: int, timeout: int) -> Self | None:
            assert (host, port) == (client.servers[0].host, client.servers[0].port)
            nonlocal attempts
            attempts += 1

            return await super().connect(host, port, timeout) if attempts == ok_on_attempt else None

    sleep_mock = mock.AsyncMock()
    monkeypatch.setattr("asyncio.sleep", sleep_mock)
    client = EnrichedClient(connection_class=MockConnection)
    assert await client._connect_to_one_server(client.servers[0])
    assert attempts == ok_on_attempt == (len(sleep_mock.mock_calls) + 1)


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connect_to_one_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(cls, host: str, port: int, timeout: int) -> Self | None:
            return None

    client = EnrichedClient(connection_class=MockConnection)
    assert await client._connect_to_one_server(client.servers[0]) is None


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connect_to_any_server_ok() -> None:
    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(cls, host: str, port: int, timeout: int) -> Self | None:
            return await super().connect(host, port, timeout) if port == successful_server.port else None

    successful_server = stompman.ConnectionParameters("localhost", 10, "login", "pass")
    client = stompman.Client(
        servers=[
            stompman.ConnectionParameters("localhost", 0, "login", "pass"),
            stompman.ConnectionParameters("localhost", 1, "login", "pass"),
            successful_server,
            stompman.ConnectionParameters("localhost", 3, "login", "pass"),
        ],
        connection_class=MockConnection,
    )
    await client._connect_to_any_server()
    assert client._protocol.connection
    assert client._protocol.connection_parameters == successful_server


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connect_to_any_server_fails() -> None:
    class MockConnection(BaseMockConnection):
        @classmethod
        async def connect(cls, host: str, port: int, timeout: int) -> Self | None:
            return None

    client = EnrichedClient(
        servers=[
            stompman.ConnectionParameters("", 0, "", ""),
            stompman.ConnectionParameters("", 1, "", ""),
            stompman.ConnectionParameters("", 2, "", ""),
            stompman.ConnectionParameters("", 3, "", ""),
        ],
        connection_class=MockConnection,
    )

    with pytest.raises(stompman.FailedAllConnectAttemptsError):
        await client._connect_to_any_server()
