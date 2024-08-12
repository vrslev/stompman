import asyncio
from collections.abc import AsyncGenerator, AsyncIterable, Coroutine, Iterable
from functools import partial
from typing import Any, TypeVar
from unittest import mock

import faker
import pytest
from faker import Faker

import stompman.connection_lifespan
from stompman import (
    AnyServerFrame,
    Client,
    ConnectedFrame,
    ConnectFrame,
    ConnectionConfirmationTimeout,
    ConnectionParameters,
    DisconnectFrame,
    ErrorFrame,
    FailedAllConnectAttemptsError,
    ReceiptFrame,
    UnsupportedProtocolVersion,
)
from stompman.config import Heartbeat
from stompman.connection_lifespan import (
    calculate_heartbeat_interval,
    check_stomp_protocol_version,
    take_connected_frame,
)
from stompman.frames import HeartbeatFrame, MessageFrame
from tests.conftest import (
    BaseMockConnection,
    EnrichedClient,
    build_dataclass,
    create_spying_connection,
    get_read_frames_with_lifespan,
)

pytestmark = pytest.mark.anyio
FAKER = faker.Faker()

IterableItemT = TypeVar("IterableItemT")


async def make_async_iter(iterable: Iterable[IterableItemT]) -> AsyncIterable[IterableItemT]:
    for item in iterable:
        yield item
    await asyncio.sleep(0)


class TestTakeConnectedFrame:
    @pytest.mark.parametrize(
        "frame_types",
        [[ConnectedFrame], [MessageFrame, HeartbeatFrame, ConnectedFrame], [HeartbeatFrame, ConnectedFrame]],
    )
    async def test_ok(self, monkeypatch: pytest.MonkeyPatch, faker: Faker, frame_types: list[type[Any]]) -> None:
        wait_for_mock = mock.AsyncMock(side_effect=partial(asyncio.wait_for, timeout=0))
        monkeypatch.setattr("asyncio.wait_for", wait_for_mock)
        timeout = faker.pyint()

        result = await take_connected_frame(
            frames_iter=make_async_iter(build_dataclass(frame_type) for frame_type in frame_types),
            connection_confirmation_timeout=timeout,
        )

        assert isinstance(result, ConnectedFrame)
        wait_for_mock.assert_called_once()
        assert wait_for_mock.mock_calls[0].kwargs["timeout"] == timeout

    async def test_unreachable(self) -> None:
        with pytest.raises(AssertionError, match="unreachable"):
            await take_connected_frame(frames_iter=make_async_iter([]), connection_confirmation_timeout=1)

    async def test_timeout(self, monkeypatch: pytest.MonkeyPatch, faker: Faker) -> None:
        connection_confirmation_timeout = faker.pyint()
        original_wait_for = asyncio.wait_for

        async def mock_wait_for(future: Coroutine[Any, Any, Any], timeout: float) -> object:
            assert timeout == connection_confirmation_timeout

            task = asyncio.create_task(future)
            await asyncio.sleep(0)
            return await original_wait_for(task, 0)

        monkeypatch.setattr("asyncio.wait_for", mock_wait_for)

        result = await take_connected_frame(
            frames_iter=make_async_iter([HeartbeatFrame()]),
            connection_confirmation_timeout=connection_confirmation_timeout,
        )

        assert result == ConnectionConfirmationTimeout(
            timeout=connection_confirmation_timeout, frames=[HeartbeatFrame()]
        )


class TestCheckStompProtocolVersion:
    def test_ok(self, faker: Faker) -> None:
        supported_version = faker.pystr()
        connected_frame = build_dataclass(ConnectedFrame, headers={"version": supported_version})

        result = check_stomp_protocol_version(connected_frame=connected_frame, supported_version=supported_version)
        assert result is None

    def test_unsupported(self, faker: Faker) -> None:
        supported_version = faker.pystr()
        given_version = faker.pystr()
        connected_frame = build_dataclass(ConnectedFrame, headers={"version": given_version})

        result = check_stomp_protocol_version(connected_frame=connected_frame, supported_version=supported_version)
        assert result == UnsupportedProtocolVersion(given_version=given_version, supported_version=supported_version)


@pytest.mark.parametrize(
    ("client_declares", "server_asks", "expected_result"), [(900, 1000, 1), (900, 800, 0.9), (900, 900, 0.9)]
)
def test_calculate_heartbeat_interval(
    faker: Faker, client_declares: int, server_asks: int, expected_result: float
) -> None:
    server_heartbeat = Heartbeat(will_send_interval_ms=faker.pyint(), want_to_receive_interval_ms=server_asks)
    connected_frame = build_dataclass(ConnectedFrame, headers={"heart-beat": server_heartbeat.to_header()})
    client_heartbeat = Heartbeat(will_send_interval_ms=client_declares, want_to_receive_interval_ms=faker.pyint())

    result = calculate_heartbeat_interval(connected_frame=connected_frame, client_heartbeat=client_heartbeat)
    assert result == expected_result


async def test_client_connection_lifespan_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    connected_frame = build_dataclass(ConnectedFrame, headers={"version": Client.PROTOCOL_VERSION, "heart-beat": "1,1"})
    connection_class, collected_frames = create_spying_connection(
        [connected_frame], [], [(receipt_frame := build_dataclass(ReceiptFrame))]
    )

    disconnect_frame = DisconnectFrame(headers={"receipt": (receipt_id := FAKER.pystr())})
    monkeypatch.setattr(stompman.connection_lifespan, "_make_receipt_id", mock.Mock(return_value=receipt_id))

    async with EnrichedClient(
        [ConnectionParameters("localhost", 10, "login", "%3Dpasscode")], connection_class=connection_class
    ) as client:
        await asyncio.sleep(0)

    connect_frame = ConnectFrame(
        headers={
            "host": "localhost",
            "accept-version": Client.PROTOCOL_VERSION,
            "heart-beat": client.heartbeat.to_header(),
            "login": "login",
            "passcode": "=passcode",
        }
    )
    assert collected_frames == [connect_frame, connected_frame, disconnect_frame, receipt_frame]


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connection_lifespan_connection_not_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_wait_for(future: Coroutine[Any, Any, Any], timeout: float) -> object:
        assert timeout == connection_confirmation_timeout
        task = asyncio.create_task(future)
        await asyncio.sleep(0)
        return await original_wait_for(task, 0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", mock_wait_for)
    error_frame = build_dataclass(ErrorFrame)
    connection_confirmation_timeout = FAKER.pyint()

    class MockConnection(BaseMockConnection):
        @staticmethod
        async def read_frames() -> AsyncGenerator[AnyServerFrame, None]:
            yield error_frame
            await asyncio.sleep(0)

    with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
        await EnrichedClient(
            connection_class=MockConnection, connection_confirmation_timeout=connection_confirmation_timeout
        ).__aenter__()

    assert exc_info.value == FailedAllConnectAttemptsError(
        retry_attempts=3,
        issues=[ConnectionConfirmationTimeout(timeout=connection_confirmation_timeout, frames=[error_frame])] * 3,
    )


@pytest.mark.usefixtures("mock_sleep")
async def test_client_connection_lifespan_unsupported_protocol_version() -> None:
    given_version = FAKER.pystr()

    with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
        await EnrichedClient(
            connection_class=create_spying_connection(
                [build_dataclass(ConnectedFrame, headers={"version": given_version})]
            )[0],
            connect_retry_attempts=1,
        ).__aenter__()

    assert exc_info.value == FailedAllConnectAttemptsError(
        retry_attempts=1,
        issues=[UnsupportedProtocolVersion(given_version=given_version, supported_version=Client.PROTOCOL_VERSION)],
    )


async def test_client_connection_lifespan_disconnect_not_confirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    wait_for_calls = []

    async def mock_wait_for(future: Coroutine[Any, Any, Any], timeout: float) -> object:
        wait_for_calls.append(timeout)
        task = asyncio.create_task(future)
        await asyncio.sleep(0)
        return await original_wait_for(task, 0)

    original_wait_for = asyncio.wait_for
    monkeypatch.setattr("asyncio.wait_for", mock_wait_for)
    disconnect_confirmation_timeout = FAKER.pyint()
    read_frames_yields = get_read_frames_with_lifespan([])
    read_frames_yields[-1].clear()
    connection_class, _ = create_spying_connection(*read_frames_yields)

    async with EnrichedClient(
        connection_class=connection_class, disconnect_confirmation_timeout=disconnect_confirmation_timeout
    ):
        pass

    assert wait_for_calls[-1] == disconnect_confirmation_timeout


async def test_client_heartbeats_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    async def mock_sleep(delay: float) -> None:
        await real_sleep(0)
        sleep_calls.append(delay)

    sleep_calls: list[float] = []
    real_sleep = asyncio.sleep
    monkeypatch.setattr("asyncio.sleep", mock_sleep)

    connection_class, _ = create_spying_connection(*get_read_frames_with_lifespan([]))
    connection_class.write_heartbeat = (write_heartbeat_mock := mock.Mock())  # type: ignore[method-assign]

    async with EnrichedClient(connection_class=connection_class):
        await real_sleep(0)
        await real_sleep(0)
        await real_sleep(0)

    assert sleep_calls == [0, 1, 1]
    assert write_heartbeat_mock.mock_calls == [mock.call(), mock.call(), mock.call()]


def test_make_receipt_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.undo()
    stompman.connection_lifespan._make_receipt_id()
