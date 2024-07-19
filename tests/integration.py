import asyncio
import os
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from itertools import starmap
from typing import Final
from uuid import uuid4

import pytest
from hypothesis import given, strategies

import stompman
from stompman import AnyClientFrame, AnyServerFrame, ConnectionLostError, HeartbeatFrame
from stompman.serde import (
    COMMANDS_TO_FRAMES,
    NEWLINE,
    NULL,
    FrameParser,
    dump_frame,
    dump_header,
    make_frame_from_parts,
    parse_header,
)

pytestmark = pytest.mark.anyio

CONNECTION_PARAMETERS: Final = stompman.ConnectionParameters(
    host=os.environ["ARTEMIS_HOST"], port=61616, login="admin", passcode=":=123"
)


@asynccontextmanager
async def create_client() -> AsyncGenerator[stompman.Client, None]:
    async with stompman.Client(
        servers=[CONNECTION_PARAMETERS], read_timeout=10, connection_confirmation_timeout=10
    ) as client:
        yield client


@pytest.fixture()
async def client() -> AsyncGenerator[stompman.Client, None]:
    async with create_client() as client:
        yield client


@pytest.fixture()
def destination() -> str:
    return "DLQ"


async def test_ok(destination: str) -> None:
    async def produce() -> None:
        async with producer.enter_transaction() as transaction:
            for message in messages:
                await producer.send(
                    body=message, destination=destination, transaction=transaction, headers={"hello": "world"}
                )

    async def consume() -> None:
        received_messages = []

        async with asyncio.timeout(5), consumer.subscribe(destination=destination):
            async for event in consumer.listen():
                match event:
                    case stompman.MessageEvent(body=body):
                        await event.ack()
                        received_messages.append(body)
                        if len(received_messages) == len(messages):
                            break

        assert sorted(received_messages) == sorted(messages)

    messages = [str(uuid4()).encode() for _ in range(10000)]

    async with create_client() as consumer, create_client() as producer, asyncio.TaskGroup() as task_group:
        task_group.create_task(consume())
        task_group.create_task(produce())


async def test_not_raises_connection_lost_error_in_aexit(client: stompman.Client) -> None:
    await client._connection.close()


async def test_not_raises_connection_lost_error_in_write_frame(client: stompman.Client) -> None:
    await client._connection.close()

    with pytest.raises(ConnectionLostError):
        await client._connection.write_frame(stompman.ConnectFrame(headers={"accept-version": "", "host": ""}))


@pytest.mark.parametrize("anyio_backend", [("asyncio", {"use_uvloop": True})])
async def test_not_raises_connection_lost_error_in_write_heartbeat(client: stompman.Client) -> None:
    await client._connection.close()

    with pytest.raises(ConnectionLostError):
        client._connection.write_heartbeat()


async def test_not_raises_connection_lost_error_in_subscription(client: stompman.Client, destination: str) -> None:
    async with client.subscribe(destination):
        await client._connection.close()


async def test_not_raises_connection_lost_error_in_transaction_without_send(client: stompman.Client) -> None:
    async with client.enter_transaction():
        await client._connection.close()


async def test_not_raises_connection_lost_error_in_transaction_with_send(
    client: stompman.Client, destination: str
) -> None:
    async with client.enter_transaction() as transaction:
        await client.send(b"first", destination=destination, transaction=transaction)
        await client._connection.close()

        with pytest.raises(ConnectionLostError):
            await client.send(b"second", destination=destination, transaction=transaction)


async def test_raises_connection_lost_error_in_send(client: stompman.Client, destination: str) -> None:
    await client._connection.close()

    with pytest.raises(ConnectionLostError):
        await client.send(b"first", destination=destination)


async def test_raises_connection_lost_error_in_listen(client: stompman.Client) -> None:
    await client._connection.close()
    client.read_timeout = 0
    with pytest.raises(ConnectionLostError):
        [event async for event in client.listen()]


def generate_frames(
    cases: list[tuple[bytes, list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]],
) -> tuple[list[bytes], list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]:
    all_bytes, all_frames = [], []

    for noise, frames in cases:
        current_all_bytes = []
        if noise:
            current_all_bytes.append(noise + NEWLINE)

        for frame in frames:
            current_all_bytes.append(NEWLINE if isinstance(frame, HeartbeatFrame) else dump_frame(frame))
            all_frames.append(frame)

        all_bytes.append(b"".join(current_all_bytes))

    return all_bytes, all_frames


def bytes_not_contains(*avoided: bytes) -> Callable[[bytes], bool]:
    return lambda checked: all(item not in checked for item in avoided)


noise_bytes_strategy = strategies.binary().filter(bytes_not_contains(NEWLINE, NULL))
header_value_strategy = strategies.text().filter(lambda text: "\x00" not in text)
headers_strategy = strategies.dictionaries(header_value_strategy, header_value_strategy).map(
    lambda headers: dict(
        parsed_header
        for header in starmap(dump_header, headers.items())
        if (parsed_header := parse_header(bytearray(header)))
    )
)

FRAMES_WITH_ESCAPED_HEADERS = tuple(command for command in COMMANDS_TO_FRAMES if command != b"CONNECT")
frame_strategy = strategies.just(HeartbeatFrame()) | strategies.builds(
    make_frame_from_parts,
    command=strategies.sampled_from(FRAMES_WITH_ESCAPED_HEADERS),
    headers=headers_strategy,
    body=strategies.binary().filter(bytes_not_contains(NULL)),
)


@given(
    strategies.builds(
        generate_frames,
        strategies.lists(strategies.tuples(noise_bytes_strategy, strategies.lists(frame_strategy))),
    ),
)
def test_parsing(case: tuple[list[bytes], list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]) -> None:
    stream_chunks, expected_frames = case
    parser = FrameParser()
    assert [frame for chunk in stream_chunks for frame in parser.parse_frames_from_chunk(chunk)] == expected_frames
