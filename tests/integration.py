import asyncio
import os
from collections.abc import Callable
from itertools import starmap
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
    parse_headers,
)

pytestmark = pytest.mark.anyio


@pytest.fixture()
def server() -> stompman.ConnectionParameters:
    return stompman.ConnectionParameters(host=os.environ["ARTEMIS_HOST"], port=61616, login="admin", passcode="%3D123")


async def test_ok(server: stompman.ConnectionParameters) -> None:
    destination = "DLQ"
    messages = [str(uuid4()).encode() for _ in range(10000)]

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

    async with (
        stompman.Client(servers=[server], read_timeout=10, connection_confirmation_timeout=10) as consumer,
        stompman.Client(servers=[server], read_timeout=10, connection_confirmation_timeout=10) as producer,
        asyncio.TaskGroup() as task_group,
    ):
        task_group.create_task(consume())
        task_group.create_task(produce())


async def test_raises_connection_lost_error(server: stompman.ConnectionParameters) -> None:
    with pytest.raises(ConnectionLostError):
        async with stompman.Client(servers=[server], read_timeout=10, connection_confirmation_timeout=10) as consumer:
            await consumer._connection.close()


def generate_frames(
    cases: list[tuple[bytes, list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]],
) -> tuple[list[bytes], list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]:
    all_bytes: list[bytes] = []
    all_frames = []

    for noise, frames in cases:
        current_all_bytes = []
        if noise:
            current_all_bytes.append(noise + NEWLINE)

        for frame in frames:
            dumped_frame = NEWLINE if isinstance(frame, HeartbeatFrame) else dump_frame(frame)
            current_all_bytes.append(dumped_frame)
            all_frames.append(frame)

        all_bytes.append(b"".join(current_all_bytes))

    return all_bytes, all_frames


def bytes_not_contains(*avoided: bytes) -> Callable[[bytes], bool]:
    return lambda checked: all(item not in checked for item in avoided)


noise_bytes_strategy = strategies.binary().filter(bytes_not_contains(NEWLINE, NULL))

frame_strategy = strategies.just(HeartbeatFrame()) | strategies.builds(
    make_frame_from_parts,
    command=strategies.sampled_from(tuple(COMMANDS_TO_FRAMES.keys())),
    headers=strategies.dictionaries(strategies.text(), strategies.text()).map(
        lambda headers: parse_headers(list(starmap(dump_header, headers.items()))) or {}
    ),
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
