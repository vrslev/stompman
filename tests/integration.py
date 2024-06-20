import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

import stompman
from stompman.errors import ConnectionLostError

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


@asynccontextmanager
async def closed_client(server: stompman.ConnectionParameters) -> AsyncGenerator[stompman.Client, None]:
    async with stompman.Client(servers=[server], read_timeout=10, connection_confirmation_timeout=10) as client:
        await client._connection.close()
        yield client


async def test_not_raises_connection_lost_error_in_aexit(server: stompman.ConnectionParameters) -> None:
    async with closed_client(server):
        pass


async def test_not_raises_connection_lost_error_in_write_frame(server: stompman.ConnectionParameters) -> None:
    async with closed_client(server) as client:
        with pytest.raises(ConnectionLostError):
            await client._connection.write_frame(stompman.ConnectFrame(headers={"accept-version": "", "host": ""}))


@pytest.mark.parametrize("anyio_backend", [("asyncio", {"use_uvloop": True})])
async def test_not_raises_connection_lost_error_in_write_heartbeat(server: stompman.ConnectionParameters) -> None:
    async with closed_client(server) as client:
        with pytest.raises(ConnectionLostError):
            client._connection.write_heartbeat()
