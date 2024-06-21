import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

import stompman
from stompman.errors import ConnectionLostError

pytestmark = pytest.mark.anyio


@asynccontextmanager
async def create_client() -> AsyncGenerator[stompman.Client, None]:
    server = stompman.ConnectionParameters(
        host=os.environ["ARTEMIS_HOST"], port=61616, login="admin", passcode="%3D123"
    )
    async with stompman.Client(servers=[server], read_timeout=10, connection_confirmation_timeout=10) as client:
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


async def test_not_raises_connection_lost_error_in_transaction_with_send(client: stompman.Client, destination: str) -> None:
    async with client.enter_transaction() as transaction:
        await client.send(b"first", destination=destination, transaction=transaction)
        await client._connection.close()

        with pytest.raises(ConnectionLostError):
            await client.send(b"second", destination=destination, transaction=transaction)


# async def test_raises_connection_lost_error_in_send(client: stompman.Client) -> None:
#     client.send()
#     async with client.enter_transaction():
#         await client._connection.close()


# async def test_raises_connection_lost_error_in_listen(client: stompman.Client) -> None:  # TODO
#     async with client.enter_transaction():
#         await client._connection.close()
