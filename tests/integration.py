import asyncio
from uuid import uuid4

import stompman


async def test_integration() -> None:
    server = stompman.ConnectionParameters(host="0.0.0.0", port=61616, login="admin", passcode="admin")  # noqa: S104
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
        stompman.Client(servers=[server]) as consumer,
        stompman.Client(servers=[server]) as producer,
        asyncio.TaskGroup() as task_group,
    ):
        task_group.create_task(consume())
        task_group.create_task(produce())
