import asyncio

import stompman
from tests.integration import CONNECTION_PARAMETERS


async def main() -> None:
    async with (
        stompman.Client(servers=[CONNECTION_PARAMETERS]) as client,
        client.subscribe("DLQ"),
    ):
        async for event in client.listen():
            print(event)  # noqa: T201
            match event:
                case stompman.MessageEvent():
                    await event.ack()


if __name__ == "__main__":
    asyncio.run(main())
