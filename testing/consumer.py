import asyncio

import stompman


async def main() -> None:
    async with (
        stompman.Client(servers=[stompman.ConnectionParameters("0.0.0.0", 61616, "admin", "admin")]) as client,  # noqa: S104
        client.subscribe("DLQ"),
    ):
        async for event in client.listen():
            print(event)  # noqa: T201
            match event:
                case stompman.MessageEvent():
                    await event.ack()


if __name__ == "__main__":
    asyncio.run(main())
