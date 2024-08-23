import asyncio
import os

import stompman

server = stompman.ConnectionParameters(
    host=os.environ.get("ARTEMIS_HOST", "0.0.0.0"),  # noqa: S104
    port=61616,
    login="admin",
    passcode=":=123",
)


async def main() -> None:
    async def handle_message(frame: stompman.MessageFrame) -> None:  # noqa: RUF029
        print(frame)  # noqa: T201

    async with stompman.Client(servers=[server]) as client:
        await client.subscribe("DLQ", handler=handle_message, on_suppressed_exception=print)


if __name__ == "__main__":
    asyncio.run(main())
