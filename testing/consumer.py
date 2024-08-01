import asyncio

import stompman
from tests.integration import CONNECTION_PARAMETERS


async def main() -> None:
    async def handle_message(frame: stompman.MessageFrame) -> None:  # noqa: RUF029
        print(frame)  # noqa: T201

    async with stompman.Client(servers=[CONNECTION_PARAMETERS]) as client:
        await client.subscribe("DLQ", handler=handle_message, on_suppressed_exception=print)


if __name__ == "__main__":
    asyncio.run(main())
