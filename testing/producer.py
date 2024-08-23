import asyncio
import os

import stompman


async def main() -> None:
    servers = [
        stompman.ConnectionParameters(
            host=os.environ["ARTEMIS_HOST"],
            port=61616,
            login="admin",
            passcode=":=123",
        )
    ]

    async with stompman.Client(servers) as client:
        await client.send(b"Hi!", "DLQ")

        async with client.begin() as transaction:
            for index in range(10):
                await transaction.send(b"Hi from transaction! " + str(index).encode(), "DLQ")
                await asyncio.sleep(0.3)


if __name__ == "__main__":
    asyncio.run(main())
