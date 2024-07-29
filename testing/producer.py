import asyncio

import stompman
from tests.integration import CONNECTION_PARAMETERS


async def main() -> None:
    async with stompman.Client(servers=[CONNECTION_PARAMETERS]) as client, client.begin() as transaction:
        for _ in range(10):
            await transaction.send(body=b"hi there!", destination="DLQ")
        await asyncio.sleep(3)
        await transaction.send(body=b"hi there!", destination="DLQ")


if __name__ == "__main__":
    asyncio.run(main())
