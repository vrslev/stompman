import asyncio

import stompman
from tests.integration import CONNECTION_PARAMETERS


async def main() -> None:
    async with (
        stompman.Client(servers=[CONNECTION_PARAMETERS]) as client,
        client.enter_transaction() as transaction,
    ):
        for _ in range(10):
            await client.send(body=b"hi there!", destination="DLQ", transaction=transaction)
        await asyncio.sleep(3)
        await client.send(body=b"hi there!", destination="DLQ", transaction=transaction)


if __name__ == "__main__":
    asyncio.run(main())
