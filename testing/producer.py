import asyncio

import stompman


async def main() -> None:
    async with (
        stompman.Client(servers=[stompman.ConnectionParameters("0.0.0.0", 61616, "admin", "admin")]) as client,  # noqa: S104
        client.enter_transaction() as transaction,
    ):
        for _ in range(10):
            await client.send(body=b"hi there!", destination="DLQ", transaction=transaction)
        await asyncio.sleep(3)
        await client.send(body=b"hi there!", destination="DLQ", transaction=transaction)


if __name__ == "__main__":
    asyncio.run(main())
