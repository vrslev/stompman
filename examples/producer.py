import asyncio

import stompman

server = stompman.ConnectionParameters(host="0.0.0.0", port=61616, login="admin", passcode=":=123")  # noqa: S104


async def main() -> None:
    async with stompman.Client([server]) as client:
        await client.send(b"Hi!", "DLQ")
        print("Said hi")  # noqa: T201

        async with client.begin() as transaction:
            for index in range(5):
                await transaction.send(b"Hi from transaction! " + str(index).encode(), "DLQ")
                print(f"Said hi in transaction ({index})")  # noqa: T201
                await asyncio.sleep(0.3)

        await client.send(b"Mu-ha-ha!", "DLQ")
        print("Laughed evilly")  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
