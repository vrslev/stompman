import asyncio

from stompman.client import Client
from stompman.connection import ConnectionParameters


async def main() -> None:
    m = Client(
        servers=[
            ConnectionParameters("0.0.0.0", 61616, "", ""),  # noqa: S104
            ConnectionParameters("0.0.0.0", 61611, "", ""),  # noqa: S104
            ConnectionParameters("0.0.0.0", 61636, "", ""),  # noqa: S104
        ],
        connect_retry_attempts=3,
        connect_retry_interval=0,
        connect_timeout=2,
        read_timeout=1,
        read_max_chunk_size=1,
    )
    await m._connect_to_any_server()


if __name__ == "__main__":
    asyncio.run(main())
