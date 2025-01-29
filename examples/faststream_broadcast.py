import asyncio

import faststream
import stompman
from faststream_stomp.broker import StompBroker

server = stompman.ConnectionParameters(host="127.0.0.1", port=9000, login="admin", passcode=":=123")
broker = StompBroker(stompman.Client([server]))


@broker.subscriber("first")
@broker.publisher("second")
def _(message: str) -> str:
    print(message)  # noqa: T201
    return "Hi from first handler!"


@broker.subscriber("second")
def _(message: str) -> None:
    print(message)  # noqa: T201


app = faststream.FastStream(broker)


@app.after_startup
async def send_first_message() -> None:
    await broker.connect()
    await broker.publish("Hi from startup!", "first")


if __name__ == "__main__":
    asyncio.run(app.run())
