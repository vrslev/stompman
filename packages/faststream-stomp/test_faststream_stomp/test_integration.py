import asyncio

import pytest
import stompman
from faststream import Context, FastStream
from faststream_stomp import StompBroker, StompRoute, StompRouter

pytestmark = pytest.mark.anyio


async def test_simple(connection_parameters: stompman.ConnectionParameters) -> None:
    app = FastStream(broker := StompBroker(stompman.Client([connection_parameters])))
    publisher = broker.publisher(destination := "test-test")
    event = asyncio.Event()

    @broker.subscriber(destination)
    def _(body: str, message: stompman.MessageFrame = Context("message.raw_message")) -> None:  # noqa: B008
        assert body == "hi"
        event.set()

    @app.after_startup
    async def _() -> None:
        await broker.connect()
        await publisher.publish(b"hi")

    async with asyncio.timeout(10), asyncio.TaskGroup() as task_group:
        run_task = task_group.create_task(app.run())
        await event.wait()
        run_task.cancel()


async def test_router(connection_parameters: stompman.ConnectionParameters) -> None:
    def route(body: str, message: stompman.MessageFrame = Context("message.raw_message")) -> None:  # noqa: B008
        assert body == "hi"
        event.set()

    destination = "test-test"
    router = StompRouter(prefix="hi-", handlers=(StompRoute(route, destination),))
    publisher = router.publisher(destination)

    broker = StompBroker(stompman.Client([connection_parameters]))
    broker.include_router(router)
    app = FastStream(broker)
    event = asyncio.Event()

    @app.after_startup
    async def _() -> None:
        await broker.connect()
        await publisher.publish(b"hi")

    async with asyncio.timeout(10), asyncio.TaskGroup() as task_group:
        run_task = task_group.create_task(app.run())
        await event.wait()
        run_task.cancel()


async def test_broker_close(connection_parameters: stompman.ConnectionParameters) -> None:
    async with StompBroker(stompman.Client([connection_parameters])):
        pass
