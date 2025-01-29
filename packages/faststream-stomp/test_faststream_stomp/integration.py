import asyncio

import pytest
import stompman
from faststream import Context, FastStream
from faststream_stomp import StompBroker, StompRoute, StompRouter, TestStompBroker


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


connection_params = stompman.ConnectionParameters(host="0.0.0.0", port=61616, login="admin", passcode=":=123")  # noqa: S104


@pytest.mark.anyio
async def test_integration_simple() -> None:
    app = FastStream(broker := StompBroker(stompman.Client([connection_params])))
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


@pytest.mark.anyio
async def test_integration_router() -> None:
    def route(body: str, message: stompman.MessageFrame = Context("message.raw_message")) -> None:  # noqa: B008
        assert body == "hi"
        event.set()

    destination = "test-test"
    router = StompRouter(prefix="hi-", handlers=(StompRoute(route, destination),))
    publisher = router.publisher(destination)

    broker = StompBroker(stompman.Client([connection_params]))
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


@pytest.mark.anyio
async def test_testing() -> None:
    broker = StompBroker(stompman.Client([connection_params]))
    destination = "test-test"

    @broker.subscriber(destination)
    def handle(body: str) -> None:
        assert body == "hi"

    async with TestStompBroker(broker) as br:
        await br.publish("hi", destination)
        assert handle.mock
        handle.mock.assert_called_once_with("hi")
