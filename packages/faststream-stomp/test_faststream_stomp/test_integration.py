import asyncio

import faker
import faststream_stomp
import pytest
import stompman
from faststream import BaseMiddleware, Context, FastStream
from faststream.broker.message import gen_cor_id

pytestmark = pytest.mark.anyio


@pytest.fixture
def broker(connection_parameters: stompman.ConnectionParameters) -> faststream_stomp.StompBroker:
    return faststream_stomp.StompBroker(stompman.Client([connection_parameters]))


async def test_simple(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    app = FastStream(broker)
    expected_body, destination = faker.pystr(), faker.pystr()
    publisher = broker.publisher(destination)
    event = asyncio.Event()

    @broker.subscriber(destination)
    def _(body: str, message: stompman.MessageFrame = Context("message.raw_message")) -> None:  # noqa: B008
        assert body == expected_body
        event.set()

    @app.after_startup
    async def _() -> None:
        await broker.connect()
        await publisher.publish(expected_body.encode(), correlation_id=gen_cor_id())

    async with asyncio.timeout(1), asyncio.TaskGroup() as task_group:
        run_task = task_group.create_task(app.run())
        await event.wait()
        run_task.cancel()


async def test_republish(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    app = FastStream(broker)
    broker.add_middleware(BaseMiddleware)
    expected_body, first_destination, second_destination = faker.pystr(), faker.pystr(), faker.pystr()
    first_publisher, second_publisher = broker.publisher(first_destination), broker.publisher(second_destination)
    event = asyncio.Event()

    @broker.subscriber(first_destination)
    @second_publisher
    def _(body: str) -> str:
        return body

    @broker.subscriber(second_destination)
    def _(body: str) -> None:
        assert body == expected_body
        event.set()

    @app.after_startup
    async def _() -> None:
        await broker.connect()
        await first_publisher.publish(expected_body.encode())

    async with asyncio.timeout(10), asyncio.TaskGroup() as task_group:
        run_task = task_group.create_task(app.run())
        await event.wait()
        run_task.cancel()


async def test_router(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    expected_body, prefix, destination = faker.pystr(), faker.pystr(), faker.pystr()

    def route(body: str, message: stompman.MessageFrame = Context("message.raw_message")) -> None:  # noqa: B008
        assert body == expected_body
        event.set()

    router = faststream_stomp.StompRouter(prefix=prefix, handlers=(faststream_stomp.StompRoute(route, destination),))
    publisher = router.publisher(destination)

    broker.include_router(router)
    app = FastStream(broker)
    event = asyncio.Event()

    @app.after_startup
    async def _() -> None:
        await broker.connect()
        await publisher.publish(expected_body)

    async with asyncio.timeout(1), asyncio.TaskGroup() as task_group:
        run_task = task_group.create_task(app.run())
        await event.wait()
        run_task.cancel()


async def test_broker_close(broker: faststream_stomp.StompBroker) -> None:
    async with broker:
        pass


async def test_subscriber_lifespan(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    @broker.subscriber(faker.pystr())
    def _() -> None: ...

    await broker.start()
    await broker.close()


class TestPing:
    async def test_ok(self, broker: faststream_stomp.StompBroker) -> None:
        async with broker:
            assert await broker.ping()

    async def test_no_connection(self, broker: faststream_stomp.StompBroker) -> None:
        assert not await broker.ping()

    async def test_timeout(self, broker: faststream_stomp.StompBroker) -> None:
        async with broker:
            assert not await broker.ping(0)
