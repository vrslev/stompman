import asyncio

import faker
import pytest
import stompman
from faststream import Context, FastStream
from faststream.broker.message import gen_cor_id
from faststream_stomp import StompBroker, StompRoute, StompRouter

pytestmark = pytest.mark.anyio


@pytest.fixture
def broker(connection_parameters: stompman.ConnectionParameters) -> StompBroker:
    return StompBroker(stompman.Client([connection_parameters]))


async def test_simple(faker: faker.Faker, broker: StompBroker) -> None:
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


async def test_router(faker: faker.Faker, broker: StompBroker) -> None:
    expected_body, prefix, destination = faker.pystr(), faker.pystr(), faker.pystr()

    def route(body: str, message: stompman.MessageFrame = Context("message.raw_message")) -> None:  # noqa: B008
        assert body == expected_body
        event.set()

    router = StompRouter(prefix=prefix, handlers=(StompRoute(route, destination),))
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


async def test_broker_close(broker: StompBroker) -> None:
    async with broker:
        pass


async def test_subscriber_lifespan(faker: faker.Faker, broker: StompBroker) -> None:
    @broker.subscriber(faker.pystr())
    def _() -> None: ...

    await broker.start()
    await broker.close()


class TestPing:
    async def test_ok(self, broker: StompBroker) -> None:
        async with broker:
            assert await broker.ping()

    async def test_no_connection(self, broker: StompBroker) -> None:
        assert not await broker.ping()

    async def test_timeout(self, broker: StompBroker) -> None:
        async with broker:
            assert not await broker.ping(0)
