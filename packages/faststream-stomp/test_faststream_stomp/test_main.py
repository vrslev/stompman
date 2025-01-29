import pytest
import stompman
from faststream.broker.message import gen_cor_id
from faststream_stomp import StompBroker, TestStompBroker

pytestmark = pytest.mark.anyio


@pytest.fixture
def fake_connection_params() -> stompman.ConnectionParameters:
    return stompman.ConnectionParameters(host="0.0.0.0", port=61616, login="admin", passcode=":=123")  # noqa: S104


@pytest.fixture
def broker(fake_connection_params: stompman.ConnectionParameters) -> StompBroker:
    return StompBroker(stompman.Client([fake_connection_params]))


async def test_testing(broker: StompBroker) -> None:
    destination = "test-test"
    correlation_id = gen_cor_id()

    @broker.subscriber(destination)
    def handle(body: str) -> None:
        assert body == "hi"

    async with TestStompBroker(broker) as br:
        await br.publish("hi", destination, correlation_id=correlation_id)
        assert handle.mock
        handle.mock.assert_called_once_with("hi")


async def test_request_not_implemented(broker: StompBroker) -> None:
    async with TestStompBroker(broker):
        with pytest.raises(NotImplementedError):
            await broker.request("")


def test_get_fmt(broker: StompBroker) -> None:
    broker.get_fmt()
