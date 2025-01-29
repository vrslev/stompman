import pytest
import stompman
from faststream.broker.message import gen_cor_id
from faststream_stomp import StompBroker, TestStompBroker

pytestmark = pytest.mark.anyio


@pytest.fixture
def fake_connection_params() -> stompman.ConnectionParameters:
    return stompman.ConnectionParameters(host="0.0.0.0", port=61616, login="admin", passcode=":=123")  # noqa: S104


async def test_testing(fake_connection_params: stompman.ConnectionParameters) -> None:
    broker = StompBroker(stompman.Client([fake_connection_params]))
    destination = "test-test"
    correlation_id = gen_cor_id()

    @broker.subscriber(destination)
    def handle(body: str) -> None:
        assert body == "hi"

    async with TestStompBroker(broker) as br:
        await br.publish("hi", destination, correlation_id=correlation_id)
        assert handle.mock
        handle.mock.assert_called_once_with("hi")


async def test_request_not_implemented(fake_connection_params: stompman.ConnectionParameters) -> None:
    broker = StompBroker(stompman.Client([fake_connection_params]))

    async with TestStompBroker(broker):
        with pytest.raises(NotImplementedError):
            await broker.request("")
