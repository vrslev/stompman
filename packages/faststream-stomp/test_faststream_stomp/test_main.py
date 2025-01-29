import pytest
import stompman
from faststream_stomp import StompBroker, TestStompBroker

pytestmark = pytest.mark.anyio


async def test_testing() -> None:
    broker = StompBroker(
        stompman.Client([stompman.ConnectionParameters(host="0.0.0.0", port=61616, login="admin", passcode=":=123")])  # noqa: S104
    )
    destination = "test-test"

    @broker.subscriber(destination)
    def handle(body: str) -> None:
        assert body == "hi"

    async with TestStompBroker(broker) as br:
        await br.publish("hi", destination)
        assert handle.mock
        handle.mock.assert_called_once_with("hi")
