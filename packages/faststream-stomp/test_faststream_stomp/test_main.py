from unittest import mock

import faker
import faststream
import pytest
import stompman
from faststream.asyncapi import get_app_schema
from faststream.broker.message import gen_cor_id
from faststream_stomp import StompBroker, TestStompBroker
from faststream_stomp.router import StompRoute, StompRoutePublisher, StompRouter
from test_stompman.conftest import build_dataclass

pytestmark = pytest.mark.anyio


@pytest.fixture
def fake_connection_params() -> stompman.ConnectionParameters:
    return build_dataclass(stompman.ConnectionParameters)


@pytest.fixture
def broker(fake_connection_params: stompman.ConnectionParameters) -> StompBroker:
    return StompBroker(stompman.Client([fake_connection_params]))


async def test_testing(faker: faker.Faker, broker: StompBroker) -> None:
    expected_body, first_destination, second_destination, third_destination, correlation_id = (
        faker.pystr(),
        faker.pystr(),
        faker.pystr(),
        faker.pystr(),
        gen_cor_id(),
    )
    second_publisher = broker.publisher(second_destination)
    third_publisher = broker.publisher(third_destination)

    @broker.subscriber(first_destination)
    @second_publisher
    @third_publisher
    def first_handle(body: str) -> str:
        assert body == expected_body
        return body

    @broker.subscriber(second_destination)
    def second_handle(body: str) -> None:
        assert body == expected_body

    async with TestStompBroker(broker) as br:
        await br.publish(expected_body, first_destination, correlation_id=correlation_id)
        assert first_handle.mock
        first_handle.mock.assert_called_once_with(expected_body)
        assert second_publisher.mock
        second_publisher.mock.assert_called_once_with(expected_body)
        assert third_publisher.mock
        third_publisher.mock.assert_called_once_with(expected_body)


async def test_broker_request_not_implemented(faker: faker.Faker, broker: StompBroker) -> None:
    async with TestStompBroker(broker):
        with pytest.raises(NotImplementedError):
            await broker.request(faker.pystr())


async def test_publisher_request_not_implemented(faker: faker.Faker, broker: StompBroker) -> None:
    async with TestStompBroker(broker):
        with pytest.raises(NotImplementedError):
            await broker.publisher(faker.pystr()).request(faker.pystr())


def test_get_fmt(broker: StompBroker) -> None:
    broker.get_fmt()


def test_asyncapi_schema(faker: faker.Faker, broker: StompBroker) -> None:
    broker.include_router(
        StompRouter(
            handlers=(StompRoute(mock.Mock(), faker.pystr(), publishers=(StompRoutePublisher(faker.pystr()),)),)
        )
    )
    get_app_schema(faststream.FastStream(broker))
