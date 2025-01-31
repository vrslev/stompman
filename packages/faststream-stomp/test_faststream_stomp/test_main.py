from unittest import mock

import faker
import faststream_stomp
import pytest
import stompman
from faststream import FastStream
from faststream.asyncapi import get_app_schema
from faststream.broker.message import gen_cor_id
from faststream_stomp.opentelemetry import StompTelemetryMiddleware, StompTelemetrySettingsProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from test_stompman.conftest import build_dataclass

pytestmark = pytest.mark.anyio


@pytest.fixture
def fake_connection_params() -> stompman.ConnectionParameters:
    return build_dataclass(stompman.ConnectionParameters)


@pytest.fixture
def broker(fake_connection_params: stompman.ConnectionParameters) -> faststream_stomp.StompBroker:
    return faststream_stomp.StompBroker(stompman.Client([fake_connection_params]))


async def test_testing(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
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

    async with faststream_stomp.TestStompBroker(broker) as br:
        await br.publish(expected_body, first_destination, correlation_id=correlation_id)
        assert first_handle.mock
        first_handle.mock.assert_called_once_with(expected_body)
        assert second_publisher.mock
        second_publisher.mock.assert_called_once_with(expected_body)
        assert third_publisher.mock
        third_publisher.mock.assert_called_once_with(expected_body)


async def test_broker_request_not_implemented(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    async with faststream_stomp.TestStompBroker(broker):
        with pytest.raises(NotImplementedError):
            await broker.request(faker.pystr())


async def test_publisher_request_not_implemented(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    async with faststream_stomp.TestStompBroker(broker):
        with pytest.raises(NotImplementedError):
            await broker.publisher(faker.pystr()).request(faker.pystr())


def test_get_fmt(broker: faststream_stomp.StompBroker) -> None:
    broker.get_fmt()


def test_asyncapi_schema(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    broker.include_router(
        faststream_stomp.StompRouter(
            handlers=(
                faststream_stomp.StompRoute(
                    mock.Mock(), faker.pystr(), publishers=(faststream_stomp.StompRoutePublisher(faker.pystr()),)
                ),
            )
        )
    )
    get_app_schema(FastStream(broker))


async def test_opentelemetry_spans(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    resource = Resource.create(attributes={"service.name": "faststream.test"})
    tracer_provider = TracerProvider(resource=resource)
    span_exporter = InMemorySpanExporter()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    broker.add_middleware(StompTelemetryMiddleware(tracer_provider=tracer_provider))
    message, destination = faker.pystr(), faker.pystr()

    async with faststream_stomp.TestStompBroker(broker):
        await broker.start()
        await broker.publish(message, destination)

    assert [tuple((one_span.attributes or {}).values()) for one_span in span_exporter.get_finished_spans()] == [
        (StompTelemetrySettingsProvider.messaging_system, destination),
        (StompTelemetrySettingsProvider.messaging_system, destination, "publish"),
    ]


async def test_opentelemetry_metrics(faker: faker.Faker, broker: faststream_stomp.StompBroker) -> None:
    resource = Resource.create(attributes={"service.name": "faststream.test"})
    metric_reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=(metric_reader,), resource=resource)
    broker.add_middleware(StompTelemetryMiddleware(meter_provider=meter_provider))
    message, destination = faker.pystr(), faker.pystr()

    async with faststream_stomp.TestStompBroker(broker):
        await broker.start()
        await broker.publish(message, destination)

    metrics_data = metric_reader.get_metrics_data()
    assert metrics_data
    assert tuple(
        metrics_data.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points[0].attributes.values()
    ) == (
        StompTelemetrySettingsProvider.messaging_system,
        destination,
    )
