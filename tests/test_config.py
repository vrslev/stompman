from typing import Any

import faker
import pytest

import stompman


def test_connection_parameters_from_pydantic_multihost_hosts_ok(faker: faker.Faker) -> None:
    full_host: dict[str, Any] = {
        "host": faker.pystr(),
        "port": faker.pyint(),
        "username": faker.pystr(),
        "password": faker.pystr(),
    }

    result = stompman.ConnectionParameters.from_pydantic_multihost_hosts(
        [{**full_host, "port": index} for index in range(5)]  # type: ignore[typeddict-item]
    )

    assert result == [
        stompman.ConnectionParameters(full_host["host"], port, full_host["username"], full_host["password"])
        for port in range(5)
    ]


def test_connection_parameters_from_pydantic_multihost_hosts_fails(faker: faker.Faker) -> None:
    full_host: dict[str, Any] = {
        "host": faker.pystr(),
        "port": faker.pyint(),
        "username": faker.pystr(),
        "password": faker.pystr(),
    }

    for key in ("host", "port", "username", "password"):
        with pytest.raises(ValueError, match=f"{key} must be set"):
            assert stompman.ConnectionParameters.from_pydantic_multihost_hosts(
                [{**full_host, key: None}, full_host],  # type: ignore[typeddict-item, list-item]
            )
