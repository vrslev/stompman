from typing import Any

import faker
import pytest

import stompman

FAKER = faker.Faker()


def test_connection_parameters_from_pydantic_multihost_hosts() -> None:
    full_host: dict[str, Any] = {
        "username": FAKER.pystr(),
        "password": FAKER.pystr(),
        "host": FAKER.pystr(),
        "port": FAKER.pyint(),
    }
    assert stompman.ConnectionParameters.from_pydantic_multihost_hosts(
        [{**full_host, "port": index} for index in range(5)]  # type: ignore[typeddict-item]
    ) == [
        stompman.ConnectionParameters(full_host["host"], index, full_host["username"], full_host["password"])
        for index in range(5)
    ]

    for key in ("username", "password", "host", "port"):
        with pytest.raises(ValueError, match=f"{key} must be set"):
            assert stompman.ConnectionParameters.from_pydantic_multihost_hosts([{**full_host, key: None}, full_host])  # type: ignore[typeddict-item, list-item]
