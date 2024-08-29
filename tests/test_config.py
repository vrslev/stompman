from typing import TypedDict, cast

import faker
import pytest
from polyfactory.factories.typed_dict_factory import TypedDictFactory

import stompman
from stompman.config import MultiHostHostLike


class StrictMultiHostHostLike(TypedDict):
    username: str
    password: str
    host: str
    port: int


class StrictMultiHostHostLikeFactory(TypedDictFactory[StrictMultiHostHostLike]): ...


def test_connection_parameters_from_pydantic_multihost_hosts_ok(faker: faker.Faker) -> None:
    hosts: list[MultiHostHostLike] = [
        {"host": "host1", "port": 1, "username": None, "password": None},
        {"host": "host2", "port": 2, "username": None, "password": None},
        {"host": "host3", "port": 3, "username": None, "password": None},
        {"host": "host4", "port": 4, "username": None, "password": None},
    ]
    host_with_credentials = faker.pyint(min_value=0, max_value=3)
    hosts[host_with_credentials]["username"] = "lev"
    hosts[host_with_credentials]["password"] = "pass"  # noqa: S105

    result = stompman.ConnectionParameters.from_pydantic_multihost_hosts(hosts)

    assert result == [
        stompman.ConnectionParameters("host1", 1, "lev", "pass"),
        stompman.ConnectionParameters("host2", 2, "lev", "pass"),
        stompman.ConnectionParameters("host3", 3, "lev", "pass"),
        stompman.ConnectionParameters("host4", 4, "lev", "pass"),
    ]


@pytest.mark.parametrize("key", ["host", "port"])
def test_connection_parameters_from_pydantic_multihost_hosts_fails_one_host_parsing() -> None:
    server = StrictMultiHostHostLikeFactory.build()

    for key in ("host", "port", "username", "password"):
        with pytest.raises(ValueError, match=f"{key} must be set"):
            assert stompman.ConnectionParameters.from_pydantic_multihost_hosts(
                [
                    {
                        **server,
                        key: None,  # type: ignore[misc]
                    },
                    cast(MultiHostHostLike, server),
                ],
            )
