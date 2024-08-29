from typing import TypedDict, cast

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


def test_connection_parameters_from_pydantic_multihost_hosts_ok() -> None:
    server = StrictMultiHostHostLikeFactory.build()
    result = stompman.ConnectionParameters.from_pydantic_multihost_hosts(
        [{**server, "port": index} for index in range(5)]
    )
    assert result == [
        stompman.ConnectionParameters(server["host"], port, server["username"], server["password"]) for port in range(5)
    ]


def test_connection_parameters_from_pydantic_multihost_hosts_fails() -> None:
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
