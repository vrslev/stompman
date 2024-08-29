from typing import TYPE_CHECKING, Literal

import faker
import pytest

import stompman

if TYPE_CHECKING:
    from stompman.config import MultiHostHostLike


class TestConnectionParametersFromPydanticMultiHostHosts:
    def test_ok(self, faker: faker.Faker) -> None:
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
    def test_fails_one_host_parsing(self, faker: faker.Faker, key: Literal["host", "port"]) -> None:
        hosts: list[MultiHostHostLike] = [
            {
                "host": faker.pystr(),
                "port": faker.pyint(),
                "username": faker.pystr(),
                "password": faker.pystr(),
                key: None,  # type: ignore[misc]
            }
        ]

        with pytest.raises(ValueError, match=f"{key} must be set"):
            stompman.ConnectionParameters.from_pydantic_multihost_hosts(hosts)
