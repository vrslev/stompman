from dataclasses import dataclass, field
from typing import Self, TypedDict
from urllib.parse import unquote


@dataclass(frozen=True, slots=True)
class Heartbeat:
    will_send_interval_ms: int
    want_to_receive_interval_ms: int

    def to_header(self) -> str:
        return f"{self.will_send_interval_ms},{self.want_to_receive_interval_ms}"

    @classmethod
    def from_header(cls, header: str) -> Self:
        first, second = header.split(",", maxsplit=1)
        return cls(int(first), int(second))


class MultiHostHostLike(TypedDict):
    username: str | None
    password: str | None
    host: str | None
    port: int | None


@dataclass(frozen=True, slots=True)
class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str = field(repr=False)

    @property
    def unescaped_passcode(self) -> str:
        return unquote(self.passcode)

    @classmethod
    def from_pydantic_multihost_hosts(cls, hosts: list[MultiHostHostLike]) -> list[Self]:
        """Create connection parameters from a list of `MultiHostUrl` objects.

        .. code-block:: python
        import stompman.

        ArtemisDsn = typing.Annotated[
            pydantic_core.MultiHostUrl,
            pydantic.UrlConstraints(
                host_required=True,
                allowed_schemes=["tcp"],
            ),
        ]

        async with stompman.Client(
            servers=stompman.ConnectionParameters.from_pydantic_multihost_hosts(
                ArtemisDsn("tcp://lev:pass@host1:61616,lev:pass@host1:61617,lev:pass@host2:61616").hosts()
            ),
        ):
            ...
        """
        servers: list[Self] = []
        for host in hosts:
            if host["host"] is None:
                msg = "host must be set"
                raise ValueError(msg)
            if host["port"] is None:
                msg = "port must be set"
                raise ValueError(msg)
            if host["username"] is None:
                msg = "username must be set"
                raise ValueError(msg)
            if host["password"] is None:
                msg = "password must be set"
                raise ValueError(msg)

            servers.append(cls(host=host["host"], port=host["port"], login=host["username"], passcode=host["password"]))
        return servers
