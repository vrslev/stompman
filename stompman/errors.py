from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stompman.connection import ConnectionParameters


@dataclass
class Error(Exception):
    def __str__(self) -> str:
        return self.__repr__()


@dataclass
class ConnectionConfirmationTimeoutError(Error):
    timeout: int


@dataclass
class UnsupportedProtocolVersionError(Error):
    given_version: str
    supported_version: str


@dataclass
class FailedAllConnectAttemptsError(Error):
    servers: list["ConnectionParameters"]
    retry_attempts: int
    retry_interval: int
    timeout: int


@dataclass
class ConnectionLostError(Error):
    timeout: int
