from dataclasses import dataclass
from typing import TYPE_CHECKING

from stompman.frames import ErrorFrame, MessageFrame, ReceiptFrame

if TYPE_CHECKING:
    from stompman.client import ConnectionParameters


@dataclass(frozen=True, kw_only=True, slots=True)
class Error(Exception):
    def __str__(self) -> str:
        return self.__repr__()


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionConfirmationTimeoutError(Error):
    timeout: int
    frames: list[MessageFrame | ReceiptFrame | ErrorFrame]


@dataclass(frozen=True, kw_only=True, slots=True)
class UnsupportedProtocolVersionError(Error):
    given_version: str
    supported_version: str


@dataclass(frozen=True, kw_only=True, slots=True)
class FailedAllConnectAttemptsError(Error):
    servers: list["ConnectionParameters"]
    retry_attempts: int
    retry_interval: int
    timeout: int


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionLostError(Error): ...
