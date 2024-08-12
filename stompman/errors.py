from dataclasses import dataclass
from typing import TYPE_CHECKING

from stompman.frames import ErrorFrame, HeartbeatFrame, MessageFrame, ReceiptFrame

if TYPE_CHECKING:
    from stompman.config import ConnectionParameters


@dataclass(kw_only=True)
class Error(Exception):
    def __str__(self) -> str:
        return self.__repr__()


@dataclass(kw_only=True)
class ConnectionLostError(Error):
    """Raised in stompman.AbstractConnection—and handled in stompman.ConnectionManager, therefore is private."""


@dataclass(kw_only=True)
class FailedAllConnectAttemptsError(Error):
    servers: list["ConnectionParameters"]
    retry_attempts: int
    retry_interval: int
    timeout: int


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionConfirmationTimeout:
    timeout: int
    frames: list[MessageFrame | ReceiptFrame | ErrorFrame | HeartbeatFrame]


@dataclass(frozen=True, kw_only=True, slots=True)
class UnsupportedProtocolVersion:
    given_version: str
    supported_version: str


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionLost: ...


StompProtocolConnectionIssue = ConnectionConfirmationTimeout | UnsupportedProtocolVersion


@dataclass(kw_only=True)
class RepeatedConnectionFailedError(Error):
    retry_attempts: int
    issues: list[StompProtocolConnectionIssue | ConnectionLost]


@dataclass(kw_only=True)
class RepeatedConnectionLostError(Error):
    retry_attempts: int
