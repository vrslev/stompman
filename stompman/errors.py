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


@dataclass(frozen=True, kw_only=True, slots=True)
class AllServersUnavailable:
    servers: list["ConnectionParameters"]
    timeout: int


StompProtocolConnectionIssue = ConnectionConfirmationTimeout | UnsupportedProtocolVersion
AnyConnectionIssue = StompProtocolConnectionIssue | ConnectionLost | AllServersUnavailable


@dataclass(kw_only=True)
class FailedAllConnectAttemptsError(Error):
    retry_attempts: int
    issues: list[AnyConnectionIssue]


@dataclass(kw_only=True)
class ConnectionLostDuringOperationError(Error):
    retry_attempts: int
