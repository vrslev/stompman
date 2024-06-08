from dataclasses import dataclass
from typing import Generic, Literal, TypedDict, TypeVar

HeadersType = TypeVar("HeadersType")


@dataclass(frozen=True, kw_only=True)
class BaseFrame(Generic[HeadersType]):
    command: str
    headers: HeadersType
    body: bytes = b""


ConnectHeaders = TypedDict(
    "ConnectHeaders",
    {
        "accept-version": str,
        "host": str,
        "login": str,
        "passcode": str,
        "heart-beat": str,
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class ConnectFrame(BaseFrame[ConnectHeaders]):
    command: Literal["CONNECT", "STOMP"] = "CONNECT"


ConnectedHeaders = TypedDict(
    "ConnectedHeaders",
    {"version": str, "server": str, "heart-beat": str},
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class ConnectedFrame(BaseFrame[ConnectedHeaders]):
    command: Literal["CONNECTED"] = "CONNECTED"


SendHeaders = TypedDict(
    "SendHeaders",
    {
        "content-length": str,
        "content-type": str,
        "destination": str,
        "transaction": str,
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class SendFrame(BaseFrame[SendHeaders | dict[str, str]]):
    command: Literal["SEND"] = "SEND"


class SubscribeHeaders(TypedDict, total=False):
    id: str
    destination: str
    ack: Literal["client", "client-individual", "auto"]
    transaction: str


@dataclass(frozen=True, kw_only=True)
class SubscribeFrame(BaseFrame[SubscribeHeaders]):
    command: Literal["SUBSCRIBE"] = "SUBSCRIBE"


class UnsubscribeHeaders(TypedDict, total=False):
    id: str


@dataclass(frozen=True, kw_only=True)
class UnsubscribeFrame(BaseFrame[UnsubscribeHeaders]):
    command: Literal["UNSUBSCRIBE"] = "UNSUBSCRIBE"


AckHeaders = TypedDict(
    "AckHeaders",
    {
        "subscription": str,
        "message-id": str,
        "transaction": str,
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class AckFrame(BaseFrame[AckHeaders]):
    command: Literal["ACK"] = "ACK"


NackHeaders = TypedDict(
    "NackHeaders",
    {
        "subscription": str,
        "message-id": str,
        "transaction": str,
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class NackFrame(BaseFrame[NackHeaders]):
    command: Literal["NACK"] = "NACK"


class BeginHeaders(TypedDict, total=False):
    transaction: str


@dataclass(frozen=True, kw_only=True)
class BeginFrame(BaseFrame[BeginHeaders]):
    command: Literal["BEGIN"] = "BEGIN"


class CommitHeaders(TypedDict, total=False):
    transaction: str


@dataclass(frozen=True, kw_only=True)
class CommitFrame(BaseFrame[CommitHeaders]):
    command: Literal["COMMIT"] = "COMMIT"


class AbortHeaders(TypedDict, total=False):
    transaction: str


@dataclass(frozen=True, kw_only=True)
class AbortFrame(BaseFrame[AbortHeaders]):
    command: Literal["ABORT"] = "ABORT"


class DisconnectHeaders(TypedDict, total=False):
    receipt: str


@dataclass(frozen=True, kw_only=True)
class DisconnectFrame(BaseFrame[DisconnectHeaders]):
    command: Literal["DISCONNECT"] = "DISCONNECT"


ReceiptHeaders = TypedDict(
    "ReceiptHeaders",
    {
        "receipt-id": str,
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class ReceiptFrame(BaseFrame[ReceiptHeaders]):
    command: Literal["RECEIPT"] = "RECEIPT"


MessageHeaders = TypedDict(
    "MessageHeaders",
    {
        "subscription": str,
        "message-id": str,
        "destination": str,
        "content-type": str,
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class MessageFrame(BaseFrame[MessageHeaders]):
    command: Literal["MESSAGE"] = "MESSAGE"


class ErrorHeaders(TypedDict, total=False):
    message: str


@dataclass(frozen=True, kw_only=True)
class ErrorFrame(BaseFrame[ErrorHeaders]):
    command: Literal["ERROR"] = "ERROR"


@dataclass(frozen=True, kw_only=True)
class HeartbeatFrame(BaseFrame[dict[str, str]]):
    command: Literal["HEARTBEAT"] = "HEARTBEAT"


@dataclass(frozen=True, kw_only=True)
class UnknownFrame(BaseFrame[dict[str, str]]): ...


ClientFrame = (
    ConnectFrame
    | SendFrame
    | SubscribeFrame
    | UnsubscribeFrame
    | BeginFrame
    | CommitFrame
    | AbortFrame
    | AckFrame
    | NackFrame
    | DisconnectFrame
)
ServerFrame = ConnectedFrame | MessageFrame | ReceiptFrame | ErrorFrame | HeartbeatFrame

AnyFrame = ClientFrame | ServerFrame | UnknownFrame

COMMANDS_TO_FRAME_TYPES: dict[str, type[ClientFrame | ServerFrame]] = {
    "CONNECT": ConnectFrame,
    "STOMP": ConnectFrame,
    "SEND": SendFrame,
    "SUBSCRIBE": SubscribeFrame,
    "UNSUBSCRIBE": UnsubscribeFrame,
    "BEGIN": BeginFrame,
    "COMMIT": CommitFrame,
    "ABORT": AbortFrame,
    "ACK": AckFrame,
    "NACK": NackFrame,
    "DISCONNECT": DisconnectFrame,
    "HEARTBEAT": HeartbeatFrame,
    "CONNECTED": ConnectedFrame,
    "MESSAGE": MessageFrame,
    "RECEIPT": ReceiptFrame,
    "ERROR": ErrorFrame,
}
