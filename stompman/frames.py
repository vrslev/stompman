from dataclasses import dataclass
from typing import Literal, NotRequired, TypedDict, TypeVar

HeadersType = TypeVar("HeadersType")


ConnectHeaders = TypedDict(
    "ConnectHeaders",
    {
        "accept-version": str,
        "host": str,
        "login": NotRequired[str],
        "passcode": NotRequired[str],
        "heart-beat": NotRequired[str],
        "content-length": NotRequired[str],
    },
)


@dataclass
class ConnectFrame:
    headers: ConnectHeaders
    body: bytes = b""


@dataclass
class StompFrame:
    headers: ConnectHeaders
    body: bytes = b""


ConnectedHeaders = TypedDict(
    "ConnectedHeaders",
    {
        "version": str,
        "server": NotRequired[str],
        "heart-beat": NotRequired[str],
        "content-length": NotRequired[str],
    },
)


@dataclass
class ConnectedFrame:
    headers: ConnectedHeaders
    body: bytes = b""


SendHeaders = TypedDict(
    "SendHeaders",
    {
        "content-length": NotRequired[str],
        "content-type": NotRequired[str],
        "destination": str,
        "transaction": NotRequired[str],
    },
)


@dataclass
class SendFrame:
    headers: SendHeaders
    body: bytes = b""


SubscribeHeaders = TypedDict(
    "SubscribeHeaders",
    {
        "id": str,
        "destination": str,
        "ack": NotRequired[Literal["client", "client-individual", "auto"]],
        "content-length": NotRequired[str],
    },
)


@dataclass
class SubscribeFrame:
    headers: SubscribeHeaders
    body: bytes = b""


UnsubscribeHeaders = TypedDict("UnsubscribeHeaders", {"id": str, "content-length": NotRequired[str]})


@dataclass
class UnsubscribeFrame:
    headers: UnsubscribeHeaders
    body: bytes = b""


AckHeaders = TypedDict(
    "AckHeaders",
    {
        "subscription": str,
        "id": str,
        "transaction": NotRequired[str],
        "content-length": NotRequired[str],
    },
)


@dataclass
class AckFrame:
    headers: AckHeaders
    body: bytes = b""


NackHeaders = TypedDict(
    "NackHeaders",
    {
        "subscription": str,
        "id": str,
        "transaction": NotRequired[str],
        "content-length": NotRequired[str],
    },
)


@dataclass
class NackFrame:
    headers: NackHeaders
    body: bytes = b""


BeginHeaders = TypedDict("BeginHeaders", {"transaction": NotRequired[str], "content-length": NotRequired[str]})


@dataclass
class BeginFrame:
    headers: BeginHeaders
    body: bytes = b""


CommitHeaders = TypedDict("CommitHeaders", {"transaction": NotRequired[str], "content-length": NotRequired[str]})


@dataclass
class CommitFrame:
    headers: CommitHeaders
    body: bytes = b""


AbortHeaders = TypedDict("AbortHeaders", {"transaction": NotRequired[str], "content-length": NotRequired[str]})


@dataclass
class AbortFrame:
    headers: AbortHeaders
    body: bytes = b""


DisconnectHeaders = TypedDict("DisconnectHeaders", {"receipt": NotRequired[str], "content-length": NotRequired[str]})


@dataclass
class DisconnectFrame:
    headers: DisconnectHeaders
    body: bytes = b""


ReceiptHeaders = TypedDict("ReceiptHeaders", {"receipt-id": str, "content-length": NotRequired[str]})


@dataclass
class ReceiptFrame:
    headers: ReceiptHeaders
    body: bytes = b""


MessageHeaders = TypedDict(
    "MessageHeaders",
    {
        "destination": str,
        "message-id": str,
        "subscription": str,
        "ack": NotRequired[str],
        "content-type": NotRequired[str],
        "content-length": NotRequired[str],
    },
)


@dataclass
class MessageFrame:
    headers: MessageHeaders
    body: bytes = b""


ErrorHeaders = TypedDict(
    "ErrorHeaders",
    {"message": NotRequired[str], "content-length": NotRequired[str], "content-type": NotRequired[str]},
)


@dataclass
class ErrorFrame:
    headers: ErrorHeaders
    body: bytes = b""


@dataclass
class HeartbeatFrame: ...


COMMANDS_TO_FRAMES: dict[
    bytes,
    type[
        SendFrame
        | SubscribeFrame
        | UnsubscribeFrame
        | BeginFrame
        | CommitFrame
        | AbortFrame
        | AckFrame
        | NackFrame
        | DisconnectFrame
        | ConnectFrame
        | StompFrame
        # ...
        | ConnectedFrame
        | MessageFrame
        | ReceiptFrame
        | ErrorFrame
    ],
] = {
    # Client frames
    b"SEND": SendFrame,
    b"SUBSCRIBE": SubscribeFrame,
    b"UNSUBSCRIBE": UnsubscribeFrame,
    b"BEGIN": BeginFrame,
    b"COMMIT": CommitFrame,
    b"ABORT": AbortFrame,
    b"ACK": AckFrame,
    b"NACK": NackFrame,
    b"DISCONNECT": DisconnectFrame,
    b"CONNECT": ConnectFrame,
    b"STOMP": StompFrame,
    # Server frames
    b"CONNECTED": ConnectedFrame,
    b"MESSAGE": MessageFrame,
    b"RECEIPT": ReceiptFrame,
    b"ERROR": ErrorFrame,
}
FRAMES_TO_COMMANDS = {value: key for key, value in COMMANDS_TO_FRAMES.items()}

AnyClientFrame = (
    SendFrame
    | SubscribeFrame
    | UnsubscribeFrame
    | BeginFrame
    | CommitFrame
    | AbortFrame
    | AckFrame
    | NackFrame
    | DisconnectFrame
    | ConnectFrame
    | StompFrame
)
AnyServerFrame = ConnectedFrame | MessageFrame | ReceiptFrame | ErrorFrame
