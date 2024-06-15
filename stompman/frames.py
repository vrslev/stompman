from dataclasses import dataclass

from stompman.frame_headers import (
    AbortHeaders,
    AckHeaders,
    BeginHeaders,
    CommitHeaders,
    ConnectedHeaders,
    ConnectHeaders,
    DisconnectHeaders,
    ErrorHeaders,
    MessageHeaders,
    NackHeaders,
    ReceiptHeaders,
    SendHeaders,
    SubscribeHeaders,
    UnsubscribeHeaders,
)


@dataclass
class ConnectFrame:
    headers: ConnectHeaders
    body: bytes = b""


@dataclass
class StompFrame:
    headers: ConnectHeaders
    body: bytes = b""


@dataclass
class ConnectedFrame:
    headers: ConnectedHeaders
    body: bytes = b""


@dataclass
class SendFrame:
    headers: SendHeaders
    body: bytes = b""


@dataclass
class SubscribeFrame:
    headers: SubscribeHeaders
    body: bytes = b""


@dataclass
class UnsubscribeFrame:
    headers: UnsubscribeHeaders
    body: bytes = b""


@dataclass
class AckFrame:
    headers: AckHeaders
    body: bytes = b""


@dataclass
class NackFrame:
    headers: NackHeaders
    body: bytes = b""


@dataclass
class BeginFrame:
    headers: BeginHeaders
    body: bytes = b""


@dataclass
class CommitFrame:
    headers: CommitHeaders
    body: bytes = b""


@dataclass
class AbortFrame:
    headers: AbortHeaders
    body: bytes = b""


@dataclass
class DisconnectFrame:
    headers: DisconnectHeaders
    body: bytes = b""


@dataclass
class ReceiptFrame:
    headers: ReceiptHeaders
    body: bytes = b""


@dataclass
class MessageFrame:
    headers: MessageHeaders
    body: bytes = b""


@dataclass
class ErrorFrame:
    headers: ErrorHeaders
    body: bytes = b""


@dataclass
class HeartbeatFrame: ...


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


COMMANDS_TO_FRAMES: dict[bytes, type[AnyClientFrame | AnyServerFrame]] = {
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
