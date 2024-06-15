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


@dataclass
class StompFrame:
    headers: ConnectHeaders


@dataclass
class ConnectedFrame:
    headers: ConnectedHeaders


@dataclass
class SendFrame:
    headers: SendHeaders
    body: bytes = b""


@dataclass
class SubscribeFrame:
    headers: SubscribeHeaders


@dataclass
class UnsubscribeFrame:
    headers: UnsubscribeHeaders


@dataclass
class AckFrame:
    headers: AckHeaders


@dataclass
class NackFrame:
    headers: NackHeaders


@dataclass
class BeginFrame:
    headers: BeginHeaders


@dataclass
class CommitFrame:
    headers: CommitHeaders


@dataclass
class AbortFrame:
    headers: AbortHeaders


@dataclass
class DisconnectFrame:
    headers: DisconnectHeaders


@dataclass
class ReceiptFrame:
    headers: ReceiptHeaders


@dataclass
class MessageFrame:
    headers: MessageHeaders
    body: bytes


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
