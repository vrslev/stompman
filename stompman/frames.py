from dataclasses import dataclass
from typing import Literal, NotRequired, TypedDict

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
ConnectedHeaders = TypedDict(
    "ConnectedHeaders",
    {
        "version": str,
        "server": NotRequired[str],
        "heart-beat": NotRequired[str],
        "content-length": NotRequired[str],
    },
)
SendHeaders = TypedDict(
    "SendHeaders",
    {
        "content-length": NotRequired[str],
        "content-type": NotRequired[str],
        "destination": str,
        "transaction": NotRequired[str],
    },
)
SubscribeHeaders = TypedDict(
    "SubscribeHeaders",
    {
        "id": str,
        "destination": str,
        "ack": NotRequired[Literal["client", "client-individual", "auto"]],
        "content-length": NotRequired[str],
    },
)
UnsubscribeHeaders = TypedDict(
    "UnsubscribeHeaders",
    {
        "id": str,
        "content-length": NotRequired[str],
    },
)
AckHeaders = TypedDict(
    "AckHeaders",
    {
        "subscription": str,
        "id": str,
        "transaction": NotRequired[str],
        "content-length": NotRequired[str],
    },
)
NackHeaders = TypedDict(
    "NackHeaders",
    {
        "subscription": str,
        "id": str,
        "transaction": NotRequired[str],
        "content-length": NotRequired[str],
    },
)
BeginHeaders = TypedDict(
    "BeginHeaders",
    {
        "transaction": str,
        "content-length": NotRequired[str],
    },
)
CommitHeaders = TypedDict(
    "CommitHeaders",
    {
        "transaction": str,
        "content-length": NotRequired[str],
    },
)
AbortHeaders = TypedDict(
    "AbortHeaders",
    {
        "transaction": str,
        "content-length": NotRequired[str],
    },
)
DisconnectHeaders = TypedDict(
    "DisconnectHeaders",
    {
        "receipt": NotRequired[str],
        "content-length": NotRequired[str],
    },
)
ReceiptHeaders = TypedDict(
    "ReceiptHeaders",
    {
        "receipt-id": str,
        "content-length": NotRequired[str],
    },
)
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
ErrorHeaders = TypedDict(
    "ErrorHeaders",
    {
        "message": str,
        "content-length": NotRequired[str],
        "content-type": NotRequired[str],
    },
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
