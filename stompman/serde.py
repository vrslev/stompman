import struct
from collections import deque
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Final, cast

from stompman.frames import (
    AbortFrame,
    AckFrame,
    AnyClientFrame,
    AnyServerFrame,
    BeginFrame,
    CommitFrame,
    ConnectedFrame,
    ConnectFrame,
    DisconnectFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    NackFrame,
    ReceiptFrame,
    SendFrame,
    StompFrame,
    SubscribeFrame,
    UnsubscribeFrame,
)

NEWLINE: Final = b"\n"
CARRIAGE: Final = b"\r"
NULL: Final = b"\x00"
BACKSLASH = b"\\"
COLON_ = b":"

HEADER_ESCAPE_CHARS: Final = {
    b"\n": b"\\n",
    b":": b"\\c",
    b"\\": b"\\\\",
    b"\r": b"",  # [\r]\n is newline, therefore can't be used in header
}
HEADER_UNESCAPE_CHARS: Final = {
    b"n": NEWLINE,
    b"c": b":",
    b"\\": b"\\",
}


def iter_bytes(bytes_: bytes) -> Iterator[bytes]:
    return (tup[0] for tup in struct.iter_unpack(f"{len(bytes_)!s}c", bytes_))


COMMANDS_TO_FRAMES: Final[dict[bytes, type[AnyClientFrame | AnyServerFrame]]] = {
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
FRAMES_TO_COMMANDS: Final = {value: key for key, value in COMMANDS_TO_FRAMES.items()}
FRAMES_WITH_BODY: Final = (SendFrame, MessageFrame, ErrorFrame)


def dump_header(key: str, value: str) -> Iterator[bytes]:
    for char in iter_bytes(key.encode()):
        yield HEADER_ESCAPE_CHARS.get(char, char)
    yield b":"
    for char in iter_bytes(value.encode()):
        yield HEADER_ESCAPE_CHARS.get(char, char)
    yield NEWLINE


def _dump_frame_iter(frame: AnyClientFrame | AnyServerFrame) -> Iterator[bytes]:
    yield FRAMES_TO_COMMANDS[type(frame)]
    yield NEWLINE
    for key, value in sorted(frame.headers.items()):
        yield from dump_header(key, cast(str, value))
    yield NEWLINE
    if isinstance(frame, FRAMES_WITH_BODY):
        yield frame.body
    yield NULL


def dump_frame(frame: AnyClientFrame | AnyServerFrame) -> bytes:
    return b"".join(_dump_frame_iter(frame))


def unescape_byte(byte: bytes, previous_byte: bytes | None) -> bytes | None:
    if previous_byte == b"\\":
        return HEADER_UNESCAPE_CHARS.get(byte)
    if byte == b"\\":
        return None
    return byte


def parse_header(buffer: bytearray) -> tuple[str, str] | None:
    key_buffer = bytearray()
    value_buffer = bytearray()
    key_parsed = False

    previous_byte = None
    just_escaped_line = False

    for byte in iter_bytes(buffer):
        if byte == b":":
            if key_parsed:
                return None
            key_parsed = True
        elif just_escaped_line:
            just_escaped_line = False
            if byte != b"\\":
                (value_buffer if key_parsed else key_buffer).append(byte[0])
        elif unescaped_byte := unescape_byte(byte, previous_byte):
            just_escaped_line = True
            (value_buffer if key_parsed else key_buffer).append(unescaped_byte[0])

        previous_byte = byte

    if key_parsed:
        with suppress(UnicodeDecodeError):
            return key_buffer.decode(), value_buffer.decode()

    return None


def make_frame_from_parts(command: bytes, headers: dict[str, str], body: bytes) -> AnyClientFrame | AnyServerFrame:
    frame_type = COMMANDS_TO_FRAMES[command]
    return (
        frame_type(headers=cast(Any, headers), body=body)  # type: ignore[call-arg]
        if frame_type in FRAMES_WITH_BODY
        else frame_type(headers=cast(Any, headers))  # type: ignore[call-arg]
    )


def parse_lines_into_frame(lines: deque[bytearray]) -> AnyClientFrame | AnyServerFrame:
    command = lines.popleft()
    headers = {}

    while line := lines.popleft():
        header = parse_header(line)
        if header and header[0] not in headers:
            headers[header[0]] = header[1]
    body = lines.popleft() if lines else b""
    return make_frame_from_parts(command=command, headers=headers, body=body)


@dataclass
class FrameParser:
    _lines: deque[bytearray] = field(default_factory=deque, init=False)
    _current_line: bytearray = field(default_factory=bytearray, init=False)
    _previous_byte: bytes = field(default=b"", init=False)
    _headers_processed: bool = field(default=False, init=False)

    def _reset(self) -> None:
        self._headers_processed = False
        self._lines.clear()
        self._current_line = bytearray()

    def parse_frames_from_chunk(self, chunk: bytes) -> Iterator[AnyClientFrame | AnyServerFrame | HeartbeatFrame]:
        for byte in iter_bytes(chunk):
            if byte == NULL:
                if self._headers_processed:
                    self._lines.append(self._current_line)
                    yield parse_lines_into_frame(self._lines)
                self._reset()

            elif not self._headers_processed and byte == NEWLINE:
                if self._current_line or self._lines:
                    if self._previous_byte == CARRIAGE:
                        self._current_line.pop()
                    self._headers_processed = not self._current_line  # extra empty line after headers

                    if not self._lines and bytes(self._current_line) not in COMMANDS_TO_FRAMES:
                        self._reset()
                    else:
                        self._lines.append(self._current_line)
                        self._current_line = bytearray()
                else:
                    yield HeartbeatFrame()

            else:
                self._current_line.append(byte[0])

            self._previous_byte = byte
