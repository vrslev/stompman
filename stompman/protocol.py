import struct
import typing
from collections import deque
from collections.abc import Iterator
from typing import Any, cast

from stompman.frames import (
    COMMANDS_TO_FRAME_TYPES,
    AnyFrame,
    BaseFrame,
    HeartbeatFrame,
    UnknownFrame,
)

PROTOCOL_VERSION = "1.1"  # https://stomp.github.io/stomp-specification-1.1.html
ESCAPE_CHARS = {
    "\n": "\\n",
    ":": "\\c",
    "\\": "\\\\",
    "\r": "\\r",
}
REVERSE_ESCAPE_CHARS = {
    b"n": b"\n",
    b"c": b":",
    b"\\": b"\\",
    b"r": b"\r",
}
EOF_MARKER = b"\x00"
HEARTBEAT_MARKER = b"\n"


NULL = b"\x00"
NEWLINE = b"\n"
CARRIAGE = b"\r"
CARRIAGE_NEWLINE_CARRIAGE_NEWLINE = (b"\r", b"\n", b"\r", b"\n")


def escape_header_value(header: str) -> str:
    return "".join(ESCAPE_CHARS.get(char, char) for char in header)


def dump_frame(frame: BaseFrame[Any]) -> bytes:
    lines = (
        frame.command.encode(),
        b"\n",
        *(f"{key}:{escape_header_value(value)}\n".encode() for key, value in sorted(frame.headers.items())),
        b"\n",
        frame.body,
        EOF_MARKER,
    )
    return b"".join(lines)


def separate_complete_and_incomplete_packet_parts(raw_frames: bytes) -> tuple[bytes, bytes]:
    if raw_frames.endswith(EOF_MARKER) or raw_frames.replace(b"\n", b"") == b"":
        return (raw_frames, b"")
    parts = raw_frames.rpartition(EOF_MARKER)
    return parts[0] + parts[1], parts[2]


def parse_command(raw_frame: deque[bytes]) -> str:
    def parse() -> Iterator[bytes]:
        previous_byte = None

        while True:
            byte = raw_frame.popleft()

            if byte == NEWLINE:
                if previous_byte and previous_byte != CARRIAGE:
                    yield previous_byte
                break

            if previous_byte:
                yield previous_byte
            previous_byte = byte

    return b"".join(parse()).decode()


def unescape_header(header_buffer: list[bytes]) -> bytes:
    def unescape() -> Iterator[bytes]:
        previous_byte = None

        for byte in header_buffer:
            if previous_byte == b"\\":
                yield REVERSE_ESCAPE_CHARS.get(byte, byte)
            elif byte != b"\\":
                yield byte

            previous_byte = byte

    return b"".join(unescape())


def parse_headers(raw_frame: deque[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    last_four_bytes: tuple[typing.Any, typing.Any, typing.Any, typing.Any] = (None, None, None, None)
    key_buffer: list[bytes] = []
    key_parsed = False
    value_buffer: list[bytes] = []

    while True:
        byte = raw_frame.popleft()
        last_four_bytes = (last_four_bytes[1], last_four_bytes[2], last_four_bytes[3], byte)

        if byte == CARRIAGE:
            key_buffer.clear()
            key_parsed = False
            value_buffer.clear()
        elif byte == NEWLINE:
            if key_parsed and (key := b"".join(key_buffer).decode()) and key not in headers:
                headers[key] = unescape_header(value_buffer).decode()

            key_buffer.clear()
            key_parsed = False
            value_buffer.clear()

            if last_four_bytes[-2] == NEWLINE or last_four_bytes == CARRIAGE_NEWLINE_CARRIAGE_NEWLINE:
                return headers
        elif key_parsed:
            value_buffer.append(byte)
        elif byte == b":":
            key_parsed = True
        else:
            key_buffer.append(byte)


def parse_body(raw_frame: deque[bytes]) -> bytes:
    buf: list[bytes] = []
    while True:
        byte = raw_frame.popleft()
        if byte == EOF_MARKER:
            return b"".join(buf)
        buf.append(byte)


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    buffer = deque(struct.unpack(f"{len(raw_frames)!s}c", raw_frames))

    while buffer:
        if (first_byte := buffer.popleft()) == NEWLINE:
            yield HeartbeatFrame(headers={})
            continue
        buffer.appendleft(first_byte)

        try:
            command = parse_command(buffer)
        except IndexError:
            return
        if not buffer:
            return

        try:
            headers = parse_headers(buffer)
        except IndexError:
            return
        if not buffer:
            return

        try:
            body = parse_body(buffer)
        except IndexError:
            return

        if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
            yield known_frame_type(headers=cast(Any, headers), body=body)
        else:
            yield UnknownFrame(command=command, headers=headers, body=body)
