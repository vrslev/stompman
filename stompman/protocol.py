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
CRLFCRLR_MARKER = (b"\r", b"\n", b"\r", b"\n")


NULL = b"\x00"
NEWLINE = b"\n"
CARRIAGE = b"\r"


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


def separate_complete_and_incomplete_packet_parts(raw_frames: bytes) -> tuple[bytes, bytes]:
    if raw_frames.endswith(EOF_MARKER) or raw_frames.replace(b"\n", b"") == b"":
        return (raw_frames, b"")
    parts = raw_frames.rpartition(EOF_MARKER)
    return parts[0] + parts[1], parts[2]


def split_lines(buffer: deque[bytes]) -> Iterator[deque[bytes]]:
    def parse_one_line() -> Iterator[bytes]:
        previous_byte = None

        while buffer:
            byte = buffer.popleft()

            if byte == NEWLINE:
                if previous_byte and previous_byte != CARRIAGE:
                    yield previous_byte
                break

            if previous_byte:
                yield previous_byte
            previous_byte = byte

    while True:
        current_line = deque[bytes]()
        for byte in parse_one_line():
            current_line.append(byte)
        yield current_line


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
            continue

        if byte != NEWLINE:
            if key_parsed:
                value_buffer.append(byte)
            elif byte == b":":
                key_parsed = True
            else:
                key_buffer.append(byte)
        else:
            if (key := b"".join(key_buffer).decode()) and key not in headers:
                headers[key] = unescape_header(value_buffer).decode()
                key_buffer.clear()
                key_parsed = False
                value_buffer.clear()

            if (last_four_bytes[-1], last_four_bytes[-2]) == (NEWLINE, NEWLINE) or last_four_bytes == CRLFCRLR_MARKER:
                return headers


def parse_body(raw_frame: deque[bytes]) -> bytes:
    buf: list[bytes] = []
    while True:
        byte = raw_frame.popleft()
        if byte == EOF_MARKER:
            return b"".join(buf)
        buf.append(byte)


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    raw_frames_deque = deque(struct.unpack(f"{len(raw_frames)!s}c", raw_frames))

    while raw_frames_deque:
        first_byte = raw_frames_deque.popleft()
        if first_byte == b"\n":
            yield HeartbeatFrame(headers={})
        else:
            raw_frames_deque.appendleft(first_byte)

            try:
                command = parse_command(raw_frames_deque)
            except IndexError:
                return
            if not raw_frames_deque:
                return

            try:
                headers = parse_headers(raw_frames_deque)
            except IndexError:
                return
            if not raw_frames_deque:
                return

            try:
                body = parse_body(raw_frames_deque)
            except IndexError:
                return

            if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
                yield known_frame_type(headers=cast(Any, headers), body=body)
            else:
                yield UnknownFrame(command=command, headers=headers, body=body)
