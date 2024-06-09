import struct
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
UNESCAPE_CHARS = {
    b"n": b"\n",
    b"c": b":",
    b"\\": b"\\",
    b"r": b"\r",
}
NULL = b"\x00"
NEWLINE = b"\n"
HEADER_SEPARATOR = b":"
CARRIAGE = b"\r"
CARRIAGE_NEWLINE_CARRIAGE_NEWLINE = (b"\r", b"\n", b"\r", b"\n")


def dump_header(key: str, value: str) -> bytes:
    escaped_key = "".join(ESCAPE_CHARS.get(char, char) for char in key)
    escaped_value = "".join(ESCAPE_CHARS.get(char, char) for char in value)
    return f"{escaped_key}:{escaped_value}\n".encode()


def dump_frame(frame: BaseFrame[Any]) -> bytes:
    lines = (
        frame.command.encode(),
        NEWLINE,
        *(dump_header(key, value) for key, value in sorted(frame.headers.items())),
        NEWLINE,
        frame.body,
        NULL,
    )
    return b"".join(lines)


def separate_complete_and_incomplete_packet_parts(raw_frames: bytes) -> tuple[bytes, bytes]:
    if raw_frames.replace(b"\n", b"") == b"":
        return (raw_frames, b"")
    parts = raw_frames.rpartition(NULL)
    return parts[0] + parts[1], parts[2]


def parse_command(buffer: Iterator[bytes]) -> str:
    def parse() -> Iterator[bytes]:
        previous_byte = None

        for byte in buffer:
            if byte == NEWLINE:
                if previous_byte and previous_byte != CARRIAGE:
                    yield previous_byte
                break

            if previous_byte:
                yield previous_byte
            previous_byte = byte

    return b"".join(parse()).decode()


def unescape_header_value(header_buffer: list[bytes]) -> str:
    def unescape() -> Iterator[bytes]:
        previous_byte = None

        for byte in header_buffer:
            if previous_byte == b"\\":
                yield UNESCAPE_CHARS.get(byte, byte)
            elif byte != b"\\":
                yield byte

            previous_byte = byte

    return b"".join(unescape()).decode()


def parse_headers(buffer: Iterator[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    last_four_bytes: tuple[bytes | None, bytes | None, bytes | None, bytes | None] = (None, None, None, None)
    key_buffer: list[bytes] = []
    key_parsed = False
    value_buffer: list[bytes] = []

    def reset() -> None:
        key_buffer.clear()
        nonlocal key_parsed
        key_parsed = False
        value_buffer.clear()

    for byte in buffer:
        last_four_bytes = (last_four_bytes[1], last_four_bytes[2], last_four_bytes[3], byte)

        if byte == CARRIAGE:
            reset()
        elif byte == NEWLINE:
            if key_parsed and (key := unescape_header_value(key_buffer)) and key not in headers:
                headers[key] = unescape_header_value(value_buffer)
            reset()

            if last_four_bytes[-2] == NEWLINE or last_four_bytes == CARRIAGE_NEWLINE_CARRIAGE_NEWLINE:
                break
        elif byte == b":":
            if key_parsed:
                reset()
            else:
                key_parsed = True
        elif key_parsed:
            value_buffer.append(byte)
        else:
            key_buffer.append(byte)
    return headers


def parse_body(raw_frame: Iterator[bytes]) -> bytes | None:
    received_null = False

    def parse() -> Iterator[bytes]:
        for byte in raw_frame:
            if byte == NULL:
                nonlocal received_null
                received_null = True
                break
            yield byte

    result = b"".join(parse())
    return result if received_null else None


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    buffer = deque(struct.unpack(f"{len(raw_frames)!s}c", raw_frames))
    remainder = []

    def iterate_buffer() -> Iterator[bytes]:
        buf = []
        while buffer:
            byte = buffer.popleft()
            buf.append(byte)
            yield byte
        nonlocal remainder
        remainder = buf

    while buffer:
        if (first_byte := buffer.popleft()) == NEWLINE:
            yield HeartbeatFrame(headers={})
            continue
        buffer.appendleft(first_byte)

        command = parse_command(iterate_buffer())
        if not buffer:
            return

        headers = parse_headers(iterate_buffer())
        if not buffer:
            return

        body = parse_body(iterate_buffer())
        if body is None:
            return

        if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
            yield known_frame_type(headers=cast(Any, headers), body=body)
        else:
            yield UnknownFrame(command=command, headers=headers, body=body)
