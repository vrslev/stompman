import struct
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
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
    if parts[2].replace(b"\n", b"") == b"":
        return (raw_frames, b"")

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
            continue

        headers = parse_headers(iterate_buffer())
        if not buffer:
            continue

        body = parse_body(iterate_buffer())
        if body is None:
            continue

        if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
            yield known_frame_type(headers=cast(Any, headers), body=body)
        else:
            yield UnknownFrame(command=command, headers=headers, body=body)


def unescape_byte(byte: bytes, previous_byte: bytes | None) -> bytes | None:
    if byte == b"\\":
        return None

    if previous_byte == b"\\":
        if unescaped_byte := UNESCAPE_CHARS.get(byte):
            return unescaped_byte
        return None

    return byte


def new_parse_headers(buffer: list[bytes]) -> tuple[str, str] | None:
    key_buffer: list[bytes] = []
    key_parsed = False
    value_buffer: list[bytes] = []

    previous_byte = None
    for byte in buffer:
        if byte == b":":
            if key_parsed:
                return None
            key_parsed = True

        elif (unescaped_byte := unescape_byte(byte, previous_byte)) is not None:
            (value_buffer if key_parsed else key_buffer).append(unescaped_byte)

        previous_byte = byte

    return (b"".join(key_buffer).decode(), b"".join(value_buffer).decode()) if key_parsed else None


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    buffer = deque(struct.unpack(f"{len(raw_frames)!s}c", raw_frames))
    lines = deque[list[bytes]]()
    one_line_buffer: list[bytes] = []
    previous_byte = None
    headers_processed = False

    while buffer:
        byte = buffer.popleft()

        if headers_processed:
            if byte == NULL:
                lines.append(one_line_buffer.copy())
                command = b"".join(lines.popleft()).decode()
                headers = {}
                while line := lines.popleft():
                    header = new_parse_headers(line)
                    if header and header[0] not in headers:
                        headers[header[0]] = header[1]
                body = b"".join(lines.popleft()) if lines else b""
                if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
                    yield known_frame_type(headers=cast(Any, headers), body=body)
                else:
                    yield UnknownFrame(command=command, headers=headers, body=body)

                headers_processed = False
                lines.clear()
                one_line_buffer.clear()
            elif byte != b"\n":
                one_line_buffer.append(byte)
        elif byte == b"\n":
            if not (one_line_buffer or lines):
                yield HeartbeatFrame(headers={})
            else:
                if not one_line_buffer:
                    headers_processed = True

                if previous_byte == b"\r":
                    one_line_buffer.pop()
                    lines.append(one_line_buffer.copy())
                else:
                    lines.append(one_line_buffer.copy())

                one_line_buffer.clear()
        else:
            one_line_buffer.append(byte)

        previous_byte = byte


@dataclass
class Parser:
    _remainder_from_last_packet: bytes = field(default=b"", init=False)

    def load_frames(self, raw_frames: bytes) -> Iterator[AnyFrame]:
        complete_bytes, incomplete_bytes = separate_complete_and_incomplete_packet_parts(
            self._remainder_from_last_packet + raw_frames
        )
        yield from load_frames(complete_bytes)
        self._remainder_from_last_packet = incomplete_bytes
