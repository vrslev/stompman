import struct
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, cast

from stompman.frames import (
    COMMANDS_TO_FRAMES,
    FRAMES_TO_COMMANDS,
    AnyFrame,
    AnyRealFrame,
    HeartbeatFrame,
)

PROTOCOL_VERSION = "1.2"  # https://stomp.github.io/stomp-specification-1.2.html
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
CARRIAGE = b"\r"
CARRIAGE_NEWLINE_CARRIAGE_NEWLINE = (CARRIAGE, NEWLINE, CARRIAGE, NEWLINE)


def dump_header(key: str, value: str) -> bytes:
    escaped_key = "".join(ESCAPE_CHARS.get(char, char) for char in key)
    escaped_value = "".join(ESCAPE_CHARS.get(char, char) for char in value)
    return f"{escaped_key}:{escaped_value}\n".encode()


def dump_frame(frame: AnyRealFrame) -> bytes:
    lines = (
        FRAMES_TO_COMMANDS[type(frame)],
        NEWLINE,
        *(dump_header(key, cast(str, value)) for key, value in sorted(frame.headers.items())),
        NEWLINE,
        frame.body,
        NULL,
    )
    return b"".join(lines)


def separate_complete_and_incomplete_packet_parts(raw_frames: bytes) -> tuple[bytes, bytes]:
    if raw_frames.replace(NEWLINE, b"") == b"":
        return (raw_frames, b"")
    parts = raw_frames.rpartition(NULL)
    if parts[2].replace(NEWLINE, b"") == b"":
        return (raw_frames, b"")

    return parts[0] + parts[1], parts[2]


def unescape_byte(byte: bytes, previous_byte: bytes | None) -> bytes | None:
    if byte == b"\\":
        return None

    if previous_byte == b"\\":
        return UNESCAPE_CHARS.get(byte, byte)

    return byte


def parse_headers(buffer: list[bytes]) -> tuple[str, str] | None:
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


def parse_lines_into_frame(lines: deque[list[bytes]]) -> AnyFrame | None:
    command = b"".join(lines.popleft())
    headers = {}

    while line := lines.popleft():
        header = parse_headers(line)
        if header and header[0] not in headers:
            headers[header[0]] = header[1]
    body = b"".join(lines.popleft()) if lines else b""

    if known_frame_type := COMMANDS_TO_FRAMES.get(command):
        return known_frame_type(headers=cast(Any, headers), body=body)
    return None


@dataclass
class Parser:
    _remainder_from_last_packet: bytes = field(default=b"", init=False)

    def load_frames(self, raw_frames: bytes) -> Iterator[AnyFrame]:
        all_bytes = self._remainder_from_last_packet + raw_frames
        buffer = deque(struct.unpack(f"{len(all_bytes)!s}c", all_bytes))
        lines = deque[list[bytes]]()
        current_line: list[bytes] = []
        previous_byte = None
        headers_processed = False

        while buffer:
            byte = buffer.popleft()

            if headers_processed and byte == NULL:
                lines.append(current_line)
                if parsed_frame := parse_lines_into_frame(lines):
                    yield parsed_frame
                headers_processed = False
                lines.clear()
                current_line = []

            elif not headers_processed and byte == NEWLINE:
                if current_line or lines:
                    if not current_line:  # extra empty line after headers
                        headers_processed = True

                    if previous_byte == b"\r":
                        current_line.pop()

                    lines.append(current_line)
                    current_line = []
                else:
                    yield HeartbeatFrame()

            else:
                current_line.append(byte)

            previous_byte = byte

        self._remainder_from_last_packet = b"".join(current_line)
