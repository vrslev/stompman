import struct
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, cast

from stompman.frames import (
    COMMANDS_TO_FRAMES,
    FRAMES_TO_COMMANDS,
    AnyClientFrame,
    AnyServerFrame,
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


def dump_frame(frame: AnyClientFrame | AnyServerFrame) -> bytes:
    lines = (
        FRAMES_TO_COMMANDS[type(frame)],
        NEWLINE,
        *(dump_header(key, cast(str, value)) for key, value in sorted(frame.headers.items())),
        NEWLINE,
        frame.body,
        NULL,
    )
    return b"".join(lines)


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


def parse_lines_into_frame(lines: deque[list[bytes]]) -> AnyClientFrame | AnyServerFrame | None:
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
    _lines: deque[list[bytes]] = field(default_factory=deque, init=False)
    _current_line: list[bytes] = field(default_factory=list, init=False)
    _previous_byte: bytes = field(default=b"", init=False)
    _headers_processed: bool = field(default=False, init=False)

    def load_frames(self, raw_frames: bytes) -> Iterator[AnyClientFrame | AnyServerFrame | HeartbeatFrame]:
        buffer = deque(struct.unpack(f"{len(raw_frames)!s}c", raw_frames))
        while buffer:
            byte = buffer.popleft()

            if self._headers_processed and byte == NULL:
                self._lines.append(self._current_line)
                if parsed_frame := parse_lines_into_frame(self._lines):
                    yield parsed_frame
                self._headers_processed = False
                self._lines.clear()
                self._current_line = []

            elif not self._headers_processed and byte == NEWLINE:
                if self._current_line or self._lines:
                    if not self._current_line:  # extra empty line after headers
                        self._headers_processed = True

                    if self._previous_byte == b"\r":
                        self._current_line.pop()

                    self._lines.append(self._current_line)
                    self._current_line = []
                else:
                    yield HeartbeatFrame()

            else:
                self._current_line.append(byte)

            self._previous_byte = byte
