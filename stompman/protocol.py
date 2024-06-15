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
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
    SendFrame,
)

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
FRAMES_WITH_BODY = (SendFrame, MessageFrame, ErrorFrame)


def iter_bytes(bytes_: bytes) -> tuple[bytes, ...]:
    return struct.unpack(f"{len(bytes_)!s}c", bytes_)


VALID_COMMANDS = [list(iter_bytes(command)) for command in COMMANDS_TO_FRAMES]


def dump_header(key: str, value: str) -> bytes:
    escaped_key = "".join(ESCAPE_CHARS.get(char, char) for char in key)
    escaped_value = "".join(ESCAPE_CHARS.get(char, char) for char in value)
    return f"{escaped_key}:{escaped_value}\n".encode()


def dump_frame(frame: AnyClientFrame | AnyServerFrame) -> bytes:
    frame_type = type(frame)
    lines = (
        FRAMES_TO_COMMANDS[frame_type],
        NEWLINE,
        *(dump_header(key, cast(str, value)) for key, value in sorted(frame.headers.items())),
        NEWLINE,
        frame.body if frame_type in FRAMES_WITH_BODY else b"",
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


def parse_lines_into_frame(lines: deque[list[bytes]]) -> AnyClientFrame | AnyServerFrame:
    command = b"".join(lines.popleft())
    headers = {}

    while line := lines.popleft():
        header = parse_headers(line)
        if header and header[0] not in headers:
            headers[header[0]] = header[1]
    body = b"".join(lines.popleft()) if lines else b""

    return COMMANDS_TO_FRAMES[command](headers=cast(Any, headers), body=body)


@dataclass
class FrameParser:
    _lines: deque[list[bytes]] = field(default_factory=deque, init=False)
    _current_line: list[bytes] = field(default_factory=list, init=False)
    _previous_byte: bytes = field(default=b"", init=False)
    _headers_processed: bool = field(default=False, init=False)

    def _reset(self) -> None:
        self._headers_processed = False
        self._lines.clear()
        self._current_line = []

    def parse_frames_from_chunk(self, chunk: bytes) -> Iterator[AnyClientFrame | AnyServerFrame | HeartbeatFrame]:
        buffer = deque(iter_bytes(chunk))
        while buffer:
            byte = buffer.popleft()

            if byte == NULL:
                if self._headers_processed:
                    self._lines.append(self._current_line)
                    yield parse_lines_into_frame(self._lines)
                self._reset()

            elif not self._headers_processed and byte == NEWLINE:
                if self._current_line or self._lines:
                    if self._previous_byte == b"\r":
                        self._current_line.pop()
                    self._headers_processed = not self._current_line  # extra empty line after headers

                    if not self._lines and self._current_line not in VALID_COMMANDS:
                        self._reset()
                    else:
                        self._lines.append(self._current_line)
                        self._current_line = []
                else:
                    yield HeartbeatFrame()

            else:
                self._current_line.append(byte)

            self._previous_byte = byte
