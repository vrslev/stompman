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
    b"n": b"\n"[0],
    b"c": b":"[0],
    b"\\": b"\\"[0],
    b"r": b"\r"[0],
}
EOF_MARKER = b"\x00"
HEARTBEAT_MARKER = b"\n"
CRLFCRLR_MARKER = [b"\r", b"\n", b"\r", b"\n"]


def escape_header_value(header: str) -> str:
    return "".join(ESCAPE_CHARS.get(char, char) for char in header)


def iter_bytes(value: bytes) -> Iterator[bytes]:
    yield from cast(tuple[bytes, ...], struct.unpack(f"{len(value)!s}c", value))


def unescape_header(header: bytes) -> bytes:
    def unescape() -> Iterator[int]:
        previous_byte = None

        for byte in iter_bytes(header):
            if previous_byte == b"\\":
                yield REVERSE_ESCAPE_CHARS.get(byte, byte[0])
            elif byte != b"\\":
                yield byte[0]

            previous_byte = byte

    return bytes(unescape())


def separate_complete_and_incomplete_packet_parts(raw_frames: bytes) -> tuple[bytes, bytes]:
    if raw_frames.endswith(EOF_MARKER) or raw_frames.replace(b"\n", b"") == b"":
        return (raw_frames, b"")
    parts = raw_frames.rpartition(EOF_MARKER)
    return parts[0] + parts[1], parts[2]


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


def parse_command(raw_frame: deque[bytes]) -> str:
    def parse() -> Iterator[bytes]:
        while raw_frame and (byte := raw_frame.popleft()) != b"\n":
            yield byte

    return b"".join(parse()).decode()


def parse_headers(raw_frame: deque[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}
    one_header_buffer: list[bytes] = []
    three_previous_bytes: tuple[typing.Any, typing.Any, typing.Any] = (None, None, None)

    while True:
        byte = raw_frame.popleft()
        if byte == b"\n":
            if three_previous_bytes[-1] == b"\n":
                should_stop = True
            elif [three_previous_bytes, byte] == CRLFCRLR_MARKER:
                should_stop = True
                one_header_buffer.pop()
                one_header_buffer.pop()
                one_header_buffer.pop()
            else:
                should_stop = False

            if one_header_buffer:
                key, value = b"".join(one_header_buffer).split(b":", 1)
                if (decoded_key := key.decode()) not in headers:
                    headers[decoded_key] = unescape_header(value).decode()
                one_header_buffer.clear()

            if should_stop:
                break
        else:
            one_header_buffer.append(byte)

        three_previous_bytes = (three_previous_bytes[1], three_previous_bytes[2], byte)
    return headers


def parse_body(raw_frame: deque[bytes]) -> bytes:
    buf: list[bytes] = []
    while True:
        byte = raw_frame.popleft()
        if byte == EOF_MARKER:
            return b"".join(buf)
        buf.append(byte)


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    raw_frames_deque = deque(iter_bytes(raw_frames))

    while raw_frames_deque:
        first_byte = raw_frames_deque.popleft()
        if first_byte == b"\n":
            yield HeartbeatFrame(headers={})
        else:
            raw_frames_deque.appendleft(first_byte)
            command = parse_command(raw_frames_deque)
            if not raw_frames_deque:
                return
            headers = parse_headers(raw_frames_deque)
            if not raw_frames_deque:
                return
            body = parse_body(raw_frames_deque)
            if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
                yield known_frame_type(headers=cast(Any, headers), body=body)
            else:
                yield UnknownFrame(command=command, headers=headers, body=body)
