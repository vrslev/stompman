import itertools
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
REVERSE_ESCAPE_CHARS = {
    b"n": b"\n"[0],
    b"c": b":"[0],
    b"\\": b"\\"[0],
    b"r": b"\r"[0],
}
EOF_MARKER = b"\x00"
HEARTBEAT_MARKER = b"\n"
CRLFCRLR_MARKER = (b"\r", b"\n", b"\r", b"\n")


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


def parse_one_line(buffer: deque[bytes]) -> bytes:
    def parse() -> Iterator[int]:
        while (byte := buffer.popleft()) != b"\n":
            yield byte[0]

    return bytes(parse())


def ends_with_crlf(buffer: deque[bytes]) -> bool:
    size = len(buffer)
    four_last_bytes = tuple(itertools.islice(buffer, max(0, size - 4), size))
    return four_last_bytes == CRLFCRLR_MARKER


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


def _build_frame_from_buffer(buffer: deque[bytes]) -> AnyFrame:
    command = parse_one_line(buffer).decode()
    headers: Any = {}

    while len(line := parse_one_line(buffer)) > 1:
        key, value = line.split(b":", 1)
        decoded_key = key.decode()

        if decoded_key not in headers:
            headers[decoded_key] = unescape_header(value).decode()

    body = b"".join(buffer)
    if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
        return known_frame_type(headers=headers, body=body)
    return UnknownFrame(command=command, headers=headers, body=body)


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    buffer = deque[bytes]()
    previous_byte = None
    has_processed_headers = False

    for byte in iter_bytes(raw_frames):
        # parse body until EOF is reached
        if has_processed_headers:
            if byte == EOF_MARKER:
                yield _build_frame_from_buffer(buffer)
                buffer.clear()
                has_processed_headers = False
            else:
                buffer.append(byte)
        # heartbeat sent
        elif byte == HEARTBEAT_MARKER and not buffer:
            yield HeartbeatFrame(headers={})
            continue
        # parse command and headers
        else:
            buffer.append(byte)

            # command and headers parsed, time to start parsing body
            if byte == b"\n" and (previous_byte == b"\n" or ends_with_crlf(buffer)):
                has_processed_headers = True

        previous_byte = byte
