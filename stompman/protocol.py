import struct
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


def _build_frame_from_buffer(
    command_buffer: list[bytes], headers: dict[str, str], body_buffer: list[bytes]
) -> AnyFrame:
    command = b"".join(command_buffer).decode()
    body = b"".join(body_buffer)
    if known_frame_type := COMMANDS_TO_FRAME_TYPES.get(command):
        return known_frame_type(headers=cast(Any, headers), body=body)
    return UnknownFrame(command=command, headers=headers, body=body)


def load_frames(raw_frames: bytes) -> Iterator[AnyFrame]:
    command_buffer = list[bytes]()
    one_header_buffer = list[bytes]()
    headers: dict[str, str] = {}
    body_buffer = list[bytes]()
    previous_byte = None
    has_processed_command = False
    has_processed_headers = False

    for byte in iter_bytes(raw_frames):
        if (byte_is_newline := byte == b"\n") and not (command_buffer or one_header_buffer or headers):
            yield HeartbeatFrame(headers={})

        elif has_processed_headers:
            if byte == EOF_MARKER:
                yield _build_frame_from_buffer(command_buffer, headers, body_buffer)
                command_buffer.clear()
                body_buffer.clear()
                headers = {}
                has_processed_command = False
                has_processed_headers = False
            else:
                body_buffer.append(byte)
        elif has_processed_command:
            if byte_is_newline:
                if previous_byte == b"\n":
                    has_processed_headers = True
                elif [one_header_buffer[-4:], byte] == CRLFCRLR_MARKER:
                    has_processed_headers = True
                    one_header_buffer.pop()
                    one_header_buffer.pop()
                    one_header_buffer.pop()

                if one_header_buffer:
                    key, value = b"".join(one_header_buffer).split(b":", 1)
                    if (decoded_key := key.decode()) not in headers:
                        headers[decoded_key] = unescape_header(value).decode()
                    one_header_buffer.clear()
            else:
                one_header_buffer.append(byte)
        else:  # noqa: PLR5501
            if byte_is_newline:
                has_processed_command = True
            else:
                command_buffer.append(byte)

        previous_byte = byte
