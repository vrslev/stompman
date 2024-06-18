import pytest
from hypothesis import given, strategies

from stompman import (
    ConnectedFrame,
    ConnectFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
)
from stompman.frames import AckFrame, AnyClientFrame, AnyServerFrame
from stompman.serde import COMMANDS_TO_FRAMES, FrameParser, dump_frame, make_frame_from_parts


@pytest.mark.parametrize(
    ("frame", "dumped_frame"),
    [
        (AckFrame(headers={"subscription": "1", "id": "1"}), (b"ACK\nid:1\nsubscription:1\n\n\x00")),
        (ConnectedFrame(headers={"version": "1.1"}), (b"CONNECTED\nversion:1.1\n\n\x00")),
        (
            MessageFrame(
                headers={"destination": "me:123", "message-id": "you\nmore\rextra\\here", "subscription": "hi"},
                body=b"I Am The Walrus",
            ),
            (
                b"MESSAGE\n"
                b"destination:me\\c123\n"
                b"message-id:you\\nmore\\rextra\\\\here\nsubscription:hi\n\n"
                b"I Am The Walrus"
                b"\x00"
            ),
        ),
    ],
)
def test_dump_frame(frame: AnyClientFrame, dumped_frame: bytes) -> None:
    assert dump_frame(frame) == dumped_frame


@pytest.mark.parametrize(
    ("raw_frames", "loaded_frames"),
    [
        # Partial packet
        (
            b"CONNECT\naccept-version:1.0\n\n\x00",
            [ConnectFrame(headers={"accept-version": "1.0"})],
        ),
        # Full packet
        (
            b"MESSAGE\naccept-version:1.0\n\nHey dude\x00",
            [MessageFrame(headers={"accept-version": "1.0"}, body=b"Hey dude")],
        ),
        # Long packet
        (
            (
                b"MESSAGE\n"
                b"content-length:14\nexpires:0\ndestination:/topic/"
                b"xxxxxxxxxxxxxxxxxxxxxxxxxl"
                b"\nsubscription:1\npriority:4\nActiveMQ.MQTT.QoS:1\nmessage-id"
                b":ID\\cxxxxxx-35207-1543430467768-204"
                b"\\c363\\c-1\\c1\\c463859\npersistent:true\ntimestamp"
                b":1548945234003\n\n222.222.22.222"
                b"\x00\nMESSAGE\ncontent-length:12\nexpires:0\ndestination:"
                b"/topic/xxxxxxxxxxxxxxxxxxxxxxxxxx"
                b"\nsubscription:1\npriority:4\nActiveMQ.MQTT.QoS:1\nmessage-id"
                b":ID\\cxxxxxx-35207-1543430467768-204"
                b"\\c363\\c-1\\c1\\c463860\npersistent:true\ntimestamp"
                b":1548945234005\n\n88.88.888.88"
                b"\x00\nMESSAGE\ncontent-length:11\nexpires:0\ndestination:"
                b"/topic/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                b"\nsubscription:1\npriority:4\nActiveMQ.MQTT.QoS:1\nmessage-id"
                b":ID\\cxxxxxx-35207-1543430467768-204"
                b"\\c362\\c-1\\c1\\c290793\npersistent:true\ntimestamp"
                b":1548945234005\n\n111.11.1.11"
                b"\x00\nMESSAGE\ncontent-length:14\nexpires:0\ndestination:"
                b"/topic/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                b"\nsubscription:1\npriority:4\nActiveMQ.MQTT.QoS:1\nmessage-id"
                b":ID\\cxxxxxx-35207-1543430467768-204"
                b"\\c362\\c-1\\c1\\c290794\npersistent:true\ntimestamp:"
                b"1548945234005\n\n222.222.22.222"
                b"\x00\nMESSAGE\ncontent-length:12\nexpires:0\ndestination:"
                b"/topic/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                b"\nsubscription:1\npriority:4\nActiveMQ.MQTT.QoS:1\nmessage-id"
                b":ID\\cxxxxxx-35207-1543430467768-204"
                b"\\c362\\c-1\\c1\\c290795\npersistent:true\ntimestamp:"
                b"1548945234005\n\n88.88.888.88\x00\nMESS"
            ),
            [
                MessageFrame(
                    headers={
                        "content-length": "14",
                        "expires": "0",
                        "destination": "/topic/xxxxxxxxxxxxxxxxxxxxxxxxxl",
                        "subscription": "1",
                        "priority": "4",
                        "ActiveMQ.MQTT.QoS": "1",
                        "message-id": "ID:xxxxxx-35207-1543430467768-204:363:-1:1:463859",
                        "persistent": "true",
                        "timestamp": "1548945234003",
                    },
                    body=b"222.222.22.222",
                ),
                HeartbeatFrame(),
                MessageFrame(
                    headers={
                        "content-length": "12",
                        "expires": "0",
                        "destination": "/topic/xxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "subscription": "1",
                        "priority": "4",
                        "ActiveMQ.MQTT.QoS": "1",
                        "message-id": "ID:xxxxxx-35207-1543430467768-204:363:-1:1:463860",
                        "persistent": "true",
                        "timestamp": "1548945234005",
                    },
                    body=b"88.88.888.88",
                ),
                HeartbeatFrame(),
                MessageFrame(
                    headers={
                        "content-length": "11",
                        "expires": "0",
                        "destination": "/topic/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "subscription": "1",
                        "priority": "4",
                        "ActiveMQ.MQTT.QoS": "1",
                        "message-id": "ID:xxxxxx-35207-1543430467768-204:362:-1:1:290793",
                        "persistent": "true",
                        "timestamp": "1548945234005",
                    },
                    body=b"111.11.1.11",
                ),
                HeartbeatFrame(),
                MessageFrame(
                    headers={
                        "content-length": "14",
                        "expires": "0",
                        "destination": "/topic/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "subscription": "1",
                        "priority": "4",
                        "ActiveMQ.MQTT.QoS": "1",
                        "message-id": "ID:xxxxxx-35207-1543430467768-204:362:-1:1:290794",
                        "persistent": "true",
                        "timestamp": "1548945234005",
                    },
                    body=b"222.222.22.222",
                ),
                HeartbeatFrame(),
                MessageFrame(
                    headers={
                        "content-length": "12",
                        "expires": "0",
                        "destination": "/topic/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                        "subscription": "1",
                        "priority": "4",
                        "ActiveMQ.MQTT.QoS": "1",
                        "message-id": "ID:xxxxxx-35207-1543430467768-204:362:-1:1:290795",
                        "persistent": "true",
                        "timestamp": "1548945234005",
                    },
                    body=b"88.88.888.88",
                ),
                HeartbeatFrame(),
            ],
        ),
        # Partial packet #2
        (
            b"CONNECT\naccept-version:1.0\n\n\x00\nCONNECTED\nversion:1.0\n\n\x00\n",
            [
                ConnectFrame(headers={"accept-version": "1.0"}),
                HeartbeatFrame(),
                ConnectedFrame(headers={"version": "1.0"}),
                HeartbeatFrame(),
            ],
        ),
        # Utf-8
        (
            b"CONNECTED\naccept-version:1.0\n\n\x00\nERROR\nheader:1.0\n\n\xc3\xa7\x00\n",
            [
                ConnectedFrame(headers={"accept-version": "1.0"}),
                HeartbeatFrame(),
                ErrorFrame(headers={"header": "1.0"}, body="ç".encode()),
                HeartbeatFrame(),
            ],
        ),
        (b"\n", [HeartbeatFrame()]),
        # Two headers: only first should be accepted
        (
            b"CONNECTED\naccept-version:1.0\naccept-version:1.1\n\n\x00",
            [ConnectedFrame(headers={"accept-version": "1.0"})],
        ),
        # no end of line after command
        (b"CONNECTED", []),
        (b"CONNECTED\n", []),
        (b"CONNECTED\x00", []),
        # \r\n after command
        (b"CONNECTED\r\n\n\n\x00", [ConnectedFrame(headers={})]),
        (b"CONNECTED\r\nheader:1.0\n\n\x00", [ConnectedFrame(headers={"header": "1.0"})]),
        # header without :
        (b"CONNECTED\nhead\nheader:1.1\n\n\x00", [ConnectedFrame(headers={"header": "1.1"})]),
        # empty header :
        (
            b"CONNECTED\nhead:\nheader:1.1\n\n\x00",
            [ConnectedFrame(headers={"head": "", "header": "1.1"})],
        ),
        # header value with :
        (b"CONNECTED\nheader:what:?\n\n\x00", [ConnectedFrame(headers={})]),
        # no NULL
        (b"CONNECTED\nheader:what:?\n\nhello", []),
        # header never end
        (b"CONNECTED\nheader:hello", []),
        (b"CONNECTED\nheader:hello\n", []),
        (b"CONNECTED\nheader:hello\n\x00", []),
        (b"CONNECTED\nn", []),
        # unknown command
        (b"SOME_COMMAND\nhead:\nheader:1.1\n\n\x00", [HeartbeatFrame()]),
        # unknown command
        (
            b"whatever\nWHATEVER\nheader:1.1\n\n\x00CONNECTED\nheader:1.1\n\n\x00\nwhatever\nCONNECTED\nheader:1.2\n\n\x00",
            [
                HeartbeatFrame(),
                ConnectedFrame(headers={"header": "1.1"}),
                HeartbeatFrame(),
                ConnectedFrame(headers={"header": "1.2"}),
            ],
        ),
    ],
)
def test_load_frames(raw_frames: bytes, loaded_frames: list[AnyServerFrame]) -> None:
    assert list(FrameParser().parse_frames_from_chunk(raw_frames)) == loaded_frames


def generate_frames(
    cases: list[tuple[bytes, list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]],
) -> tuple[list[bytes], list[AnyClientFrame | AnyServerFrame | HeartbeatFrame]]:
    all_bytes: list[bytes] = []
    all_frames: list[AnyClientFrame | AnyServerFrame | HeartbeatFrame] = []

    for intermediate_bytes, frames in cases:
        if intermediate_bytes:
            all_bytes.append(intermediate_bytes + b"\n")

        all_bytes.append(
            b"".join(b"\n" if isinstance(frame, HeartbeatFrame) else dump_frame(frame) for frame in frames)
        )
        all_frames += frames

    return all_bytes, all_frames


intermediate_bytes_strategy = (
    # TODO: Check if that's OK
    strategies.binary().filter(lambda bytes_: b"\n" not in bytes_).filter(lambda bytes_: b"\x00" not in bytes_)
)
frame_strategy = strategies.builds(
    make_frame_from_parts,
    command=strategies.sampled_from(tuple(COMMANDS_TO_FRAMES.keys())),
    headers=strategies.dictionaries(
        # TODO: Fix \\ and \x00
        strategies.text().filter(lambda key: "\x00" not in key).filter(lambda key: "\\" not in key),
        strategies.text().filter(lambda value: "\x00" not in value).filter(lambda value: "\\" not in value),
    ),
    body=strategies.binary().filter(lambda body: b"\x00" not in body),
) | strategies.just(HeartbeatFrame())


@given(
    strategies.builds(
        generate_frames,
        strategies.lists(strategies.tuples(intermediate_bytes_strategy, strategies.lists(frame_strategy))),
    ),
)
def test_props(case: tuple[list[bytes], list[AnyServerFrame | AnyClientFrame]]) -> None:
    stream_chunks, expected_frames = case
    parser = FrameParser()

    parsed_frames: list[AnyClientFrame | AnyServerFrame | HeartbeatFrame] = []
    for chunk in stream_chunks:
        parsed_frames.extend(parser.parse_frames_from_chunk(chunk))

    assert parsed_frames == expected_frames
