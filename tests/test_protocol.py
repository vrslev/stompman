import pytest

from stompman import (
    ConnectedFrame,
    ConnectFrame,
    ErrorFrame,
    HeartbeatFrame,
    MessageFrame,
)
from stompman.frames import AckFrame, ClientFrame, ServerFrame
from stompman.protocol import Parser, dump_frame


@pytest.mark.parametrize(
    ("frame", "dumped_frame"),
    [
        (
            AckFrame(
                headers={"subscription": "1", "message-id": "1"},
                body=b"I Am The Walrus",
            ),
            (b"ACK\nmessage-id:1\nsubscription:1\n\nI Am The Walrus\x00"),
        ),
        (
            ConnectedFrame(headers={"heart-beat": "1,1", "version": "1"}),
            (b"CONNECTED\n" b"heart-beat:1,1\nversion:1\n\n" b"\x00"),
        ),
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
def test_dump_frame(frame: ClientFrame, dumped_frame: bytes) -> None:
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
            b"CONNECT\naccept-version:1.0\n\nHey dude\x00",
            [ConnectFrame(headers={"accept-version": "1.0"}, body=b"Hey dude")],
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
        (
            b"\n",
            [HeartbeatFrame()],
        ),
        # Two headers: only first should be accepted
        (
            b"CONNECTED\naccept-version:1.0\naccept-version:1.1\n\n\x00",
            [ConnectedFrame(headers={"accept-version": "1.0"})],
        ),
        # no end of line after command
        (b"SOME_COMMAND", []),
        (b"SOME_COMMAND\n", []),
        (b"SOME_COMMAND\x00", []),
        # \r\n after command
        (b"CONNECTED\r\n\n\n\x00", [ConnectedFrame(headers={}, body=b"\n")]),
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
        (b"SOME_COMMAND\nheader:what:?\n\nhello", []),
        # header never end
        (b"SOME_COMMAND\nheader:hello", []),
        (b"SOME_COMMAND\nheader:hello\n", []),
        (b"SOME_COMMAND\nheader:hello\n\x00", []),
        (b"SOME_COMMAND\nn", []),
    ],
)
def test_load_frames(raw_frames: bytes, loaded_frames: list[ServerFrame]) -> None:
    assert list(Parser().load_frames(raw_frames)) == loaded_frames


@pytest.mark.parametrize(
    ("raw_frames", "loaded_frames"), [(b"CONNECTED\nheader:1.0\n\n\x00", [ConnectedFrame(headers={"header": "1.0"})])]
)
def test_load_frames_again(raw_frames: bytes, loaded_frames: list[ServerFrame]) -> None:
    assert list(Parser().load_frames(raw_frames)) == loaded_frames
