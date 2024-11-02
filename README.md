# stompman

A Python client for STOMP asynchronous messaging protocol that is:

- asynchronous,
- not abandoned,
- has typed, modern, comprehensible API.

## How To Use

Before you start using stompman, make sure you have it installed:

```sh
uv add stompman
poetry add stompman
```

Initialize a client:

```python
async with stompman.Client(
    servers=[
        stompman.ConnectionParameters(host="171.0.0.1", port=61616, login="user1", passcode="passcode1"),
        stompman.ConnectionParameters(host="172.0.0.1", port=61616, login="user2", passcode="passcode2"),
    ],

    # Handlers:
    on_error_frame=lambda error_frame: print(error_frame.body),
    on_heartbeat=lambda: print("Server sent a heartbeat"),

    # SSL — can be either `None` (default), `True`, or `ssl.SSLContext'
    ssl=None,

    # Optional parameters with sensible defaults:
    heartbeat=stompman.Heartbeat(will_send_interval_ms=1000, want_to_receive_interval_ms=1000),
    connect_retry_attempts=3,
    connect_retry_interval=1,
    connect_timeout=2,
    connection_confirmation_timeout=2,
    disconnect_confirmation_timeout=2,
    read_timeout=2,
    write_retry_attempts=3,
) as client:
    ...
```

### Sending Messages

To send a message, use the following code:

```python
await client.send(b"hi there!", destination="DLQ", headers={"persistent": "true"})
```

Or, to send messages in a transaction:

```python
async with client.begin() as transaction:
    for _ in range(10):
        await transaction.send(body=b"hi there!", destination="DLQ", headers={"persistent": "true"})
        await asyncio.sleep(0.1)
```

### Listening for Messages

Now, let's subscribe to a destination and listen for messages:

```python
async def handle_message_from_dlq(message_frame: stompman.MessageFrame) -> None:
    print(message_frame.body)


await client.subscribe("DLQ", handle_message_from_dlq, on_suppressed_exception=print)
```

Entered `stompman.Client` will block forever waiting for messages if there are any active subscriptions.

Sometimes it's useful to avoid that:

```python
dlq_subscription = await client.subscribe("DLQ", handle_message_from_dlq, on_suppressed_exception=print)
await dlq_subscription.unsubscribe()
```

By default, subscription have ACK mode "client-individual". If handler successfully processes the message, an `ACK` frame will be sent. If handler raises an exception, a `NACK` frame will be sent. You can catch (and log) exceptions using `on_suppressed_exception` parameter:

```python
await client.subscribe(
    "DLQ",
    handle_message_from_dlq,
    on_suppressed_exception=lambda exception, message_frame: print(exception, message_frame),
)
```

You can change the ack mode used by specifying the `ack` parameter:

```python
# Server will assume that all messages sent to the subscription before the ACK'ed message are received and processed:
await client.subscribe("DLQ", handle_message_from_dlq, ack="client", on_suppressed_exception=print)

# Server will assume that messages are received as soon as it send them to client:
await client.subscribe("DLQ", handle_message_from_dlq, ack="auto", on_suppressed_exception=print)
```

You can pass custom headers to `client.subscribe()`:

```python
await client.subscribe("DLQ", handle_message_from_dlq, ack="client", headers={"selector": "location = 'Europe'"}, on_suppressed_exception=print)
```

### Cleaning Up

stompman takes care of cleaning up resources automatically. When you leave the context of async context managers `stompman.Client()`, or `client.begin()`, the necessary frames will be sent to the server.

### Handling Connectivity Issues

- If multiple servers were provided, stompman will attempt to connect to each one simultaneously and will use the first that succeeds. If all servers fail to connect, an `stompman.FailedAllConnectAttemptsError` will be raised. In normal situation it doesn't need to be handled: tune retry and timeout parameters in `stompman.Client()` to your needs.

- When connection is lost, stompman will attempt to handle it automatically. `stompman.FailedAllConnectAttemptsError` will be raised if all connection attempts fail. `stompman.FailedAllWriteAttemptsError` will be raised if connection succeeds but sending a frame or heartbeat lead to losing connection.

### ...and caveats

- stompman supports Python 3.11 and newer.
- It implements [STOMP 1.2](https://stomp.github.io/stomp-specification-1.2.html) — the latest version of the protocol.
- Heartbeats are required, and sent automatically in background (defaults to 1 second).

Also, I want to pointed out that:

- Protocol parsing is inspired by [aiostomp](https://github.com/pedrokiefer/aiostomp/blob/3449dcb53f43e5956ccc7662bb5b7d76bc6ef36b/aiostomp/protocol.py) (meaning: consumed by me and refactored from).
- stompman is tested and used with [Artemis ActiveMQ](https://activemq.apache.org/components/artemis/).
- Specification says that headers in CONNECT and CONNECTED frames shouldn't be escaped for backwards compatibility. stompman escapes headers in CONNECT frame (outcoming), but does not unescape headers in CONNECTED (outcoming).

### Examples

See producer and consumer examples in [testing/](./testing/).
