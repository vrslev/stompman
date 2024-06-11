# stompman

A Python client for STOMP asynchronous messaging protocol that is:

- asynchronous,
- not abandoned,
- has pleasant and comprehensible API. Also: async generators, match statements, heavy typing coverage and no callbacks.

There were no such one—and now there're is.

## How To Use

Before you start using stompman, make sure you have it installed:

```sh
pip install stompman
poetry add stompman
```

Initialize a client:

```python
async with stompman.Client(
    servers=[
        stompman.ConnectionParameters(host="171.0.0.1", port=61616, login="user1", passcode="passcode1"),
        stompman.ConnectionParameters(host="172.0.0.1", port=61616, login="user2", passcode="passcode2"),
    ],
    # Optional parameters with sensible defaults:
    heartbeat=stompman.Heartbeat(will_send_interval_ms=1000, want_to_receive_interval_ms=1000),
    connect_retry_attempts=3,
    connect_retry_interval=1,
    connect_timeout=2,
    connection_confirmation_timeout=2,
    read_timeout=2,
) as client:
    ...
```

### Sending Messages

To send a message, use the following code:

```python
await client.send(body=b"hi there!", destination="DLQ", headers={"persistent": "true"})
```

Or, to send messages in a transaction:

```python
async with client.enter_transaction() as transaction:
    for _ in range(10):
        await client.send(body=b"hi there!", destination="DLQ", transaction=transaction)
        await asyncio.sleep(0.1)
```

### Listening for Messages

Now, let's subscribe to a queue and listen for messages.

Notice that `listen_to_events()` is not bound to a destination: it will listen to all subscribed destinations. If you want separate subscribtions, create separate clients for that.

```python
async with client.subscribe("DLQ"):
    async for event in client.listen_to_events():
        ...
```

`...`—and that's where it gets interesting.

Before learning how to processing messages from server, we need to understand how other libraries do it. They use callbacks. Damn callbacks in asynchronous programming.

I wanted to avoid them, and came up with an elegant solution: combining async generator and match statement. Here how it looks like:

```python
async for event in client.listen_to_events():
    match event:
        case stompman.MessageEvent(body=body):
            print(f"message: {body!s}")
            await event.ack()
        case stompman.ErrorEvent(message_header=short_description, body=body):
            print(f"{short_description}:\n{body!s}")
```

More complex example, that involves handling all possible events:

```python
async with asyncio.TaskGroup() as task_group:
    async for event in client.listen_to_events():
        match event:
            case stompman.MessageEvent(body=body):
                # Validate message ASAP and ack/nack, so that server won't assume we're not reliable
                try:
                    validated_message = MyMessageModel.model_validate_json(body)
                except ValidationError:
                    await event.nack()
                    raise

                await event.ack()
                task_group.create_task(run_business_logic(validated_message))
            case stompman.ErrorEvent(message_header=short_description, body=body):
                logger.error(
                    "Received an error from server", short_description=short_description, body=body, event=event
                )
            case stompman.HeartbeatEvent():
                task_group.create_task(update_healthcheck_status())
```

### Cleaning Up

stompman takes care of cleaning up resources automatically. When you leave the context of async context managers `stompman.Client()`, `client.subscribe()`, or `client.enter_transaction()`, the necessary frames will be sent to the server.

### Handling Connectivity Issues

- If multiple servers were provided, stompman will attempt to connect to each one simultaneously and will use the first that succeeds.

- If all servers fail to connect, an `stompman.FailedAllConnectAttemptsError` will be raised. In normal situation it doesn't need to be handled: tune retry and timeout parameters in `stompman.Client()` to your needs.

- If a connection is lost, a `stompman.ConnectionLostError` will be raised. You should implement reconnect logic manually, for example, with stamina:

  ```python
  for attempt in stamina.retry_context(on=stompman.ConnectionLostError):
      with attempt:
          async with stompman.Client(...) as client:
              ...
  ```

### ...and caveats

- stompman only runs on Python 3.11 and newer.
- It implements [STOMP 1.2](https://stomp.github.io/stomp-specification-1.2.html) — the latest version of the protocol.
- The client-individual ack mode is used, which means that server requires `ack` or `nack`. In contrast, with `client` ack mode server assumes you don't care about messages that occured before you connected. And, with `auto` ack mode server assumes client successfully received the message.
- Heartbeats are required, and sent automatically on `listen_to_events()` (defaults to 1 second).

Also, I want to pointed out that:

- Protocol parsing is inspired by [aiostomp](https://github.com/pedrokiefer/aiostomp/blob/3449dcb53f43e5956ccc7662bb5b7d76bc6ef36b/aiostomp/protocol.py) (meaning: consumed by me and refactored from).
- stompman is tested and used with [Artemis ActiveMQ](https://activemq.apache.org/components/artemis/).

## No docs

I try to keep it simple and easy to understand. May be counter-intuitive for some, but concise high-quality code speaks for itself. There're no comments and little indirection. Read the source if you wish, leave an issue if it's not enough or you want to add or fix something.
