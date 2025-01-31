from typing import cast

import stompman
from faststream.broker.message import StreamMessage, gen_cor_id
from faststream.broker.message import decode_message as decode_message_sync
from faststream.utils.functions import to_async


async def parse_message(message: stompman.MessageFrame) -> StreamMessage[stompman.MessageFrame]:  # noqa: RUF029
    return StreamMessage(
        raw_message=message,
        body=message.body,
        headers=cast("dict[str, str]", message.headers),
        content_type=message.headers.get("content-type"),
        message_id=message.headers["message-id"],
        correlation_id=cast("str", message.headers.get("correlation-id", gen_cor_id())),
    )


decode_message = to_async(decode_message_sync)
