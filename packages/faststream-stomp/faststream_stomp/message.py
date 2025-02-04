from typing import cast

import stompman
from faststream.broker.message import StreamMessage, gen_cor_id
from faststream.broker.message import decode_message as decode_message_sync
from faststream.utils.functions import to_async


class StompStreamMessage(StreamMessage[stompman.AckableMessageFrame]):
    async def ack(self) -> None:
        await self.raw_message.ack()
        return await super().ack()

    async def nack(self) -> None:
        await self.raw_message.nack()
        return await super().nack()
# TODO: Refactor

async def parse_message(message: stompman.AckableMessageFrame) -> StompStreamMessage:  # noqa: RUF029
    return StompStreamMessage(
        raw_message=message,
        body=message.body,
        headers=cast("dict[str, str]", message.headers),
        content_type=message.headers.get("content-type"),
        message_id=message.headers["message-id"],
        correlation_id=cast("str", message.headers.get("correlation-id", gen_cor_id())),
    )


decode_message = to_async(decode_message_sync)
