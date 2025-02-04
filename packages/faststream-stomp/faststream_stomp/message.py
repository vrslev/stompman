import typing
from typing import cast

import stompman
from faststream.broker.message import StreamMessage, gen_cor_id


class StompStreamMessage(StreamMessage[stompman.AckableMessageFrame]):
    async def ack(self) -> None:
        await self.raw_message.ack()
        return await super().ack()
    # TODO: test ack/nack
    async def nack(self) -> None:
        await self.raw_message.nack()
        return await super().nack()

    @classmethod
    async def from_frame(cls, message: stompman.AckableMessageFrame) -> typing.Self:
        return cls(
            raw_message=message,
            body=message.body,
            headers=cast("dict[str, str]", message.headers),
            content_type=message.headers.get("content-type"),
            message_id=message.headers["message-id"],
            correlation_id=cast("str", message.headers.get("correlation-id", gen_cor_id())),
        )
