from types import TracebackType
import typing

if typing.TYPE_CHECKING:
    from stompman import MessageEvent

class AsyncProcessContext(typing.AsyncContextManager["MessageEvent"]):
    event: "MessageEvent"
    ack_on_error: bool
    propagate_exception: bool

    def __init__(
        self,
        event: "MessageEvent",
        *,
        ack_on_error: bool = False,
        propagate_exception: bool = False,
    ):
        self.event = event
        self.ack_on_error = ack_on_error
        self.propagate_exception = propagate_exception

    async def __aenter__(self) -> "MessageEvent":
        return self.event

    async def __aexit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None
    ) -> bool | None:
        if __exc_type is None:
            await self.event.ack()
            return True

        if self.ack_on_error:
            await self.event.ack()
        else:
            await self.event.nack()

        if self.propagate_exception:
            return False

        return True

