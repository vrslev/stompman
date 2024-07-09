import typing
from types import TracebackType

if typing.TYPE_CHECKING:
    from stompman import MessageEvent

E_co = typing.TypeVar("E_co", bound=BaseException, covariant=True)


class AsyncProcessContext(typing.AsyncContextManager["MessageEvent"]):
    event: "MessageEvent"
    on_suppressed_exception: typing.Callable[[E_co, "MessageEvent"], typing.Any]
    supressed_exception_classes: tuple[type[Exception], ...] = (Exception,)

    def __init__(
        self,
        event: "MessageEvent",
        *,
        on_suppressed_exception: typing.Callable[[E_co, "MessageEvent"], typing.Any],
        supressed_exception_classes: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        self.event = event
        self.on_suppressed_exception = on_suppressed_exception  # type: ignore[assignment]
        self.supressed_exception_classes = supressed_exception_classes

    async def __aenter__(self) -> "MessageEvent":
        return self.event

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, __traceback: TracebackType | None
    ) -> bool | None:
        called_nack = False
        exception_suppressed = True

        if exc_type is not None and exc_value is not None:
            if not issubclass(exc_type, self.supressed_exception_classes):
                await self.event.ack()
                return False

            called_nack = True
            exception_suppressed = True
            await self.event.nack()
            self.on_suppressed_exception(exc_value, self.event)

        if not called_nack:
            await self.event.ack()

        return exception_suppressed
