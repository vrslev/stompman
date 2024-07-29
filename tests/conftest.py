import pytest

import stompman


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
    ],
)
def anyio_backend(request: pytest.FixtureRequest) -> object:
    return request.param


async def noop_message_handler(frame: stompman.MessageFrame) -> None: ...


def noop_error_handler(exception: Exception, frame: stompman.MessageFrame) -> None: ...
