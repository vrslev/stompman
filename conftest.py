from typing import cast

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


@pytest.fixture(
    params=[
        stompman.ConnectionParameters(host="activemq-artemis", port=61616, login="admin", passcode=":=123"),
        stompman.ConnectionParameters(host="activemq-classic", port=61613, login="admin", passcode=":=123"),
    ]
)
def connection_parameters(request: pytest.FixtureRequest) -> stompman.ConnectionParameters:
    return cast(stompman.ConnectionParameters, request.param)
