import pytest


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop"),
        pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
    ],
    autouse=True,
)
def anyio_backend(request: pytest.FixtureRequest) -> object:
    return request.param
