from typing import Any

import pytest

import stompman
from tests.conftest import build_dataclass


@pytest.mark.parametrize(
    "class_",
    [
        stompman.ConnectionLostError,
        stompman.FailedAllConnectAttemptsError,
        stompman.FailedAllWriteAttemptsError,
    ],
)
def test_error_str(class_: type[Any]) -> None:
    error = build_dataclass(class_)
    assert str(error) == repr(error)
