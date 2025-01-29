from typing import Any

import pytest
import stompman

from test_stompman.conftest import build_dataclass


@pytest.mark.parametrize(
    "class_",
    [stompman.ConnectionLostError, stompman.FailedAllConnectAttemptsError, stompman.FailedAllWriteAttemptsError],
)
def test_error_str(class_: Any) -> None:  # noqa: ANN401
    error = build_dataclass(class_)
    assert str(error) == repr(error)
