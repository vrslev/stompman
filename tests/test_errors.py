import stompman


def test_error_str() -> None:
    error = stompman.ConnectionConfirmationTimeoutError(timeout=1, frames=[])
    assert str(error) == repr(error)
