from stompman import ConnectionConfirmationTimeoutError


def test_error_str() -> None:
    error = ConnectionConfirmationTimeoutError(timeout=1)
    assert str(error) == repr(error)
