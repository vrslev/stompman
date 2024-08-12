import stompman


def test_error_str() -> None:
    error = stompman.RepeatedConnectionFailedError(retry_attempts=1, issues=[])
    assert str(error) == repr(error)
