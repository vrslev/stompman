import itertools
from typing import Any, get_args
from unittest import mock

import pytest

from stompman import FailedAllConnectAttemptsError
from stompman.connection_manager import ActiveConnectionState, attempt_to_connect, connect_to_first_server
from stompman.errors import AnyConnectionIssue
from tests.conftest import build_dataclass
from tests.test_connection_manager import FAKER

pytestmark = [pytest.mark.anyio]


async def test_attempt_to_connect_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("asyncio.sleep", (sleep_mock := mock.AsyncMock()))
    active_connection_state = ActiveConnectionState(connection=mock.Mock(), lifespan=mock.Mock())
    connection_issues = [build_dataclass(issue_type) for issue_type in get_args(AnyConnectionIssue)]

    result = await attempt_to_connect(
        connect=mock.AsyncMock(side_effect=[*connection_issues, active_connection_state, connection_issues[0]]),
        connect_retry_interval=5,
        connect_retry_attempts=len(connection_issues) + 1,
    )

    assert result is active_connection_state
    assert sleep_mock.mock_calls == [mock.call(5), mock.call(10), mock.call(15), mock.call(20)]


@pytest.mark.usefixtures("mock_sleep")
async def test_attempt_to_connect_fails() -> None:
    connection_issues_generator = (
        build_dataclass(issue_type) for issue_type in itertools.cycle(get_args(AnyConnectionIssue))
    )
    attempts = FAKER.pyint(min_value=1, max_value=10)
    connection_issues = [next(connection_issues_generator) for _ in range(attempts)]

    with pytest.raises(FailedAllConnectAttemptsError) as exc_info:
        await attempt_to_connect(
            connect=mock.AsyncMock(side_effect=connection_issues),
            connect_retry_interval=FAKER.pyint(),
            connect_retry_attempts=attempts,
        )

    assert exc_info.value.issues == connection_issues
    assert exc_info.value.retry_attempts == attempts


async def return_argument_async(arg: Any) -> Any:  # noqa: ANN401, RUF029
    return arg


async def test_connect_to_first_server_ok() -> None:
    expected_active_connection_state = ActiveConnectionState(connection=mock.AsyncMock(), lifespan=mock.AsyncMock())
    active_connection_state = await connect_to_first_server(
        [
            return_argument_async(None),
            return_argument_async(None),
            return_argument_async(expected_active_connection_state),
            return_argument_async(None),
        ]
    )
    assert active_connection_state is expected_active_connection_state


async def test_connect_to_first_server_fails() -> None:
    active_connection_state = await connect_to_first_server(
        [
            return_argument_async(None),
            return_argument_async(None),
            return_argument_async(None),
            return_argument_async(None),
        ]
    )
    assert active_connection_state is None
