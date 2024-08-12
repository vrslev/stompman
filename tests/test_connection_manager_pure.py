import itertools
from typing import Any, get_args
from unittest import mock

import pytest

from stompman import FailedAllConnectAttemptsError
from stompman.config import ConnectionParameters
from stompman.connection_manager import (
    ActiveConnectionState,
    attempt_to_connect,
    connect_to_first_server,
    make_healthy_connection,
)
from stompman.errors import (
    AllServersUnavailable,
    AnyConnectionIssue,
    ConnectionLost,
    ConnectionLostError,
    StompProtocolConnectionIssue,
)
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


async def test_make_healthy_connection_ok() -> None:
    active_connection_state = ActiveConnectionState(
        connection=mock.AsyncMock(), lifespan=mock.AsyncMock(enter=mock.AsyncMock(side_effect=[None]))
    )
    result = await make_healthy_connection(
        active_connection_state=active_connection_state, servers=[], connect_timeout=0
    )

    assert result is active_connection_state


async def test_make_healthy_connection_no_active_state() -> None:
    servers = [build_dataclass(ConnectionParameters) for _ in range(FAKER.pyint(max_value=10))]
    connect_timeout = FAKER.pyint()
    result = await make_healthy_connection(
        active_connection_state=None, servers=servers, connect_timeout=connect_timeout
    )

    assert result == AllServersUnavailable(servers=servers, timeout=connect_timeout)


async def test_make_healthy_connection_connection_lost() -> None:
    active_connection_state = ActiveConnectionState(
        connection=mock.AsyncMock(), lifespan=mock.AsyncMock(enter=mock.AsyncMock(side_effect=ConnectionLostError))
    )
    result = await make_healthy_connection(
        active_connection_state=active_connection_state, servers=[], connect_timeout=0
    )

    assert result == ConnectionLost()


@pytest.mark.parametrize("issue_type", get_args(StompProtocolConnectionIssue))
async def test_make_healthy_connection_stomp_protocol_issue(issue_type: type[StompProtocolConnectionIssue]) -> None:
    issue = build_dataclass(issue_type)
    active_connection_state = ActiveConnectionState(
        connection=mock.AsyncMock(), lifespan=mock.AsyncMock(enter=mock.AsyncMock(side_effect=[issue]))
    )
    result = await make_healthy_connection(
        active_connection_state=active_connection_state, servers=[], connect_timeout=0
    )

    assert result == issue
