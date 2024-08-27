# type: ignore
# ruff: noqa
# TODO: Add Client.from_configs(connection_manager_config, connection_lifespan_config)
import asyncio
from collections.abc import Callable
from contextlib import AsyncExitStack
from typing import Self


class Heartbeat: ...


class MultiHostHostLike: ...


class ConnectionParameters:
    host: str
    port: int
    login: str
    passcode: str

    def from_pydantic_multihost_hosts(cls, hosts: list[MultiHostHostLike]) -> list[Self]: ...


class Connection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    read_max_chunk_size: int
    read_timeout: int

    @classmethod
    async def connect(
        cls, *, host: str, port: int, timeout: int, read_max_chunk_size: int, read_timeout: int
    ) -> Self | None: ...


class ActiveConnectionState:
    connection: Connection
    lifespan: "ConnectionLifespan"


class ConnectionLifespanFactory: ...


class ConnectionManagerConfig:
    servers: list[ConnectionParameters]
    connect_retry_attempts: int
    connect_retry_interval: int
    connect_timeout: int
    read_timeout: int
    read_max_chunk_size: int
    write_retry_attempts: int
    connection_class: type[Connection]


class ConnectionManager:
    lifespan_factory: ConnectionLifespanFactory
    connection_config: ConnectionManagerConfig
    _active_connection_state: ActiveConnectionState | None
    _reconnect_lock: asyncio.Lock


class ConnectionLifespanConfig:
    protocol_version: str
    heartbeat: Heartbeat
    connection_confirmation_timeout: int
    disconnect_confirmation_timeout: int


class ConnectionLifespan:
    lifespan_config: ConnectionLifespanConfig
    connection: Connection
    connection_parameters: ConnectionParameters
    active_subscriptions: object
    active_transactions: object
    set_heartbeat_interval: Callable[[float], None]


class Client:
    lifespan_config: ConnectionLifespanConfig
    connection_config: ConnectionManagerConfig

    # about client:
    on_error_frame: Callable[[object], None] | None
    on_heartbeat: Callable[[], None] | None

    # about current client
    _connection_manager: ConnectionManager
    _active_subscriptions: object
    _active_transactions: object
    _exit_stack: AsyncExitStack
    _heartbeat_task: asyncio.Task[None]
    _listen_task: asyncio.Task[None]
    _task_group: asyncio.TaskGroup
