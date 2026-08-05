"""StateStoreManager for DuckDB and MotherDuck state backends.

DuckDB is an embedded, single-process database: a local ``.duckdb`` file can
only be opened for read/write by one process at a time, so a local-file
target is best suited to a single Meltano process. [MotherDuck][motherduck]
lifts that restriction by serving as a shared cloud endpoint, and is
addressed with the same ``md:<database>`` connection target DuckDB itself
uses.

Unlike ClickHouse, DuckDB supports real primary-key constraints and
row-level ``UPDATE``, so the state table uses a plain ``INSERT ... ON
CONFLICT`` upsert and the lock table relies on a primary-key violation to
detect contention, the same approach used by the Postgres/MSSQL backends.

[motherduck]: https://motherduck.com/
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from contextlib import contextmanager
from time import sleep
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, unquote, urlparse

import duckdb
from meltano.core.error import MeltanoError
from meltano.core.setting_definition import SettingDefinition, SettingKind
from meltano.core.state_store.base import (
    MeltanoState,
    MissingStateBackendSettingsError,
    StateIDLockedError,
    StateStoreManager,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable


DEFAULT_TABLE_NAME = "state"
DEFAULT_SCHEMA_NAME = "meltano"
LOCK_TIMEOUT_SECONDS = 30
STALE_LOCK_SECONDS = 300  # 5 minutes
MOTHERDUCK_PREFIX = "md:"

logger = logging.getLogger(__name__)


class DuckDBStateBackendError(MeltanoError):
    """Base error for the DuckDB state backend."""


DUCKDB_DATABASE = SettingDefinition(
    name="state_backend.duckdb.database",
    label="DuckDB Database",
    description=(
        "Local file path (or ':memory:') for an embedded database, or a "
        "MotherDuck database in the form 'md:<database>' for a shared/cloud "
        "database. Overrides the value derived from the URI."
    ),
    kind=SettingKind.STRING,
    env_specific=True,
)

DUCKDB_MOTHERDUCK_TOKEN = SettingDefinition(
    name="state_backend.duckdb.motherduck_token",
    label="MotherDuck Token",
    description="Authentication token used to connect to MotherDuck",
    kind=SettingKind.STRING,
    sensitive=True,
    env_specific=True,
)

DUCKDB_SCHEMA = SettingDefinition(
    name="state_backend.duckdb.schema",
    label="DuckDB Schema",
    description="Schema used for state storage (default: meltano)",
    kind=SettingKind.STRING,
    env_specific=True,
)

DUCKDB_TABLE = SettingDefinition(
    name="state_backend.duckdb.table",
    label="DuckDB Table",
    description="Table name for state storage (default: state)",
    kind=SettingKind.STRING,
    env_specific=True,
)


def database_target_from_uri(
    uri: str,
    *,
    database: str | None = None,
) -> tuple[str, str | None]:
    """Derive the duckdb connection target and any token embedded in the URI.

    Args:
        uri: the state backend URI (``duckdb://<path-or-md-target>``). The
            portion following ``duckdb://`` is passed straight through to
            ``duckdb.connect``, so it can be a local file path, ``:memory:``,
            or a MotherDuck target such as ``md:my_database``.
        database: explicit database override (path, ``:memory:``, or
            ``md:<database>``). Takes precedence over the URI.

    Returns:
        A tuple of ``(target, motherduck_token)``, where the token is the
        value of a ``motherduck_token`` query parameter on the URI, if any.

    Raises:
        MissingStateBackendSettingsError: if no database target can be determined.
    """
    parsed = urlparse(uri)
    query_params = parse_qs(parsed.query)
    token = query_params.get("motherduck_token", [None])[0]
    if token:
        token = unquote(token)

    target = database or unquote(f"{parsed.netloc}{parsed.path}")
    if not target:
        msg = "DuckDB database path or MotherDuck database is required"
        raise MissingStateBackendSettingsError(msg)

    return target, token


def catalog_name_from_target(target: str) -> str:
    """Derive the DuckDB catalog (database) name a connection target resolves to.

    DuckDB names the current/default catalog after the connection target: ``"memory"``
    for ``:memory:``, the file's stem for a local path, or the database name for a
    MotherDuck (``md:<database>``) target. Schema/table references built as ``schema.table``
    (rather than ``catalog.schema.table``) are resolved against this catalog implicitly,
    which becomes ambiguous the moment a schema name collides with it -- e.g. a MotherDuck
    database named "meltano" colliding with DEFAULT_SCHEMA_NAME. Fully qualifying every
    reference with the catalog name (derived here) sidesteps that ambiguity entirely.

    Args:
        target: the value passed to ``duckdb.connect()`` -- a local file path,
            ``":memory:"``, or ``"md:<database>"``.

    Returns:
        The catalog name DuckDB will use for this connection.
    """
    if target == ":memory:":
        return "memory"

    # ruff: disable[PTH119,PTH122]
    parsed = urlparse(target)
    if parsed.scheme in {"md", "motherduck"}:
        base_file = os.path.basename(parsed.path)
        path_db = os.path.splitext(base_file)[0]
        return path_db or "my_db"

    base_file = os.path.basename(target)
    return os.path.splitext(base_file)[0]
    # ruff: enable[PTH119,PTH122]


class DuckDBStateStoreManager(StateStoreManager):
    """State backend for DuckDB and MotherDuck."""

    @property
    @override
    def label(self) -> str:
        """Return a human-readable label for this backend."""
        return "MotherDuck" if self.is_motherduck else "DuckDB"

    def __init__(
        self,
        uri: str,
        *,
        database: str | None = None,
        motherduck_token: str | None = None,
        schema: str | None = None,
        table: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialise the DuckDBStateStoreManager.

        Args:
            uri: the state backend URI (``duckdb://<path-or-md-target>``).
            database: local file path, ``:memory:``, or ``md:<database>`` override.
            motherduck_token: MotherDuck authentication token.
            schema: schema used for state storage (default: meltano).
            table: state table name (default: state).
            kwargs: additional keyword args passed to the parent.
        """
        super().__init__(**kwargs)
        self.uri = uri
        self.target, uri_token = database_target_from_uri(uri, database=database)
        self.motherduck_token = motherduck_token or uri_token
        self.is_motherduck = self.target.startswith(MOTHERDUCK_PREFIX)

        self.schema = schema or DEFAULT_SCHEMA_NAME
        self.table = table or DEFAULT_TABLE_NAME
        # Fully qualified with the catalog name (not just schema.table) so a schema name
        # that collides with the catalog name -- e.g. a MotherDuck database named "meltano"
        # colliding with DEFAULT_SCHEMA_NAME -- doesn't hit DuckDB's "Ambiguous reference to
        # catalog or schema" error, which fully-qualified references sidestep entirely.
        self.catalog = catalog_name_from_target(self.target)
        self.state_table = f'"{self.catalog}"."{self.schema}"."{self.table}"'
        self.lock_table = f'"{self.catalog}"."{self.schema}"."{self.table}_lock"'

        self._connection: duckdb.DuckDBPyConnection | None = None
        self._ensure_tables()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get a cached duckdb connection.

        Returns:
            A duckdb connection.
        """
        if self._connection is None:
            config: dict[str, Any] = {}
            if self.motherduck_token:
                config["motherduck_token"] = self.motherduck_token
            self._connection = duckdb.connect(self.target, config=config)
        return self._connection

    @connection.setter
    def connection(self, value: duckdb.DuckDBPyConnection) -> None:
        """Set the duckdb connection (for testing/mocking)."""
        self._connection = value

    def _ensure_tables(self) -> None:
        """Create the schema and the state/lock tables if absent."""
        self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.catalog}"."{self.schema}"')
        self.connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.state_table} (
                state_id VARCHAR PRIMARY KEY,
                partial_state VARCHAR,
                completed_state VARCHAR,
                updated_at TIMESTAMP DEFAULT now()
            )
            """,
        )
        self.connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.lock_table} (
                state_id VARCHAR PRIMARY KEY,
                lock_id VARCHAR NOT NULL,
                locked_at TIMESTAMP DEFAULT now()
            )
            """,
        )

    @override
    def set(self, state: MeltanoState) -> None:
        """Upsert the state row for the given state id.

        Args:
            state: the state to set.
        """
        partial_json = json.dumps(state.partial_state) if state.partial_state else None
        completed_json = json.dumps(state.completed_state) if state.completed_state else None
        self.connection.execute(
            f"""
            INSERT INTO {self.state_table}
                (state_id, partial_state, completed_state, updated_at)
            VALUES (?, ?, ?, now())
            ON CONFLICT (state_id) DO UPDATE SET
                partial_state = EXCLUDED.partial_state,
                completed_state = EXCLUDED.completed_state,
                updated_at = now()
            """,  # noqa: S608
            (state.state_id, partial_json, completed_json),
        )

    @override
    def get(self, state_id: str) -> MeltanoState | None:
        """Get the state for the given state id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state, or None if not found.
        """
        row = self.connection.execute(
            f"SELECT partial_state, completed_state FROM {self.state_table} WHERE state_id = ?",  # noqa: S608
            (state_id,),
        ).fetchone()

        if row is None:
            return None

        partial_state, completed_state = row
        return MeltanoState(
            state_id=state_id,
            partial_state=json.loads(partial_state) if partial_state else {},
            completed_state=json.loads(completed_state) if completed_state else {},
        )

    @override
    def delete(self, state_id: str) -> None:
        """Delete state for the given state id.

        Args:
            state_id: the state_id to clear state for.
        """
        self.connection.execute(
            f"DELETE FROM {self.state_table} WHERE state_id = ?",  # noqa: S608
            (state_id,),
        )

    @override
    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared.
        """
        row = self.connection.execute(f"SELECT COUNT(*) FROM {self.state_table}").fetchone()  # noqa: S608
        count = int(row[0]) if row else 0
        self.connection.execute(f"TRUNCATE TABLE {self.state_table}")
        return count

    @override
    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state ids, optionally filtered by a glob pattern.

        Args:
            pattern: glob-style pattern to filter by.

        Returns:
            An iterable of state ids.
        """
        if pattern and pattern != "*":
            sql_pattern = pattern.replace("*", "%").replace("?", "_")
            rows = self.connection.execute(
                f"SELECT state_id FROM {self.state_table} WHERE state_id LIKE ?",  # noqa: S608
                (sql_pattern,),
            ).fetchall()
        else:
            rows = self.connection.execute(
                f"SELECT state_id FROM {self.state_table}",  # noqa: S608
            ).fetchall()
        return [str(row[0]) for row in rows]

    @override
    def close(self) -> None:
        """Close the duckdb connection if it has been opened."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _cleanup_stale_locks(self) -> None:
        """Remove locks older than STALE_LOCK_SECONDS."""
        self.connection.execute(
            f"""
            DELETE FROM {self.lock_table}
            WHERE locked_at < now() - INTERVAL '{STALE_LOCK_SECONDS} seconds'
            """,  # noqa: S608
        )

    @override
    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: float = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given state id using a lock table.

        DuckDB enforces primary-key constraints, so this relies on a
        constraint violation to detect contention, the same approach used by
        the Postgres/MSSQL backends.

        Args:
            state_id: the state_id to lock.
            retry_seconds: seconds to wait between retries.

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired within the timeout.
        """
        lock_id = str(uuid.uuid4())
        seconds_waited = 0.0

        while seconds_waited < LOCK_TIMEOUT_SECONDS:  # pragma: no branch
            self._cleanup_stale_locks()
            try:
                self.connection.execute(
                    f"INSERT INTO {self.lock_table} (state_id, lock_id) VALUES (?, ?)",  # noqa: S608
                    (state_id, lock_id),
                )
                break  # lock acquired
            except duckdb.ConstraintException:
                pass  # lock held by another process, retry

            seconds_waited += retry_seconds
            if seconds_waited >= LOCK_TIMEOUT_SECONDS:
                msg = f"Could not acquire lock for state_id: {state_id}"
                raise StateIDLockedError(msg)
            sleep(retry_seconds)

        try:
            yield
        finally:
            self.connection.execute(
                f"DELETE FROM {self.lock_table} WHERE state_id = ? AND lock_id = ?",  # noqa: S608
                (state_id, lock_id),
            )
