from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock
from urllib.parse import urlparse

import duckdb
import pytest
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings
from meltano.core.state_store.base import MissingStateBackendSettingsError, StateIDLockedError

from meltano_state_backend_duckdb.backend import (
    DuckDBStateStoreManager,
    database_target_from_uri,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from meltano.core.project import Project


# ---------------------------------------------------------------------------
# database_target_from_uri
# ---------------------------------------------------------------------------


def test_database_target_from_local_path_uri() -> None:
    target, token = database_target_from_uri("duckdb:///absolute/path/state.duckdb")
    assert target == "/absolute/path/state.duckdb"
    assert token is None


def test_database_target_from_relative_path_uri() -> None:
    target, token = database_target_from_uri("duckdb://relative/state.duckdb")
    assert target == "relative/state.duckdb"
    assert token is None


def test_database_target_from_memory_uri() -> None:
    target, token = database_target_from_uri("duckdb://:memory:")
    assert target == ":memory:"
    assert token is None


def test_database_target_from_motherduck_uri() -> None:
    target, token = database_target_from_uri(
        "duckdb://md:my_database?motherduck_token=my_token",
    )
    assert target == "md:my_database"
    assert token == "my_token"  # noqa: S105


def test_database_target_percent_encoded_token() -> None:
    _, token = database_target_from_uri(
        "duckdb://md:my_database?motherduck_token=a%40b",
    )
    assert token == "a@b"  # noqa: S105


def test_database_override_takes_precedence() -> None:
    target, _ = database_target_from_uri(
        "duckdb://md:ignored",
        database="explicit.duckdb",
    )
    assert target == "explicit.duckdb"


def test_database_target_missing() -> None:
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="DuckDB database path or MotherDuck database is required",
    ):
        database_target_from_uri("duckdb://")


# ---------------------------------------------------------------------------
# manager construction / settings
# ---------------------------------------------------------------------------


def test_get_manager(project: Project) -> None:
    with mock.patch(
        "meltano_state_backend_duckdb.backend.DuckDBStateStoreManager._ensure_tables",
    ) as mock_ensure_tables:
        manager = state_store_manager_from_project_settings(project.settings)

    mock_ensure_tables.assert_called_once()
    assert isinstance(manager, DuckDBStateStoreManager)

    parsed = urlparse(manager.uri)
    assert parsed.scheme == "duckdb"

    assert manager.target == ":memory:"
    assert manager.schema == "test_schema"
    assert manager.table == "test_table"
    assert manager.is_motherduck is False


def test_get_manager_from_uri(project_with_uri: Project) -> None:
    with mock.patch(
        "meltano_state_backend_duckdb.backend.DuckDBStateStoreManager._ensure_tables",
    ) as mock_ensure_tables:
        manager = state_store_manager_from_project_settings(project_with_uri.settings)

    mock_ensure_tables.assert_called_once()
    assert isinstance(manager, DuckDBStateStoreManager)

    parsed = urlparse(manager.uri)
    assert parsed.scheme == "duckdb"

    assert manager.target == "md:test_database"
    assert manager.motherduck_token == "test_token"  # noqa: S105
    assert manager.is_motherduck is True
    assert manager.schema == "meltano"
    assert manager.table == "state"


@pytest.mark.parametrize(
    ("setting_name", "env_var_name"),
    (
        pytest.param(
            "state_backend.duckdb.database",
            "MELTANO_STATE_BACKEND_DUCKDB_DATABASE",
            id="database",
        ),
        pytest.param(
            "state_backend.duckdb.motherduck_token",
            "MELTANO_STATE_BACKEND_DUCKDB_MOTHERDUCK_TOKEN",
            id="motherduck_token",
        ),
        pytest.param(
            "state_backend.duckdb.schema",
            "MELTANO_STATE_BACKEND_DUCKDB_SCHEMA",
            id="schema",
        ),
        pytest.param(
            "state_backend.duckdb.table",
            "MELTANO_STATE_BACKEND_DUCKDB_TABLE",
            id="table",
        ),
    ),
)
def test_settings(project: Project, setting_name: str, env_var_name: str) -> None:
    setting = project.settings.find_setting(setting_name)
    assert setting is not None

    env_vars = setting.env_vars(prefixes=["meltano"])
    assert env_vars[0].key == env_var_name


def test_is_motherduck_default_database() -> None:
    with mock.patch(
        "meltano_state_backend_duckdb.backend.DuckDBStateStoreManager._ensure_tables",
    ):
        manager = DuckDBStateStoreManager(uri="duckdb://:memory:", database="md:")
    assert manager.is_motherduck is True


def test_is_motherduck_false_for_local_file() -> None:
    with mock.patch(
        "meltano_state_backend_duckdb.backend.DuckDBStateStoreManager._ensure_tables",
    ):
        manager = DuckDBStateStoreManager(uri="duckdb://:memory:", database="/tmp/state.duckdb")  # noqa: S108
    assert manager.is_motherduck is False


def test_connection_passes_motherduck_token() -> None:
    with mock.patch(
        "meltano_state_backend_duckdb.backend.DuckDBStateStoreManager._ensure_tables",
    ):
        manager = DuckDBStateStoreManager(
            uri="duckdb://md:my_db",
            motherduck_token="my_token",  # noqa: S106
        )

    with mock.patch("duckdb.connect") as mock_connect:
        _ = manager.connection

    mock_connect.assert_called_once_with("md:my_db", config={"motherduck_token": "my_token"})


# ---------------------------------------------------------------------------
# functional CRUD / lock behavior against a real in-memory database
# ---------------------------------------------------------------------------


@pytest.fixture
def subject() -> Generator[DuckDBStateStoreManager, None, None]:
    manager = DuckDBStateStoreManager(
        uri="duckdb://:memory:",
        schema="testschema",
        table="teststate",
    )
    yield manager
    manager.close()


def test_set_and_get_state(subject: DuckDBStateStoreManager) -> None:
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    subject.set(state)

    result = subject.get("test_job")
    assert result is not None
    assert result.state_id == "test_job"
    assert result.partial_state == {"singer_state": {"partial": 1}}
    assert result.completed_state == {"singer_state": {"complete": 1}}


def test_set_upserts_existing_state(subject: DuckDBStateStoreManager) -> None:
    subject.set(MeltanoState(state_id="test_job", partial_state={"a": 1}))
    subject.set(MeltanoState(state_id="test_job", partial_state={"a": 2}))

    result = subject.get("test_job")
    assert result is not None
    assert result.partial_state == {"a": 2}


def test_get_state_not_found(subject: DuckDBStateStoreManager) -> None:
    assert subject.get("nonexistent") is None


def test_get_state_with_null_values(subject: DuckDBStateStoreManager) -> None:
    subject.set(MeltanoState(state_id="test_job"))

    result = subject.get("test_job")
    assert result is not None
    assert result.partial_state == {}
    assert result.completed_state == {}


def test_delete_state(subject: DuckDBStateStoreManager) -> None:
    subject.set(MeltanoState(state_id="test_job", partial_state={"a": 1}))
    subject.delete("test_job")

    assert subject.get("test_job") is None


def test_clear_all(subject: DuckDBStateStoreManager) -> None:
    subject.set(MeltanoState(state_id="job1", partial_state={"a": 1}))
    subject.set(MeltanoState(state_id="job2", partial_state={"a": 2}))

    count = subject.clear_all()

    assert count == 2
    assert list(subject.get_state_ids()) == []


def test_get_state_ids(subject: DuckDBStateStoreManager) -> None:
    subject.set(MeltanoState(state_id="job1"))
    subject.set(MeltanoState(state_id="job2"))
    subject.set(MeltanoState(state_id="job3"))

    state_ids = sorted(subject.get_state_ids())
    assert state_ids == ["job1", "job2", "job3"]


def test_get_state_ids_with_pattern(subject: DuckDBStateStoreManager) -> None:
    subject.set(MeltanoState(state_id="test_job_1"))
    subject.set(MeltanoState(state_id="test_job_2"))
    subject.set(MeltanoState(state_id="other_job"))

    state_ids = sorted(subject.get_state_ids("test_*"))
    assert state_ids == ["test_job_1", "test_job_2"]


def test_close_is_idempotent(subject: DuckDBStateStoreManager) -> None:
    subject.close()
    assert subject._connection is None
    subject.close()  # no-op
    assert subject._connection is None


# ---------------------------------------------------------------------------
# acquire_lock
# ---------------------------------------------------------------------------


def test_acquire_lock(subject: DuckDBStateStoreManager) -> None:
    with subject.acquire_lock("test_job", retry_seconds=0):
        rows = subject.connection.execute(
            'SELECT lock_id FROM "testschema"."teststate_lock" WHERE state_id = ?',
            ("test_job",),
        ).fetchall()
        assert len(rows) == 1

    rows = subject.connection.execute(
        'SELECT lock_id FROM "testschema"."teststate_lock" WHERE state_id = ?',
        ("test_job",),
    ).fetchall()
    assert rows == []


def test_acquire_lock_retry(subject: DuckDBStateStoreManager) -> None:
    """A held lock blocks a second acquirer until the first releases."""
    with subject.acquire_lock("test_job", retry_seconds=0):
        with (
            mock.patch("meltano_state_backend_duckdb.backend.sleep") as mock_sleep,
            mock.patch(
                "meltano_state_backend_duckdb.backend.LOCK_TIMEOUT_SECONDS",
                0.05,
            ),
            pytest.raises(StateIDLockedError),
            subject.acquire_lock("test_job", retry_seconds=0.01),
        ):
            pass  # pragma: no cover
        assert mock_sleep.call_count > 0


def test_acquire_lock_max_retries_exceeded() -> None:
    manager = DuckDBStateStoreManager(uri="duckdb://:memory:", schema="s", table="t")

    def execute_side_effect(query: str, *_args: object) -> None:
        if query.strip().startswith("INSERT"):
            msg = "duplicate"
            raise duckdb.ConstraintException(msg)

    mock_connection = mock.Mock()
    mock_connection.execute.side_effect = execute_side_effect
    manager.connection = mock_connection

    retry_seconds = 0.01
    with (
        mock.patch("meltano_state_backend_duckdb.backend.sleep") as mock_sleep,
        pytest.raises(
            StateIDLockedError,
            match="Could not acquire lock for state_id: test_job",
        ),
        manager.acquire_lock("test_job", retry_seconds=retry_seconds),
    ):
        pass  # pragma: no cover

    assert mock_sleep.call_count == int(30 / retry_seconds) - 1


def test_acquire_lock_multiple_retries_then_success() -> None:
    manager = DuckDBStateStoreManager(uri="duckdb://:memory:", schema="s", table="t")
    mock_connection = mock.Mock()
    mock_connection.execute.side_effect = [
        None,  # cleanup stale locks
        duckdb.ConstraintException("duplicate"),  # insert fails
        None,  # cleanup stale locks
        duckdb.ConstraintException("duplicate"),  # insert fails
        None,  # cleanup stale locks
        None,  # insert succeeds
        None,  # release
    ]
    manager.connection = mock_connection

    with (
        mock.patch("meltano_state_backend_duckdb.backend.sleep") as mock_sleep,
        manager.acquire_lock("test_job", retry_seconds=0.01),
    ):
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(0.01)
