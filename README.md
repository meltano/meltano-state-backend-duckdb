# `meltano-state-backend-duckdb`

<!-- Display these if and when we publish to PyPI. -->

<!--
[![PyPI version](https://img.shields.io/pypi/v/meltano-state-backend-duckdb.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-state-backend-duckdb)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-state-backend-duckdb.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-state-backend-duckdb) -->

This is a [Meltano] extension that provides a [DuckDB]/[MotherDuck] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with git+https://github.com/meltano/meltano-state-backend-duckdb.git meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano git+https://github.com/meltano/meltano-state-backend-duckdb.git
```

## Configuration

To store state in DuckDB, set the `state_backend.uri` setting to `duckdb://` followed by a
local file path, `:memory:`, or a [MotherDuck] database in the form `md:<database>`. The
value following `duckdb://` is passed straight through to `duckdb.connect`.

State will be stored in two tables that Meltano will create automatically:

- `state` - Stores the actual state data
- `state_lock` - Manages concurrency locks

All connection parameters can be provided in the URI, as individual Meltano settings, or a
mix of both. Explicit settings take precedence over URI values.

### Local file

```yaml
state_backend:
  uri: duckdb:///absolute/path/to/state.duckdb
```

> [!IMPORTANT]
> DuckDB is an embedded, single-process database. A local `.duckdb` file can only be opened
> for read/write by one process at a time, so a local-file target is best suited to a single
> Meltano process running state operations sequentially.

### MotherDuck

```yaml
state_backend:
  uri: duckdb://md:my_database
  duckdb:
    motherduck_token: my_token
```

Or with the token embedded in the URI (must be URL-encoded if it contains special characters):

```yaml
state_backend:
  uri: duckdb://md:my_database?motherduck_token=my_token
```

Or via an environment variable:

```bash
export MELTANO_STATE_BACKEND_DUCKDB_MOTHERDUCK_TOKEN='my_token'
meltano config set meltano state_backend.uri 'duckdb://md:my_database'
```

[MotherDuck] is a cloud-hosted DuckDB service and does not have the single-writer restriction
of a local file, so it's a better fit for concurrent Meltano runs.

### Explicit settings

The database target, schema, and table can also be set independently of the URI:

```yaml
state_backend:
  uri: duckdb://
  duckdb:
    database: /absolute/path/to/state.duckdb  # or 'md:my_database' or ':memory:'
    schema: meltano   # Optional: defaults to meltano
    table: state      # Optional: defaults to state
```

#### Connection Parameters

- **database**: Local file path, `:memory:`, or `md:<database>` for MotherDuck (overrides the URI)
- **motherduck_token**: Authentication token used to connect to MotherDuck
- **schema**: Schema where state tables will be created (defaults to `meltano`)
- **table**: Table name used for state storage (defaults to `state`)

## Development

### Setup

```bash
uv sync
```

### Run tests

Run lint, type checks, and tests for the current Python version:

```bash
uvx --with tox-uv --with tox-gh tox -e lint,types,3.14
```

To run the full matrix (requires Python 3.10–3.14 installed locally):

```bash
uvx --with tox-uv --with tox-gh tox run-parallel
```

### Bump the version

Using the [GitHub CLI][gh]:

```bash
gh release create v<new-version>
```

[duckdb]: https://duckdb.org/
[gh]: https://cli.github.com/
[meltano]: https://meltano.com
[motherduck]: https://motherduck.com/
[pipx]: https://github.com/pypa/pipx
[state-backend]: https://docs.meltano.com/concepts/state_backends
[uv]: https://docs.astral.sh/uv
