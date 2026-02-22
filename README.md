# sync-duckdb-to-motherduck

Copy-paste reference for syncing local DuckDB data to [MotherDuck](https://motherduck.com). SQL, Python, and Bash methods — pick what fits.

## Prerequisites

- [DuckDB](https://duckdb.org/docs/installation/) v1.1+ (for `CREATE OR REPLACE DATABASE ... FROM` syntax)
- A [MotherDuck](https://app.motherduck.com/) account and API token
- Set your token: `export MOTHERDUCK_TOKEN=your_token_here`

## Methods at a Glance

| Method | Source | Best For |
|--------|--------|----------|
| `COPY FROM DATABASE` | Local DuckDB | Full database sync with control over source/target |
| `CREATE OR REPLACE DATABASE ... FROM '<path>'` | Local DuckDB file | One-liner clone |
| `CREATE OR REPLACE DATABASE ... FROM CURRENT_DATABASE()` | Local DuckDB (opened in CLI) | Quick push from CLI session |
| Parquet → MotherDuck | `.parquet` files | Syncing pipeline exports |
| PostgreSQL → MotherDuck | PostgreSQL | Cross-database migration |
| Python script | Local DuckDB / Parquet | Dynamic table discovery, selective sync |
| Bash wrapper | Local DuckDB | Cron jobs, CI pipelines |

## SQL Methods

All examples in [`sync_duckdb.sql`](sync_duckdb.sql). Run any block directly:

```bash
duckdb < sync_duckdb.sql
# or paste individual blocks into the DuckDB CLI
```

### 1. COPY FROM DATABASE

Attach both databases and copy everything:

```sql
ATTACH 'local.db' AS local_db;
ATTACH 'md:my_remote_db' AS remote_db;

COPY FROM DATABASE local_db TO remote_db;
```

### 2. CREATE OR REPLACE DATABASE ... FROM file path

One-liner clone:

```sql
ATTACH 'md:';
CREATE OR REPLACE DATABASE my_remote_db FROM '/path/to/local.db';
```

### 3. CREATE OR REPLACE DATABASE ... FROM CURRENT_DATABASE()

When you open a local file directly with the CLI:

```bash
duckdb local.db -c "ATTACH 'md:'; CREATE OR REPLACE DATABASE my_remote_db FROM CURRENT_DATABASE();"
```

### 4. Parquet → MotherDuck

```sql
ATTACH 'md:my_remote_db' AS remote_db;

CREATE OR REPLACE TABLE remote_db.main.my_table AS
  SELECT * FROM read_parquet('*.parquet');
```

### 5. PostgreSQL → MotherDuck

```sql
INSTALL postgres;
LOAD postgres;

ATTACH 'md:my_remote_db' AS remote_db;
ATTACH 'dbname=mydb user=postgres host=localhost' AS pg (TYPE POSTGRES, READ_ONLY);

CREATE OR REPLACE TABLE remote_db.main.users AS
  SELECT * FROM pg.public.users;
```

## Python

[`sync_duckdb.py`](sync_duckdb.py) discovers all tables automatically and syncs them with `CREATE OR REPLACE TABLE ... AS SELECT *`.

```bash
# Sync all tables
python sync_duckdb.py local.db my_remote_db

# Sync specific tables only
python sync_duckdb.py local.db my_remote_db --tables users,orders

# Sync parquet files
python sync_duckdb.py --parquet 'data/*.parquet' my_remote_db events
```

## Bash

[`sync_duckdb.sh`](sync_duckdb.sh) wraps the `CREATE OR REPLACE DATABASE ... FROM CURRENT_DATABASE()` method for cron or CI.

```bash
# One-off sync
./sync_duckdb.sh local.db my_remote_db

# Cron (every hour)
0 * * * * MOTHERDUCK_TOKEN=... /path/to/sync_duckdb.sh /path/to/local.db my_remote_db
```

## Tips & Caveats

- **Physical copy.** All methods above create a full physical copy — not a live link. Re-run to update.
- **Large tables.** For very large tables, consider chunking inserts or using `COPY FROM DATABASE` which handles this internally.
- **Context persistence.** When using the DuckDB CLI interactively, `ATTACH 'md:'` persists for the session. You don't need to re-attach between queries.
- **Token via `.env`.** Copy `.env.example` to `.env` and source it: `source .env && export MOTHERDUCK_TOKEN`

## Links

- [MotherDuck Documentation](https://motherduck.com/docs/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB ATTACH Statement](https://duckdb.org/docs/sql/statements/attach.html)
- [MotherDuck Python SDK](https://motherduck.com/docs/integrations/languages/python/)
