-- sync_duckdb.sql — 5 methods to sync data into MotherDuck
-- Run any block with: duckdb < sync_duckdb.sql (or paste into DuckDB CLI)

-- ============================================================================
-- Method 1: COPY FROM DATABASE — attach both databases, copy everything
-- ============================================================================
-- Best for: full database sync when you want control over source/target

ATTACH 'local.db' AS local_db;
ATTACH 'md:my_remote_db' AS remote_db;

COPY FROM DATABASE local_db TO remote_db;

DETACH local_db;
DETACH remote_db;

-- ============================================================================
-- Method 2: CREATE OR REPLACE DATABASE ... FROM '<path>'
-- ============================================================================
-- Best for: one-liner clone of a local file to MotherDuck

ATTACH 'md:' AS md;

CREATE OR REPLACE DATABASE my_remote_db FROM '/path/to/local.db';

-- ============================================================================
-- Method 3: CREATE OR REPLACE DATABASE ... FROM CURRENT_DATABASE()
-- ============================================================================
-- Best for: when you open a local .db file directly with the DuckDB CLI
-- Usage: duckdb local.db -c "ATTACH 'md:'; CREATE OR REPLACE DATABASE my_remote_db FROM CURRENT_DATABASE();"

ATTACH 'md:';

CREATE OR REPLACE DATABASE my_remote_db FROM CURRENT_DATABASE();

-- ============================================================================
-- Method 4: Parquet files → MotherDuck
-- ============================================================================
-- Best for: syncing parquet exports (e.g. from Spark, dbt, or data pipelines)

ATTACH 'md:my_remote_db' AS remote_db;

CREATE OR REPLACE TABLE remote_db.main.my_table AS
  SELECT * FROM read_parquet('*.parquet');

-- Multiple specific files:
CREATE OR REPLACE TABLE remote_db.main.events AS
  SELECT * FROM read_parquet(['events_2024.parquet', 'events_2025.parquet']);

DETACH remote_db;

-- ============================================================================
-- Method 5: PostgreSQL → MotherDuck
-- ============================================================================
-- Best for: migrating or syncing tables from Postgres directly

INSTALL postgres;
LOAD postgres;

ATTACH 'md:my_remote_db' AS remote_db;
ATTACH 'dbname=mydb user=postgres host=localhost' AS pg (TYPE POSTGRES, READ_ONLY);

CREATE OR REPLACE TABLE remote_db.main.users AS
  SELECT * FROM pg.public.users;

CREATE OR REPLACE TABLE remote_db.main.orders AS
  SELECT * FROM pg.public.orders;

DETACH pg;
DETACH remote_db;
