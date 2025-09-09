# Snowquery

<!-- badges: start -->
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/snowquery)](https://cran.r-project.org/package=snowquery)
<!-- badges: end -->

## Overview

Run SQL queries on Snowflake, Redshift, Postgres, SQLite, or a local DuckDB database from R using a single function: `queryDB()`. 

snowquery now also supports streaming remote query results directly into a local DuckDB file for fast, repeatable, low‑cost analysis.

### Installation

```r
# The easiest way to get snowquery
install.packages("snowquery")

# Or you can get the development version from GitHub
# install.packages("devtools")
devtools::install_github("mermelstein/snowquery")
```

### Redshift notes

Redshift is currently only available on the development version of this package. See [installation instructions](#installation) above.

When connecting to a Redshift DWH you might need to specify an SSL connection. You can do this with a `sslmode='require'` connection variable or by passing that to the `queryDB()` function directly.

### Snowflake notes

Because Snowflake's driver requires a ton of fiddling in order to make it work for R. It sucks. A lot.

To sum up the current experience of running SQL against Snowflake from:

  - python: good &#x2705;
  - R: bad &#x274C;

That's why the `snowquery` package takes the [Snowflake python connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas) and leverages it in the background to run queries from R.

### Documentation

For more information on using `snowquery`, please see the [package website](https://snowquery.org).

### Requirements

Base R dependencies are installed automatically via the DESCRIPTION. Additional runtime needs:

1. DuckDB caching: `duckdb` (installed automatically when you install the package, but you may also install manually with `install.packages("duckdb")`).
2. Snowflake: A Python 3 runtime accessible to `reticulate`. The package will automatically install / upgrade `snowflake-connector-python[pandas]` the first time you run a Snowflake query. No manual `pip install` is required in normal use.
3. Redshift / Postgres: Handled through `RPostgres` (already imported). If you need SSL, set `sslmode` in the YAML or pass it directly.
4. SQLite: Provide a file path in the credential YAML (see below).

If the package cannot find a working Python, you'll receive an actionable error explaining what to install.

### Credentials

For all db connections you will need a YAML file called `snowquery_creds.yaml` at `~/snowquery_creds.yaml` with one or more named connections:


```yaml
---
my_snowflake_dwh:
  db_type: snowflake
  account: your_account
  warehouse: your_wh
  database: your_db
  user: your_user          # note: key is 'user' not 'username' for snowflake
  password: your_pw
  role: your_role

my_redshift_dwh:
  db_type: redshift
  host: redshift-cluster.host.aws.com
  port: 5439
  database: analytics
  username: rs_user
  password: rs_pw
  sslmode: require

my_postgres_db:
  db_type: postgres
  host: localhost
  port: 5432
  database: pg_db
  username: pg_user
  password: pg_pw

my_sqlite_db:
  db_type: sqlite
  database: /path/to/local.sqlite

my_duckdb_local:
  db_type: duckdb   # connects to the default analytics.duckdb file in working dir

```

This follows a named connection format, where you can have multiple named connections in the same file. For example you might have a `my_snowflake_dwh` connection and a `my_snowflake_admin` connection, each with their own credentials.

This package looks for the credential file at this location: `~/snowquery_creds.yaml`. **If it is in any other location it will not work.** If the package cannot locate the file you will receive an error like: `cannot open file '/expected/path/to/file/snowquery_creds.yaml': No such file or directory`. You can manually pass credentials to the `queryDB()` function but it is recommended to use the YAML file.

You are now ready to query away!

### Usage

Load this library in your R environment with `library(snowquery)`.

#### Basic Remote Query
```r
queryDB("SELECT * FROM MY_AWESOME_TABLE", conn_name='my_snowflake_dwh')
```
or
```r
# Query Snowflake and get results as a data frame
results <- queryDB("SELECT * FROM my_large_table LIMIT 1000", conn_name = "my_snowflake_dwh")
```
or 
```r
# You can also pass in credentials manually
results <- queryDB("SELECT * FROM my_table",
                   db_type='snowflake',
                   username='my_username',
                   password='my_password',
                   account='my_account',
                   database='my_database',
                   warehouse='my_warehouse',
                   role='my_role',
                   timeout=30)
print(results)
```

#### Caching to DuckDB
Provide `cache_table_name` to stream results into the local DuckDB file `analytics.duckdb`:

```r
# Cache a Snowflake query (streaming batches when possible)
queryDB(
  "SELECT * FROM MY_SCHEMA.BIG_FACT_TABLE WHERE load_date >= '2025-09-01'",
  conn_name = "my_snowflake_dwh",
  cache_table_name = "big_fact_local",
  overwrite = TRUE
)
# message: Successfully cached 1234567 rows to DuckDB table 'big_fact_local' (analytics.duckdb).

# Cache a Postgres query (uses dplyr/dbplyr lazy streaming under the hood)
queryDB(
  "SELECT id, event_ts, metric FROM events WHERE event_ts >= now() - interval '7 days'",
  conn_name = "my_postgres_db",
  cache_table_name = "recent_events",
  overwrite = TRUE
)
```

Key behaviors:
* Snowflake path streams using `fetch_pandas_batches()` when available; otherwise falls back to a single fetch.
* Postgres / Redshift path uses a lazy dplyr table registered into DuckDB for efficient transfer.
* `overwrite = TRUE` creates/replaces the DuckDB table; set `overwrite = FALSE` to append (for Snowflake append happens batch‑by‑batch; for Postgres/Redshift you can modify logic as needed).
* You cannot cache from a DuckDB source (`db_type == 'duckdb'`).

#### Querying the Local DuckDB Cache
Add a DuckDB connection to the YAML (see `my_duckdb_local` example above) and query cached tables:

```r
local_summary <- queryDB(
  "SELECT COUNT(*) AS n_rows FROM big_fact_local",
  conn_name = "my_duckdb_local"
)
local_summary
```

You can also use DBI or dplyr directly:

```r
library(DBI)
duck_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "analytics.duckdb")
DBI::dbGetQuery(duck_con, "SELECT * FROM recent_events LIMIT 5")
DBI::dbDisconnect(duck_con, shutdown = TRUE)
```

There is one function you need: `queryDB()`. It will take a SQL query as a string parameter and run it on the db.

For example:

```R
library(snowquery)

query <- "SELECT * FROM MY_AWESOME_TABLE"
result_dataframe <- queryDB(query, conn_name='my_snowflake_dwh')
print(result_dataframe)
```

or 

```R
library(snowquery)

queryDB("SELECT * FROM MY_AWESOME_TABLE", conn_name='my_snowflake_dwh')
```

or

```R
library(snowquery)
# You can also pass in credentials manually
result <- queryDB("SELECT * FROM my_table",
                   db_type='snowflake',
                   username='my_username',
                   password='my_password',
                   account='my_account',
                   database='my_database',
                   warehouse='my_warehouse',
                   role='my_role',
                   timeout=30)
print(result)
```

### DuckDB Caching Notes

Workflow:
1. Pull once from a remote DWH.
2. Iterate locally against DuckDB for joins, aggregations, prototyping.
3. Refresh by re‑running with `overwrite = TRUE`, or append with `overwrite = FALSE` (Snowflake / append path: ensure schema consistency).

Why DuckDB?
* Fast analytical execution (vectorized / columnar).
* Lightweight (no server to run).
* Plays nicely with data frames and dplyr (`dbplyr` translations work out of the box for Postgres/Redshift streaming path).

Limitations / Notes:
* Snowflake streaming depends on connector feature availability; falls back to full fetch if batch iterator missing.
* Appending from heterogenous schemas is not validated automatically.
* No explicit indexing (internal zone maps generally sufficient).
* Caching from a DuckDB source is intentionally blocked (it is already local).

Planned (future): progress display, verbosity flag, helper to enumerate cached tables.