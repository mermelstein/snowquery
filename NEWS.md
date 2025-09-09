# snowquery 1.3.0

* DuckDB local caching: stream Snowflake / Postgres / Redshift query results into `analytics.duckdb` via `cache_table_name`.
* Snowflake connector auto-install + version enforcement (min 2.7.4) with resilient import logic.
* Batch streaming for Snowflake when `fetch_pandas_batches()` available, fallback to full fetch otherwise.
* Unified row count completion messages for all caching paths.
* Added SQLite examples & clarified credential YAML schema (including DuckDB + SQLite entries).
* Refactored internal connection helper; simplified error messaging.
* Documentation overhaul (README) to cover caching workflow, limitations & credential schema.
* Expanded package Title and Description to reflect multi-database + DuckDB caching functionality.

# snowquery 1.2.1

* Minor README / documentation wording update. Auto-install the snowflake python package if it cant be found

# snowquery 1.2.0

* Added SQLite support

# snowquery 1.1.0

* Added Redshift support

# snowquery 1.0.0

* Formalized structure for database credential file
* Allow overwriting YAML credential file with locally-passed variables
* Improved error messages for Snowflake and Postgres database reads
* Improved error messages for missing connection variables
* Connection timeout options
* Detect issues with local python environment

# snowquery 0.0.1

* Initial CRAN submission.
