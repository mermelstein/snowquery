#' @keywords internal
"_PACKAGE"

#' Run SQL on Snowflake, Redshift, Postgres, SQLite and DuckDB from a single function.
#'
#' This package provides a unified interface for issuing SQL queries across multiple data warehouses and
#' databases (Snowflake, Amazon Redshift, PostgreSQL, SQLite, DuckDB). It can also stream large remote
#' result sets directly into a local DuckDB file (`analytics.duckdb`) for fast, low-cost, repeatable analytics.
#' Snowflake queries are executed through the official Python connector via reticulate; the package will
#' auto-install and enforce a minimum connector version when first used.
#'
#' Credentials are managed through a single YAML file supporting multiple named connections.
#' To use this package, you will need to provide your database credentials in a
#' YAML file called `snowquery_creds.yaml`. The file should be located in the
#' root directory of your R project and should have the following format:
#'
#' ```yaml
#' ---
#' my_snowflake_dwh:
#'    db_type: 'snowflake' # or 'redshift' or 'postgres' or 'sqlite'
#'    account: 'your_account_name'
#'    warehouse: 'your_warehouse_name'
#'    database: 'your_database_name'
#'    username: 'your_username'
#'    password: 'your_password'
#'    role: 'your_role'
#' 
#' ```
#'
#' This follows a named connection format, where you can have multiple named connections in the same file. 
#' For example you might have a `my_snowflake_dwh` connection and a `my_snowflake_admin` connection, each with their own credentials.
#' 
#' Replace the values in the YAML file with your own credentials.
#' Once you have created the `snowquery_creds.yaml` file, you can use the
#' `queryDB()` function to query your database.
