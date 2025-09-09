#' @title Query a database
#' @description Run a SQL query on a Snowflake, Redshift or Postgres database and return the results as a data frame. See the [snowquery README](https://github.com/mermelstein/snowquery#credentials) for more information on how to pass in your credentials.
#'
#' @param query A string of the SQL query to execute
#' @param conn_name The name of the connection to use in snowquery_creds.yaml (e.g. "my_snowflake_dwh")
#' @param db_type The type of database to connect to (e.g. "snowflake", "redshift" or "postgres")
#' @param username The username to use for authentication
#' @param password The password to use for authentication
#' @param host The hostname or IP address of the database server
#' @param port The port number to use for the database connection
#' @param database The name of the database to connect to
#' @param warehouse [Snowflake](https://www.snowflake.com/data-cloud-glossary/virtual-warehouse/) The name of the warehouse to use for the Snowflake connection
#' @param account [Snowflake](https://docs.snowflake.com/en/user-guide/admin-account-identifier/) The name of the Snowflake account to connect to
#' @param role [Snowflake](https://docs.snowflake.com/en/sql-reference/ddl-user-security/) The name of the role to use for the Snowflake connection
#' @param sslmode Whether to use sslmode for the postgres or redshift connection
#' @param timeout The number of seconds to wait for the database to connect successfully
#' @param cache_table_name The name of the table to create inside the DuckDB file. If provided, the query result is streamed directly to DuckDB and a confirmation message is returned instead of a data frame.
#' @param overwrite A boolean (TRUE/FALSE) to control whether to overwrite an existing table in the cache.
#' @return A data frame containing the results of the query, or a confirmation message if `cache_table_name` is used.
#' @examples
#' \dontrun{
#' # Query the database and get a dataframe of results
#' result <- queryDB("SELECT * FROM my_table", conn_name='my_snowflake_dwh')
#' print(result)
#' }
#' \dontrun{
#' # Stream a large query result directly to the local DuckDB cache
#' queryDB("SELECT * FROM very_large_table",
#'         conn_name = 'my_snowflake_dwh',
#'         cache_table_name = 'large_table_local',
#'         overwrite = TRUE)
#' }
#'
#' @import yaml
#' @import reticulate
#' @import RPostgres
#' @import RSQLite
#' @import duckdb
#' @import DBI
#' @importFrom reticulate import use_python
#' @importFrom reticulate import import
#' @export
queryDB <- function(
  query,
  conn_name = "default",
  db_type = NULL,
  username = NULL,
  password = NULL,
  host = NULL,
  port = NULL,
  database = NULL,
  warehouse = NULL,
  account = NULL,
  role = NULL,
  sslmode = NULL,
  timeout = 15,
  cache_table_name = NULL,
  overwrite = TRUE)
{
  # Determine db_type: prioritize function argument, then YAML file.
  snowquery_creds <- yaml::read_yaml('~/snowquery_creds.yaml', fileEncoding = "UTF-8")
  conn_details <- snowquery_creds[[conn_name]]
  db_type_check <- if (!is.null(db_type)) {
    tolower(db_type)
  } else if (!is.null(conn_details$db_type)) {
    tolower(conn_details$db_type)
  } else {
    ""
  }

  # Caching logic: only applies if cache_table_name is given AND db is NOT Snowflake.
  if (!is.null(cache_table_name) && db_type_check != 'snowflake') {
    if (db_type_check == 'duckdb') {
      stop("Cannot cache a query from a duckdb source to itself.")
    }
    
    # For Postgres/Redshift, use the memory-efficient streaming method.
    message(paste("Caching", db_type_check, "query to DuckDB table:", cache_table_name))
    return(.cache_query_result(
      source_conn_name = conn_name,
      source_query = query,
      dest_table_name = cache_table_name,
      overwrite = overwrite
    ))

  } else {
    # Standard query-to-dataframe logic for ALL databases.
    # This block handles:
    # 1. All Snowflake queries.
    # 2. All non-caching queries for other DBs.

    if (!is.null(cache_table_name) && db_type_check == 'snowflake') {
      message("Note: Caching is not supported for Snowflake. Returning a dataframe instead.")
    }

    # Get connection using the helper function
    con <- .get_db_connection(conn_name = conn_name, db_type = db_type, config_path = "~/snowquery_creds.yaml")
    
    is_snowflake_conn <- "snowflake.connector.connection.SnowflakeConnection" %in% class(con)

    if (is_snowflake_conn) {
      cursor <- con$cursor()
      cursor$execute(query)
      df <- cursor$fetch_pandas_all()
      cursor$close()
      con$close()
      df[] <- lapply(df, function(x) if(is.list(x)) sapply(x, paste, collapse = ",") else x)
    } else {
      # This works for Postgres, Redshift, SQLite, and DuckDB (when querying)
      res <- DBI::dbSendQuery(con, query)
      df <- DBI::dbFetch(res)
      DBI::dbClearResult(res)
      DBI::dbDisconnect(con)
    }

    return(df)
  }
}
