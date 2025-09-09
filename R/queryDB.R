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
#' @param warehouse [Snowflake](https://docs.snowflake.com/en/user-guide/warehouses/) The name of the warehouse to use for the Snowflake connection
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

  # Caching logic: only applies if cache_table_name is given.
  if (!is.null(cache_table_name)) {
    if (db_type_check == 'duckdb') {
      stop("Cannot cache a query from a duckdb source to itself.")
    }
    
    # For Postgres/Redshift, use the memory-efficient streaming method.
    if (db_type_check %in% c('postgres', 'redshift')) {
        message(paste("Caching", db_type_check, "query to DuckDB table:", cache_table_name))
        return(.cache_query_result(
          source_conn_name = conn_name,
          source_query = query,
          dest_table_name = cache_table_name,
          overwrite = overwrite
        ))
    } else if (db_type_check == 'snowflake') {
        # For Snowflake, we fetch in batches and write to DuckDB.
        message(paste("Caching Snowflake query to DuckDB table:", cache_table_name))
        
        con <- .get_db_connection(conn_name = conn_name, db_type = db_type)
        on.exit(con$close(), add = TRUE)
        
        duckdb_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "analytics.duckdb")
        on.exit(DBI::dbDisconnect(duckdb_con, shutdown = TRUE), add = TRUE)

        cursor <- con$cursor()
        cursor$execute(query)
        
        # Streaming fetch using batches if available; fallback to full fetch.
        wrote_any <- FALSE
    if (reticulate::py_has_attr(cursor, "fetch_pandas_batches")) {
      gen <- cursor$fetch_pandas_batches()
      for (batch in reticulate::iterate(gen)) {
        if (is.null(batch) || nrow(batch) == 0) next
        if (!wrote_any) {
          DBI::dbWriteTable(duckdb_con, cache_table_name, batch, overwrite = TRUE)
          wrote_any <- TRUE
        } else {
          DBI::dbWriteTable(duckdb_con, cache_table_name, batch, append = TRUE)
        }
      }
    } else {
      # Fallback: connector lacks batch iterator; fetch all at once
      all_df <- cursor$fetch_pandas_all()
      if (nrow(all_df) > 0) {
        DBI::dbWriteTable(duckdb_con, cache_table_name, all_df, overwrite = TRUE)
        wrote_any <- TRUE
      }
    }
    if (!wrote_any && overwrite) {
      # Create an empty table placeholder (no rows) if nothing was returned
      DBI::dbExecute(duckdb_con, paste0("CREATE OR REPLACE TABLE ", cache_table_name, " AS SELECT * FROM (SELECT 1) WHERE 1=0;"))
    }
        
        cursor$close()
        
        row_count_df <- DBI::dbGetQuery(duckdb_con, sprintf("SELECT COUNT(*) AS n FROM %s", cache_table_name))
        row_count <- row_count_df$n
        
        message(sprintf("Successfully cached %s rows to DuckDB table '%s' (analytics.duckdb).", row_count, cache_table_name))
        return(invisible(row_count))
    }

  } else {
    # Standard query-to-dataframe logic for ALL databases.
    # This block handles all non-caching queries.

    # Get connection using the helper function
    con <- .get_db_connection(conn_name = conn_name, db_type = db_type, config_path = "~/snowquery_creds.yaml")
    
    is_snowflake_conn <- "snowflake.connector.connection.SnowflakeConnection" %in% class(con)

    # Ensure connection is closed on exit
    if (is_snowflake_conn) {
      on.exit(con$close(), add = TRUE)
    } else {
      on.exit(DBI::dbDisconnect(con), add = TRUE)
    }

    # Execute query and fetch results
    if (is_snowflake_conn) {
      cursor <- con$cursor()
      cursor$execute(query)
      df <- cursor$fetch_pandas_all()
      cursor$close()
      # check if any df columns are a list and convert to character (issue with pandas df to R df conversion)
      df[] <- lapply(df, function(x) if(is.list(x)) sapply(x, paste, collapse = ",") else x)
    } else {
      res <- DBI::dbSendQuery(con, query)
      df <- DBI::dbFetch(res)
      DBI::dbClearResult(res)
    }
    
    return(df)
  }
}
