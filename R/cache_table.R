#' @title Cache a remote query result to a local DuckDB database
#' @description Efficiently streams the result of a query from a remote source (Snowflake, Redshift, Postgres)
#' to a local DuckDB file. This method is memory-efficient and suitable for very large query results as it
#' streams data without loading the entire result set into R's memory.
#'
#' @param source_conn_name The name of the remote database connection in your snowquery_creds.yaml file.
#' @param source_query The SQL query to execute on the remote source.
#' @param dest_table_name The name of the table to be created in the local DuckDB database.
#' @param overwrite A boolean (TRUE/FALSE) to control whether to overwrite the destination table if it already exists.
#' @param config_path The path to your snowquery_creds.yaml file.
#' @return Invisibly returns a confirmation message.
#' @importFrom dplyr tbl
#' @importFrom dbplyr sql
#' @importFrom duckdb duckdb duckdb_register duckdb_unregister
#' @import DBI
.cache_query_result <- function(source_conn_name, source_query, dest_table_name, overwrite = TRUE, config_path = '~/snowquery_creds.yaml') {
  # Establish connections
  source_con <- .get_db_connection(conn_name = source_conn_name, config_path = config_path)
  on.exit(DBI::dbDisconnect(source_con), add = TRUE)
  
  duckdb_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "analytics.duckdb")
  on.exit(DBI::dbDisconnect(duckdb_con, shutdown = TRUE), add = TRUE)
  
  # Create a lazy table from the source query
  lazy_query <- dplyr::tbl(source_con, dbplyr::sql(source_query))
  
  # Register the lazy query as a temporary view in DuckDB
  duckdb::duckdb_register(duckdb_con, "temp_view", lazy_query)
  
  # Execute the query to transfer data into the destination table
  if (overwrite) {
    DBI::dbExecute(duckdb_con, paste0("CREATE OR REPLACE TABLE ", dest_table_name, " AS SELECT * FROM temp_view;"))
  } else {
    DBI::dbExecute(duckdb_con, paste0("INSERT INTO ", dest_table_name, " SELECT * FROM temp_view;"))
  }
  
  # Unregister the view
  duckdb::duckdb_unregister(duckdb_con, "temp_view")
  
  # Get row count for the confirmation message (alias for reliable column name)
  row_count_df <- DBI::dbGetQuery(duckdb_con, paste0("SELECT COUNT(*) AS n FROM ", dest_table_name))
  row_count <- if (nrow(row_count_df) == 1) row_count_df$n else NA_integer_

  message(sprintf("Successfully cached %s rows to DuckDB table '%s' (analytics.duckdb).", row_count, dest_table_name))
}
