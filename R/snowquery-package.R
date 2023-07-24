#' @keywords internal
"_PACKAGE"

#' This package provides functions for querying Snowflake, Redshift and Postgres databases using R.
#' To use this package, you will need to provide your database credentials in a
#' YAML file called `snowquery_creds.yaml`. The file should be located in the
#' root directory of your R project and should have the following format:
#'
#' ```yaml
#' ---
#' my_snowflake_dwh:
#'    db_type: 'snowflake' # or 'redshift' or 'postgres'
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
