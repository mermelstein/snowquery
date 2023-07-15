#' @keywords internal
"_PACKAGE"

#' This package provides functions for querying Snowflake databases using R.
#' To use this package, you will need to provide your Snowflake credentials in a
#' YAML file called `snowquery_creds.yaml`. The file should be located in the
#' root directory of your R project and should have the following format:
#'
#' ```yaml
#' ---
#' snowflake:
#'     account: 'your_account_name'
#'     warehouse: 'your_warehouse_name'
#'     database: 'your_database_name'
#'     username: 'your_username'
#'     password: 'your_password'
#'     role: 'your_role'
#' 
#' ```
#'
#' Replace the values in the YAML file with your own Snowflake credentials.
#' Once you have created the `snowquery_creds.yaml` file, you can use the
#' `queryDB()` function to query your Snowflake database.
