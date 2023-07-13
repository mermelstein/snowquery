#' @title Query a Snowflake database
#' @description Run a SQL query on a Snowflake database (requires a `~/snowquery_creds.yaml` file)
#'
#' @param query A string of the SQL query to execute
#' @return A data frame containing the results of the query
#' @examples
#' \dontrun{
#' # Query the database and get a dataframe of results
#' result <- queryDB("SELECT * FROM my_table")
#' print(result)
#' }
#'
#' @import yaml
#' @import reticulate
#' @importFrom reticulate import use_python
#' @importFrom reticulate import import
#' @export
queryDB <- function(query) {

  # pull in the credential file
  snowquery_creds_filepath <- '~/snowquery_creds.yaml'
  snowquery_creds <- yaml::read_yaml(snowquery_creds_filepath,  fileEncoding = "UTF-8")

  # Find the location of the Python executable and pass to use_python()
  python_executable <- py_config()$python
  use_python(python_executable)

  # Import the snowflake.connector module from the snowflake-connector-python package
  snowflake <- import("snowflake.connector")

  # Connect to the Snowflake database
  con <- snowflake$connect(
    user = snowquery_creds$snowflake$user,
    password = snowquery_creds$snowflake$password,
    account = snowquery_creds$snowflake$account,
    database = snowquery_creds$snowflake$database,
    warehouse = snowquery_creds$snowflake$warehouse,
    role = snowquery_creds$snowflake$role
  )

  # Run a SQL query
  cursor <- con$cursor()
  cursor$execute(query)
  df <- cursor$fetch_pandas_all()
  cursor$close()

  # Disconnect from the database
  con$close()

  # Return the query results
  return(df)
}
