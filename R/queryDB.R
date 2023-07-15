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
#' @import RPostgres
#' @import DBI
#' @importFrom reticulate import use_python
#' @importFrom reticulate import import
#' @export
queryDB <- function(
  query,
  db_type,
  username = NULL,
  password = NULL,
  host = NULL,
  port = NULL,
  database = NULL,
  warehouse = NULL,
  account = NULL,
  role = NULL)
{
  # Check if db_type is provided
  if (missing(db_type)) {
    stop(paste0("db_type is missing. Expected 'postgres' or 'snowflake'.\n",
    "Please provide a db_type argument to queryDB(). \n",
    "For example: queryDB('SELECT * FROM my_table', db_type = 'snowflake')"))
  }

  # pull in the credential file
  snowquery_creds_filepath <- '~/snowquery_creds.yaml'
  snowquery_creds <- yaml::read_yaml(snowquery_creds_filepath,  fileEncoding = "UTF-8")

  if (tolower(db_type) == "snowflake") {
    # Find the location of the Python executable and pass to use_python()
    python_executable <- py_config()$python
    use_python(python_executable)

    # Import the snowflake.connector module from the snowflake-connector-python package
    snowflake <- import("snowflake.connector")

    # Check if credentials are provided manually by user
    if (is.null(username) || is.null(password) || is.null(account) || is.null(database) || is.null(warehouse) || is.null(role)) {
      # Check if credentials are available in the credential file
      if (is.null(snowquery_creds$snowflake$user) || is.null(snowquery_creds$snowflake$password) || is.null(snowquery_creds$snowflake$account) || is.null(snowquery_creds$snowflake$database) || is.null(snowquery_creds$snowflake$warehouse) || is.null(snowquery_creds$snowflake$role)) {
        stop(paste0("Missing credentials for Snowflake. \n",
        "Please pass in credentials to queryDB() or add them to the snowquery_creds.yaml file."))
      } else {
        # Use credentials in yaml file to build connection
        con <- snowflake$connect(
          user = snowquery_creds$snowflake$user,
          password = snowquery_creds$snowflake$password,
          account = snowquery_creds$snowflake$account,
          database = snowquery_creds$snowflake$database,
          warehouse = snowquery_creds$snowflake$warehouse,
          role = snowquery_creds$snowflake$role
        )
      }
    } else {
      # Use credentials passed by user to build connection string
      con <- snowflake$connect(
        user = username,
        password = password,
        account = account,
        database = database,
        warehouse = warehouse,
        role = role
      )
    }

    # Run the SQL query
    cursor <- con$cursor()
    cursor$execute(query)
    df <- cursor$fetch_pandas_all()
    cursor$close()
    # Disconnect from the database
    con$close()
    # Return the query results
    return(df)

  } else if (tolower(db_type) == "postgres") {
    # Check if credentials are provided manually by user
    if (is.null(username) || is.null(password) || is.null(host) || is.null(port) || is.null(database)) {
      # Check if credentials are available in the credential file
      if (is.null(snowquery_creds$postgres$username) || is.null(snowquery_creds$postgres$password) || is.null(snowquery_creds$postgres$database) || is.null(snowquery_creds$postgres$host) || is.null(snowquery_creds$postgres$port)) {
        stop(paste0("Missing credentials for Postgres. \n",
        "Please pass in credentials to queryDB() or add them to the snowquery_creds.yaml file."))
      } else {
        # Use credentials in yaml file to build connection
        con <- DBI::dbConnect(RPostgres::Postgres(),
          dbname = snowquery_creds$postgres$database,
          host = snowquery_creds$postgres$host,
          port = snowquery_creds$postgres$port,
          user = snowquery_creds$postgres$username,
          password = snowquery_creds$postgres$password
        )
      }
    } else {
      # Use credentials passed by user to build connection string
      con <- DBI::dbConnect(RPostgres::Postgres(),
        user = username,
        password = password,
        account = account,
        database = database,
        warehouse = warehouse,
        role = role
      )
    }

    # Run the SQL query
    res <- DBI::dbSendQuery(con, query)
    df <- DBI::dbFetch(res)
    DBI::dbClearResult(res)
    # Disconnect from the database
    DBI::dbDisconnect(con)
    # Return the query results
    return(df)

  } else {
    stop(paste0("Invalid db_type '", db_type, "'. \n",
    "Snowquery currently only supports 'snowflake' and 'postgres' databases."))
  }
}
