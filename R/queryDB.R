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

  check_null <- function(var, default) {
    if (is.null(var)) {
      default
    } else {
      var
    }
  }
  
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

    username_ <- check_null(username, check_null(snowquery_creds$snowflake$user, NULL))
    password_ <- check_null(password, check_null(snowquery_creds$snowflake$password, NULL))
    account_ <- check_null(account, check_null(snowquery_creds$snowflake$account, NULL))
    database_ <- check_null(database, check_null(snowquery_creds$snowflake$database, NULL))
    warehouse_ <- check_null(warehouse, check_null(snowquery_creds$snowflake$warehouse, NULL))
    role_ <- check_null(role, check_null(snowquery_creds$snowflake$role, NULL))
    # Check if any credentials are missing
    if (is.null(username_) || is.null(password_) || is.null(account_) || is.null(database_) || is.null(warehouse_) || is.null(role_)) {
      # Get the names of the missing credential variables
      missing_vars <- c()
      if (is.null(username_)) missing_vars <- c(missing_vars, "username")
      if (is.null(password_)) missing_vars <- c(missing_vars, "password")
      if (is.null(account_)) missing_vars <- c(missing_vars, "account")
      if (is.null(database_)) missing_vars <- c(missing_vars, "database")
      if (is.null(warehouse_)) missing_vars <- c(missing_vars, "warehouse")
      if (is.null(role_)) missing_vars <- c(missing_vars, "role")
      # Error message if credentials are missing
      stop(paste0("Missing credentials for Snowflake. \n",
      "The following credential variable(s) are missing: ", paste(missing_vars, collapse = ", "), ".\n",
      "Please pass in credentials to queryDB() or add them to the snowquery_creds.yaml file."))
    } else {
      # Use available credentials to build connection string
      con <- snowflake$connect(
        user = username_,
        password = password_,
        account = account_,
        database = database_,
        warehouse = warehouse_,
        role = role_
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
    database_ <- check_null(database, check_null(snowquery_creds$postgres$database, NULL))
    username_ <- check_null(username, check_null(snowquery_creds$postgres$username, NULL))
    password_ <- check_null(password, check_null(snowquery_creds$postgres$password, NULL))
    port_ <- check_null(port, check_null(snowquery_creds$postgres$port, NULL))
    host_ <- check_null(host, check_null(snowquery_creds$postgres$host, NULL))
    # Check if any credentials are missing
    if (is.null(username_) || is.null(password_) || is.null(host_) || is.null(database_) || is.null(port_)) {
      # Get the names of the missing credential variables
      missing_vars <- c()
      if (is.null(database_)) missing_vars <- c(missing_vars, "database")
      if (is.null(username_)) missing_vars <- c(missing_vars, "username")
      if (is.null(password_)) missing_vars <- c(missing_vars, "password")
      if (is.null(port_)) missing_vars <- c(missing_vars, "port")
      if (is.null(host_)) missing_vars <- c(missing_vars, "host")
      # Error message if credentials are missing
      stop(paste0("Missing credentials for Postgres. \n",
      "The following credential variable(s) are missing: ", paste(missing_vars, collapse = ", "), ".\n",
      "Please pass in credentials to queryDB() or add them to the snowquery_creds.yaml file."))
    } else {
      # Use available credentials to build connection string
      con <- DBI::dbConnect(RPostgres::Postgres(),
          dbname = database_,
          host = host_,
          port = port_,
          user = username_,
          password = password_
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
    "Snowquery currently only supports 'snowflake' and 'postgres' database types."))
  }
}
