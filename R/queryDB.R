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
#' @param timeout The number of seconds to wait for the database to connect successfully
#' @return A data frame containing the results of the query
#' @examples
#' \dontrun{
#' # Query the database and get a dataframe of results
#' result <- queryDB("SELECT * FROM my_table", conn_name='my_snowflake_dwh')
#' print(result)
#' }
#' \dontrun{
#' # You can also pass in credentials manually
#' result <- queryDB("SELECT * FROM my_table",
#'                    db_type='snowflake',
#'                    username='my_username',
#'                    password='my_password',
#'                    account='my_account',
#'                    database='my_database',
#'                    warehouse='my_warehouse',
#'                    role='my_role',
#'                    timeout=30)
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
  timeout = 15)
{

  check_null <- function(var, default) {
    if (is.null(var)) {
      default
    } else {
      var
    }
  }

  db_type_error_message <- paste0("Invalid db_type '", db_type, "'. \n",
  "Snowquery currently only supports the following database types: 'snowflake', 'redshift' and 'postgres'")

  # pull in the credential file
  snowquery_creds_filepath <- '~/snowquery_creds.yaml'
  snowquery_creds <- yaml::read_yaml(snowquery_creds_filepath,  fileEncoding = "UTF-8")

  # Get the connection details from the snowquery_creds object
  conn_details <- snowquery_creds[[conn_name]]

  # Extract the db_type variable from the connection details
  db_type <- check_null(db_type, check_null(conn_details$db_type, NULL))

  # Check if db_type is provided
  if (missing(db_type) || is.null(db_type)) {
    stop(paste0("db_type is missing for the '", conn_name, "' connection.\n",
    "Please provide a database type to queryDB(). Expected values are 'snowflake', 'redshift' or 'postgres'.\n",
    "You can add a db_type variable to the '", conn_name, "' connection in the snowquery_creds.yaml file or pass it in manually:\n",
    "For example: queryDB('SELECT * FROM my_table', conn_name = '", conn_name, "', db_type = 'snowflake')"))
  }

  if (tolower(db_type) == "snowflake") {
    tryCatch({
      # Find the location of the Python executable and pass to use_python()
      python_executable <- py_config()$python
      use_python(python_executable)
    }, error = function(e) {
      stop(paste0("Failed to find the python executable. Please make sure python 3 is installed and accessible from your environment.\n",
      "You can download Python 3 from https://www.python.org/downloads/ or via Homebrew if on MacOS. \n",
      "After installing Python 3, make sure it is added to your system PATH. \n",
      "Error message: ", e$message))
    })

    tryCatch({
      # Import the snowflake.connector module from the snowflake-connector-python package
      snowflake <- import("snowflake.connector")
    }, error = function(e) {
      stop(paste0("Failed to import the snowflake.connector module. Please make sure it is installed and accessible from your environment. \n",
      "Try running the following command from your terminal or command line:\n\n",
      "pip install 'snowflake-connector-python[pandas]'\n\n",
      "Error message: ", e$message))
    })
    username_ <- check_null(username, check_null(conn_details$user, NULL))
    password_ <- check_null(password, check_null(conn_details$password, NULL))
    account_ <- check_null(account, check_null(conn_details$account, NULL))
    database_ <- check_null(database, check_null(conn_details$database, NULL))
    warehouse_ <- check_null(warehouse, check_null(conn_details$warehouse, NULL))
    role_ <- check_null(role, check_null(conn_details$role, NULL))

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
      stop(paste0("Missing credentials for the Snowflake connection. \n",
      "The following credential variable(s) are missing: ", paste(missing_vars, collapse = ", "), ".\n",
      "Please pass in credentials to queryDB() or add them to the snowquery_creds.yaml file."))
    } else {
      # Use credentials to build connection string
      con <- snowflake$connect(
        user = username_,
        password = password_,
        account = account_,
        database = database_,
        warehouse = warehouse_,
        role = role_,
        login_timeout = timeout # seconds
      )
    }

    # Run the SQL query
    cursor <- con$cursor()
    cursor$execute(query)
    df <- cursor$fetch_pandas_all()
    cursor$close()
    # Disconnect from the database
    con$close()
    # check if any df columns are a list and convert to character (issue with pandas df to R df conversion)
    df[] <- lapply(df, function(x) if(is.list(x)) sapply(x, paste, collapse = ",") else x)
    # Return the query results
    return(df)

  } else if (tolower(db_type) %in% c("postgres", "redshift")) {
    # Check if credentials are provided manually by user
    database_ <- check_null(database, check_null(conn_details$database, NULL))
    username_ <- check_null(username, check_null(conn_details$username, NULL))
    password_ <- check_null(password, check_null(conn_details$password, NULL))
    port_ <- check_null(port, check_null(conn_details$port, NULL))
    host_ <- check_null(host, check_null(conn_details$host, NULL))
    sslmode_ <- check_null(sslmode, check_null(conn_details$sslmode, NULL))
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
      stop(paste0("Missing credentials for the ", db_type, " connection. \n",
      "The following credential variable(s) are missing: ", paste(missing_vars, collapse = ", "), ".\n",
      "Please pass in credentials to queryDB() or add them to the snowquery_creds.yaml file."))
    } else {
      if (tolower(db_type) == "postgres") {
        driver_type <- RPostgres::Postgres()
      } else if (tolower(db_type) == "redshift") {
        driver_type <- RPostgres::Redshift()
      } else {
        stop(db_type_error_message)
      }
      # Use credentials to build connection string
      con <- DBI::dbConnect(driver_type,
          dbname = database_,
          host = host_,
          port = port_,
          user = username_,
          password = password_,
          sslmode = sslmode_,
          connect_timeout = timeout # seconds
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
    stop(db_type_error_message)
  }
}
