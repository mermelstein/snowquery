.get_db_connection <- function(conn_name, db_type = NULL, config_path = "~/snowquery_creds.yaml") {
  
  check_null <- function(var, default) {
    if (is.null(var)) {
      default
    } else {
      var
    }
  }

  snowquery_creds <- yaml::read_yaml(config_path, fileEncoding = "UTF-8")
  conn_details <- snowquery_creds[[conn_name]]
  
  # Determine db_type: prioritize function argument, then YAML file.
  db_type_check <- if (!is.null(db_type)) {
    tolower(db_type)
  } else if (!is.null(conn_details$db_type)) {
    tolower(conn_details$db_type)
  } else {
    stop(paste0("db_type is missing for the '", conn_name, "' connection."))
  }

  db_type_error_message <- paste0("Invalid db_type '", db_type_check, "'. \n",
                                  "Snowquery currently supports: 'snowflake', 'redshift', 'postgres', 'sqlite', and 'duckdb'")

  if (db_type_check == "snowflake") {
    tryCatch({
      python_executable <- reticulate::py_config()$python
      reticulate::use_python(python_executable)
    }, error = function(e) {
      stop("Failed to find python executable. ", e$message)
    })
    # Suppress warnings about ephemeral reticulate environments
    suppressWarnings({
        tryCatch({
            snowflake <- reticulate::import("snowflake.connector")
        }, error = function(e) {
            message("Failed to import snowflake.connector. Attempting to install...")
            tryCatch({
                reticulate::py_install("snowflake-connector-python[pandas]", pip = TRUE)
                # Assign to the parent environment
                snowflake <<- reticulate::import("snowflake.connector")
                message("Successfully installed and imported snowflake.connector.")
            }, error = function(e_install) {
                stop(paste0("Failed to install the 'snowflake-connector-python' package. ",
                            "Please try installing it manually by running 'pip install \"snowflake-connector-python[pandas]\"' in your terminal. ",
                            "Installation error: ", e_install$message))
            })
        })
    })
    
    username_ <- check_null(conn_details$user, NULL)
    password_ <- check_null(conn_details$password, NULL)
    account_ <- check_null(conn_details$account, NULL)
    database_ <- check_null(conn_details$database, NULL)
    warehouse_ <- check_null(conn_details$warehouse, NULL)
    role_ <- check_null(conn_details$role, NULL)

    if (is.null(username_) || is.null(password_) || is.null(account_)) {
      stop("Missing credentials for Snowflake. Check user, password, and account.")
    }
    
    con <- snowflake$connect(
      user = username_,
      password = password_,
      account = account_,
      database = database_,
      warehouse = warehouse_,
      role = role_
    )
    return(con)

  } else if (db_type_check %in% c("postgres", "redshift")) {
    database_ <- check_null(conn_details$database, NULL)
    username_ <- check_null(conn_details$username, NULL)
    password_ <- check_null(conn_details$password, NULL)
    port_ <- check_null(conn_details$port, NULL)
    host_ <- check_null(conn_details$host, NULL)
    sslmode_ <- check_null(conn_details$sslmode, NULL)

    if (is.null(username_) || is.null(password_) || is.null(host_) || is.null(database_) || is.null(port_)) {
      stop("Missing credentials for ", db_type_check, ". Check database, username, password, port, and host.")
    }
    
    if (db_type_check == "postgres") {
      driver_type <- RPostgres::Postgres()
    } else {
      driver_type <- RPostgres::Redshift()
    }
    
    con <- DBI::dbConnect(driver_type,
                          dbname = database_,
                          host = host_,
                          port = port_,
                          user = username_,
                          password = password_,
                          sslmode = sslmode_)
    return(con)

  } else if (db_type_check == "sqlite") {
    database_ <- check_null(conn_details$database, NULL)
    if (is.null(database_)) {
      stop("Database file path is missing for SQLite connection.")
    }
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = database_)
    return(con)

  } else if (db_type_check == "duckdb") {
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "analytics.duckdb", read_only = TRUE)
    return(con)
  } else {
    stop(db_type_error_message)
  }
}
