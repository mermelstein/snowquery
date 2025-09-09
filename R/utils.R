.ensure_snowflake_connector <- function(min_version = "2.7.4") {
  # Single-responsibility helper: ensure the Python snowflake connector
  # (with pandas extras) is installed, at or above min_version, and imported.
  suppressWarnings({
    # 1. Install if missing
    if (!reticulate::py_module_available("snowflake.connector")) {
      message("snowflake.connector not available. Installing...")
      tryCatch({
        reticulate::py_install("snowflake-connector-python[pandas]", pip = TRUE)
      }, error = function(e) {
        stop("Installation of 'snowflake-connector-python' failed: ", e$message)
      })
    }

    # 2. Import (fail hard if not importable)
    snowflake <- tryCatch(
      reticulate::import("snowflake.connector", delay_load = FALSE),
      error = function(e) {
        stop("Failed to import 'snowflake.connector' after installation attempt: ", e$message)
      }
    )

    # 3. Version check & upgrade if too old
    version_raw <- tryCatch(reticulate::py_get_attr(snowflake, "__version__"), error = function(e) NA_character_)
    version_str <- {
      if (is.null(version_raw) || (length(version_raw) == 1 && is.na(version_raw))) {
        NA_character_
      } else {
        as.character(version_raw)[1]
      }
    }
    if (!is.na(version_str) && nzchar(version_str)) {
      # Normalize version (strip possible build/meta tags)
      normalized <- sub("[+].*$", "", version_str)
      if (suppressWarnings(utils::compareVersion(normalized, min_version)) < 0) {
        message(sprintf(
          "snowflake-connector-python version %s < required %s. Upgrading...",
          version_str, min_version
        ))
        tryCatch({
          reticulate::py_install("snowflake-connector-python[pandas]", pip = TRUE, pip_options = "--upgrade")
        }, error = function(e) {
          stop("Upgrade of 'snowflake-connector-python' failed: ", e$message)
        })
        snowflake <- reticulate::import("snowflake.connector", delay_load = FALSE, force = TRUE)
        message("Upgrade complete.")
      }
    } else {
      message("Could not determine snowflake.connector version; skipping version enforcement.")
    }

    snowflake
  })
}

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
    
    # Get the snowflake module using the helper function
    snowflake <- .ensure_snowflake_connector()
    
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
