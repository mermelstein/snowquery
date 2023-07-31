# Snowquery

<!-- badges: start -->
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/snowquery)](https://cran.r-project.org/package=snowquery)
<!-- badges: end -->

## Overview

Run SQL queries on Snowflake, Redshift, or a postgres database from an R script. 

This package is designed to make it easy to run SQL queries from R. It is designed to work with Snowflake, Redshift, or a postgres database. It is not designed to work with other databases, but it could be extended to do so.

### Installation

```r
# The easiest way to get snowquery
install.packages("snowquery")

# Or you can get the development version from GitHub
# install.packages("devtools")
devtools::install_github("mermelstein/snowquery")
```

### Redshift notes

Redshift is currently only available on the development version of this package. See [installation instructions](#installation) above.

When connecting to a Redshift DWH you might need to specify an SSL connection. You can do this with a `sslmode='require'` connection variable or by passing that to the `queryDB()` function directly.

### Snowflake notes

Because Snowflake's driver requires a ton of fiddling in order to make it work for R. It sucks. A lot.

To sum up the current experience of running SQL against Snowflake from:

  - python: good &#x2705;
  - R: bad &#x274C;

That's why the `snowquery` package takes the [Snowflake python connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas) and leverages it in the background to run queries from R.

### Documentation

For more information on using `snowquery`, please see the [package website](https://snowquery.org).

### Requirements for Use

Redshift and Postgres db connections are entirely contained by this package. If querying Snowflake you must have a local python installation and the Snowflake python connector installed. If you need to install python you can do that with [Homebrew](https://brew.sh/) from the terminal:

```bash
# for example to install python 3.10 on MacOS
brew install python@3.10
```

If you need to install the Snowflake python connector, you can do that with the following command from the terminal:
```bash
pip install "snowflake-connector-python[pandas]"
```

### Credentials

For all db connections you will need to have your database credentials in a YAML file called `snowquery_creds.yaml`. The file should be located in the root directory of your machine and should have the following format (depending on which database type you are connecting to):


```yaml
---
my_snowflake_dwh:
    db_type: snowflake
    account: 
    warehouse: 
    database: 
    username: 
    password: 
    role: 
my_redshift_dwh:
    db_type: redshift
    sslmode: require
    host: 
    port: 
    database: 
    username: 
    password: 
my_postgres_db:
    db_type: postgres
    host: 
    port: 
    database: 
    username: 
    password: 

```

This follows a named connection format, where you can have multiple named connections in the same file. For example you might have a `my_snowflake_dwh` connection and a `my_snowflake_admin` connection, each with their own credentials.

This package looks for the credential file at this location: `~/snowquery_creds.yaml`. **If it is in any other location it will not work.** If the package cannot locate the file you will receive an error like: `cannot open file '/expected/path/to/file/snowquery_creds.yaml': No such file or directory`. You can manually pass credentials to the `queryDB()` function but it is recommended to use the YAML file.

You are now ready to query away!

### Usage

Load this library in your R environment with `library(snowquery)`.

There is one function you need: `queryDB()`. It will take a SQL query as a string parameter and run it on the db.

For example:

```R
library(snowquery)

query <- "SELECT * FROM MY_AWESOME_TABLE"
result_dataframe <- queryDB(query, conn_name='my_snowflake_dwh')
print(result_dataframe)
```

or 

```R
library(snowquery)

queryDB("SELECT * FROM MY_AWESOME_TABLE", conn_name='my_snowflake_dwh')
```

or

```R
library(snowquery)
# You can also pass in credentials manually
result <- queryDB("SELECT * FROM my_table",
                   db_type='snowflake',
                   username='my_username',
                   password='my_password',
                   account='my_account',
                   database='my_database',
                   warehouse='my_warehouse',
                   role='my_role',
                   timeout=30)
print(result)
```