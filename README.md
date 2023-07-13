# Snowquery

<!-- badges: start -->
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/snowquery)](https://cran.r-project.org/package=snowquery)
<!-- badges: end -->

## Overview

Run SQL queries on a Snowflake instance from an R script. This will be similar to how you might be using DBI or odbc to query a postgres or Redshift database, but because Snowflake's driver requires a ton of fiddling in order to make it work for R this is an alternate solution.

This sums up the current experience of running SQL against Snowflake from:
  - python: good &#x2705;
  - R: bad &#x274C;

That's why the `snowquery` package takes the [Snowflake python connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-pandas) and leverages it in the background to run queries from R.

### Installation

```r
# The easiest way to get snowquery
install.packages("snowquery")

# Or you can get the development version from GitHub
# install.packages("devtools")
devtools::install_github("mermelstein/snowquery")
```

### Requirements for Use

You must have a local python installation and the Snowflake python connector installed. If you need to install python you can do that with Homebrew from the terminal:

```bash
# for example to install python 3.9 on MacOS
brew install python@3.9
```

If you need to install the Snowflake python connector, you can do that with the following command from the terminal:
```bash
pip install "snowflake-connector-python[pandas]"
```

You will also need to have your Snowflake credentials in a YAML file called `snowquery_creds.yaml`. The file should be located in the
root directory of your machine and should have the following format:

```yaml
---
snowflake:
    account: 'your_account_name'
    warehouse: 'your_warehouse_name'
    database: 'your_database_name'
    username: 'your_username'
    password: 'your_password'
    role: 'your_role'

```

The main function of this package looks for that file at this location: `~/snowquery_creds.yaml`. **If it is in any other location it will not work.** If the package cannot locate the file you will receive an error like: `cannot open file '/expected/path/to/file/snowquery_creds.yaml': No such file or directory`.z

You are now ready to query away!

### Usage

Load this library in your R environment with `library(snowquery)`.

There is one function you need: `queryDB()`. It will take a string parameter and run that as a SQL query.

For example:

```R
library(snowquery)

query <- "SELECT * FROM MY_AWESOME_TABLE"
result_dataframe <- queryDB(query)
print(result_dataframe)
```

or 

```R
library(snowquery)

queryDB("SELECT * FROM MY_AWESOME_TABLE")
```