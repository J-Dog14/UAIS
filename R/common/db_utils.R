# Database utility functions for UAIS R scripts

library(DBI)
library(dplyr)

#' Read a database table as a tibble
#' @param conn DBI connection object
#' @param table_name Name of the table
#' @param schema Optional schema name (for Postgres)
#' @return Tibble containing table data
read_table <- function(conn, table_name, schema = NULL) {
  if (!is.null(schema)) {
    full_table <- paste(schema, table_name, sep = ".")
  } else {
    full_table <- table_name
  }
  
  return(tbl(conn, full_table) %>% collect())
}

#' Write a data frame to a database table
#' @param df Data frame or tibble to write
#' @param conn DBI connection object
#' @param table_name Target table name
#' @param if_exists Behavior if table exists ('fail', 'replace', 'append')
write_table <- function(df, conn, table_name, if_exists = "append") {
  if (if_exists == "replace") {
    if (dbExistsTable(conn, table_name)) {
      dbRemoveTable(conn, table_name)
    }
  }
  
  dbWriteTable(conn, table_name, df, append = (if_exists == "append"))
}

#' Execute a SQL query and return results
#' @param conn DBI connection object
#' @param query SQL query string
#' @return Tibble with query results
execute_query <- function(conn, query) {
  result <- dbGetQuery(conn, query)
  return(as_tibble(result))
}

#' Check if a table exists
#' @param conn DBI connection object
#' @param table_name Name of the table
#' @return Logical indicating if table exists
table_exists <- function(conn, table_name) {
  return(dbExistsTable(conn, table_name))
}

