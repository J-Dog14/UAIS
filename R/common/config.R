# UAIS Configuration Management for R
# Loads YAML config and provides database connections

library(yaml)
library(DBI)

#' Load configuration from db_connections.yaml
#' @return List containing database connections and raw data paths
load_config <- function() {
  # Try multiple possible paths to find config file
  # First, try relative path from R/common/
  config_path <- file.path("..", "..", "config", "db_connections.yaml")
  
  # If not found, try from project root (if script is run from R/ subdirectory)
  if (!file.exists(config_path)) {
    config_path <- file.path("config", "db_connections.yaml")
  }
  
  # If still not found, try from current working directory
  if (!file.exists(config_path)) {
    # Try to find config relative to current working directory
    # This works if script is run from project root
    possible_paths <- c(
      file.path(getwd(), "config", "db_connections.yaml"),
      file.path(getwd(), "..", "config", "db_connections.yaml")
    )
    for (path in possible_paths) {
      if (file.exists(path)) {
        config_path <- path
        break
      }
    }
  }
  
  if (!file.exists(config_path)) {
    stop(paste("Config file not found. Tried:", 
               paste(c(file.path("..", "..", "config", "db_connections.yaml"),
                      file.path("config", "db_connections.yaml"),
                      config_path), collapse = ", "),
               "\nCopy db_connections.example.yaml to db_connections.yaml and configure"))
  }
  
  config <- yaml::read_yaml(config_path)
  return(config)
}

#' Get database connection for app database
#' @return DBI connection object
get_app_connection <- function() {
  config <- load_config()
  app_config <- config$databases$app
  
  if (!is.null(app_config$sqlite)) {
    return(DBI::dbConnect(RSQLite::SQLite(), app_config$sqlite))
  } else if (!is.null(app_config$postgres)) {
    pg <- app_config$postgres
    return(DBI::dbConnect(
      RPostgres::Postgres(),
      host = pg$host,
      port = pg$port,
      dbname = pg$database,
      user = pg$user,
      password = pg$password
    ))
  } else {
    stop("No app database configuration found")
  }
}

#' Get database connection for warehouse database
#' @return DBI connection object
get_warehouse_connection <- function() {
  config <- load_config()
  warehouse_config <- config$databases$warehouse
  
  if (!is.null(warehouse_config$sqlite)) {
    return(DBI::dbConnect(RSQLite::SQLite(), warehouse_config$sqlite))
  } else if (!is.null(warehouse_config$postgres)) {
    pg <- warehouse_config$postgres
    return(DBI::dbConnect(
      RPostgres::Postgres(),
      host = pg$host,
      port = pg$port,
      dbname = pg$database,
      user = pg$user,
      password = pg$password
    ))
  } else {
    stop("No warehouse database configuration found")
  }
}

#' Get raw data paths
#' @return Named list of raw data directory paths
get_raw_paths <- function() {
  config <- load_config()
  return(config$raw_data_paths)
}

