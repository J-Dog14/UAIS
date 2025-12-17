# Runs all Pitching Data
# This file processes all pitching data from a specified directory.
# For interactive folder selection, use main.R instead.

# ==== Minimal deps ====
library(xml2)
library(purrr)
library(dplyr)
library(readr)
library(stringr)
library(tibble)
library(tidyr)
library(DBI)
library(RSQLite)
# Try to load RPostgres, fall back to RPostgreSQL if not available
if (!requireNamespace("RPostgres", quietly = TRUE)) {
  if (requireNamespace("RPostgreSQL", quietly = TRUE)) {
    library(RPostgreSQL)
    # RPostgreSQL uses different connection function, we'll handle this in get_warehouse_connection
  } else {
    warning("Neither RPostgres nor RPostgreSQL is installed. Install with: install.packages('RPostgres')")
  }
} else {
  library(RPostgres)
}
library(uuid)
library(tools)

# Load common utilities
# Try multiple paths to find the common utilities
find_and_source_common <- function() {
  # Try paths relative to current working directory
  possible_config_paths <- c(
    file.path(getwd(), "R", "common", "config.R"),
    file.path(getwd(), "..", "R", "common", "config.R"),
    file.path("..", "common", "config.R"),
    file.path("..", "..", "R", "common", "config.R"),
    file.path(dirname(getwd()), "R", "common", "config.R")
  )
  
  config_path <- NULL
  for (path in possible_config_paths) {
    if (file.exists(path)) {
      config_path <- normalizePath(path)
      break
    }
  }
  
  if (is.null(config_path)) {
    stop("Could not find R/common/config.R. Current working directory: ", getwd())
  }
  
  # Source config.R
  source(config_path)
  
  # Find and source db_utils.R in the same directory
  db_utils_path <- file.path(dirname(config_path), "db_utils.R")
  if (file.exists(db_utils_path)) {
    source(db_utils_path)
  } else {
    warning("Could not find db_utils.R at: ", db_utils_path)
  }
}

find_and_source_common()

# ---------- Helpers ----------
# Progress logging helper (timestamp removed for cleaner output)
log_progress <- function(...) {
  # Use do.call to handle arguments properly, including any sep= arguments
  args <- list(...)
  # Remove sep if present (we'll ignore it)
  args <- args[names(args) != "sep"]
  message <- do.call(paste0, args)
  # Force output immediately
  cat(message, "\n", sep = "")
  flush.console()  # Force output to console immediately
}

`%||%` <- function(a, b) if (!is.null(a)) a else b
nzchr <- function(x) ifelse(is.na(x) | x == "", NA_character_, x)
nznum <- function(x) suppressWarnings(readr::parse_number(x))

# Load athlete manager
athlete_manager_paths <- c(
  file.path("..", "common", "athlete_manager.R"),
  file.path(getwd(), "R", "common", "athlete_manager.R"),
  file.path("R", "common", "athlete_manager.R"),
  file.path(dirname(getwd()), "R", "common", "athlete_manager.R")
)
athlete_manager_loaded <- FALSE
for (path in athlete_manager_paths) {
  if (file.exists(path)) {
    source(path)
    athlete_manager_loaded <- TRUE
    log_progress("Loaded athlete_manager.R from:", path)
    break
  }
}
if (!athlete_manager_loaded) {
  warning("Could not find athlete_manager.R - warehouse integration will be limited")
}

# ---------- Configuration ----------
# Set to NULL to use current directory, or specify path
DATA_ROOT <- "H:/Pitching/Data"  # Set to your pitching data directory path, NULL for testing with local files
USE_WAREHOUSE <- TRUE  # Set to TRUE to write to PostgreSQL warehouse, FALSE for local SQLite
DB_FILE <- "pitching_data.db"  # Only used if USE_WAREHOUSE = FALSE
TRUNCATE_BEFORE_INSERT <- FALSE  # Set to TRUE to clear table before inserting (prevents duplicates on re-run)

# ---------- Name normalization and UUID matching ----------
#' Normalize name for matching: convert LAST, FIRST to FIRST LAST and remove dates
#' @param name Name string (can be "LAST, FIRST" or "LAST, FIRST DATE")
#' @return Normalized name string "FIRST LAST" (uppercase, trimmed)
normalize_name_for_matching <- function(name) {
  if (is.na(name) || name == "") return(NA_character_)
  
  # Remove dates (patterns like "2024-01-15", "1/15/2024", "01-15-2024", etc.)
  # Remove 4-digit years and date patterns
  name <- gsub("\\s*\\d{1,2}[/-]\\d{1,2}[/-]\\d{2,4}", "", name)  # Dates like 1/15/2024 or 01-15-2024
  name <- gsub("\\s*\\d{4}[/-]\\d{1,2}[/-]\\d{1,2}", "", name)  # Dates like 2024-01-15
  name <- gsub("\\s*\\d{4}", "", name)  # Standalone 4-digit years
  
  # Trim whitespace
  name <- trimws(name)
  
  # Handle "LAST, FIRST" format - convert to "FIRST LAST"
  if (grepl(",", name)) {
    parts <- strsplit(name, ",")[[1]]
    if (length(parts) == 2) {
      last <- trimws(parts[1])
      first <- trimws(parts[2])
      name <- paste(first, last)
    }
  }
  
  # Convert to uppercase and remove extra spaces for consistent matching
  name <- toupper(gsub("\\s+", " ", trimws(name)))
  
  return(name)
}

#' Fetch athletes from app database and create name-to-UUID mapping
#' @param conn Optional database connection (defaults to app connection)
#' @return Named list mapping normalized names to UUIDs
fetch_athlete_uuid_map <- function(conn = NULL) {
  log_progress("=== fetch_athlete_uuid_map START ===")
  
  if (is.null(conn)) {
    log_progress("No connection provided, attempting to get app connection...")
    conn <- tryCatch({
      log_progress("Calling get_app_connection()...")
      app_conn <- get_app_connection()
      log_progress("SUCCESS: Got app connection")
      app_conn
    }, error = function(e) {
      log_progress("ERROR: Could not connect to app database!")
      log_progress("Error message:", conditionMessage(e))
      log_progress("Will generate new UUIDs for athletes")
      return(NULL)
    })
    if (!is.null(conn)) {
      on.exit({
        log_progress("Disconnecting from app database...")
        tryCatch(DBI::dbDisconnect(conn), error = function(e) NULL)
      })
    }
  }
  
  if (is.null(conn)) {
    log_progress("Connection is NULL, returning empty map")
    return(list())
  }
  
  log_progress("Connection established, proceeding to query...")
  
  # Try to fetch from athletes table first, then try User table
  athletes_df <- NULL
  
  # Try athletes table
  if (table_exists(conn, "athletes")) {
    tryCatch({
      athletes_df <- read_table(conn, "athletes")
      log_progress("Loaded", nrow(athletes_df), "athletes from 'athletes' table")
    }, error = function(e) {
      log_progress("Could not read from 'athletes' table:", conditionMessage(e))
    })
  }
  
  # Try User table (Postgres schema) - use direct SQL query with quoted identifier
  log_progress("Checking if athletes_df is null or empty...")
  log_progress("athletes_df is null:", is.null(athletes_df))
  if (!is.null(athletes_df)) {
    log_progress("athletes_df has", nrow(athletes_df), "rows")
  }
  
  if (is.null(athletes_df) || nrow(athletes_df) == 0) {
    log_progress("=== ATTEMPTING TO QUERY USER TABLE ===")
    log_progress("Connection class:", class(conn))
    log_progress("Connection valid:", !is.null(conn))
    
    tryCatch({
      # Try querying public."User" table directly (quoted identifier for reserved word)
      query <- 'SELECT uuid AS athlete_uuid, name FROM public."User"'
      log_progress("QUERY 1: Trying:", query)
      athletes_df <- DBI::dbGetQuery(conn, query)
      log_progress("QUERY 1 RESULT: Got", nrow(athletes_df), "rows")
      if (nrow(athletes_df) > 0) {
        log_progress("*** SUCCESS: Loaded", nrow(athletes_df), "athletes from public.\"User\" table ***")
        log_progress("Sample names:", paste(head(athletes_df$name, 5), collapse = ", "))
        log_progress("Sample UUIDs:", paste(head(athletes_df$athlete_uuid, 5), collapse = ", "))
      } else {
        log_progress("QUERY 1: Query succeeded but returned 0 rows")
      }
    }, error = function(e) {
      log_progress("*** QUERY 1 ERROR:", conditionMessage(e), "***")
      # Try without schema prefix
      tryCatch({
        query <- 'SELECT uuid AS athlete_uuid, name FROM "User"'
        log_progress("QUERY 2: Trying:", query)
        athletes_df <- DBI::dbGetQuery(conn, query)
        log_progress("QUERY 2 RESULT: Got", nrow(athletes_df), "rows")
        if (nrow(athletes_df) > 0) {
          log_progress("*** SUCCESS: Loaded", nrow(athletes_df), "athletes from \"User\" table ***")
          log_progress("Sample names:", paste(head(athletes_df$name, 5), collapse = ", "))
          log_progress("Sample UUIDs:", paste(head(athletes_df$athlete_uuid, 5), collapse = ", "))
        } else {
          log_progress("QUERY 2: Query succeeded but returned 0 rows")
        }
      }, error = function(e2) {
        log_progress("*** QUERY 2 ERROR:", conditionMessage(e2), "***")
        log_progress("*** CRITICAL: Could not read from 'User' table - UUID matching will fail ***")
      })
    })
  } else {
    log_progress("athletes_df already has data, skipping User table query")
  }
  
  if (is.null(athletes_df) || nrow(athletes_df) == 0) {
    log_progress("*** CRITICAL: No athletes found in app database - returning empty map ***")
    return(list())
  }
  
  log_progress("*** Processing", nrow(athletes_df), "athletes from database ***")
  
  # Create mapping: normalized_name -> UUID
  uuid_map <- list()
  for (i in seq_len(nrow(athletes_df))) {
    athlete_name <- athletes_df$name[i]
    athlete_uuid <- athletes_df$athlete_uuid[i]
    
    if (!is.na(athlete_name) && !is.na(athlete_uuid)) {
      normalized <- normalize_name_for_matching(athlete_name)
      if (!is.na(normalized)) {
        # If multiple athletes have same normalized name, keep the first one
        # (could be improved with DOB matching later)
        if (!normalized %in% names(uuid_map)) {
          uuid_map[[normalized]] <- as.character(athlete_uuid)
        } else {
          log_progress("  [WARNING] Duplicate normalized name:", normalized, "- keeping first UUID")
        }
      } else {
        log_progress("  [WARNING] Could not normalize name:", athlete_name)
      }
    } else {
      log_progress("  [WARNING] Skipping athlete with missing name or UUID")
    }
  }
  
  log_progress("*** Created UUID mapping for", length(uuid_map), "unique normalized names ***")
  # Debug: show a few examples
  if (length(uuid_map) > 0) {
    example_names <- head(names(uuid_map), min(10, length(uuid_map)))
    log_progress("  Example mappings:")
    for (nm in example_names) {
      log_progress("    '", nm, "' -> '", uuid_map[[nm]], "'")
    }
  } else {
    log_progress("  *** WARNING: UUID map is EMPTY! ***")
  }
  
  log_progress("=== fetch_athlete_uuid_map END ===")
  return(uuid_map)
}

#' Get UUID for an athlete by matching normalized name
#' @param name Athlete name (can be "LAST, FIRST" format)
#' @param uuid_map Named list mapping normalized names to UUIDs
#' @return UUID string or NA if not found
get_uuid_by_name <- function(name, uuid_map) {
  if (length(uuid_map) == 0) return(NA_character_)
  
  normalized <- normalize_name_for_matching(name)
  if (is.na(normalized)) return(NA_character_)
  
  if (normalized %in% names(uuid_map)) {
    return(uuid_map[[normalized]])
  }
  
  return(NA_character_)
}
read_xml_robust <- function(path) {
  tryCatch(
    {
      if (grepl("\\.gz$", path, ignore.case = TRUE)) {
        txt <- paste(readLines(gzfile(path), warn = FALSE), collapse = "\n")
        read_xml(txt)
      } else {
        read_xml(path)
      }
    },
    error = function(e) {
      txt <- if (grepl("\\.gz$", path, ignore.case = TRUE)) {
        paste(readLines(gzfile(path), warn = FALSE), collapse = "\n")
      } else {
        readr::read_file(path)
      }
      end_tag <- "</Subject>"
      m <- regexpr(end_tag, txt, fixed = TRUE)
      if (m > 0) {
        trimmed <- substr(txt, 1, m + nchar(end_tag) - 1)
        read_xml(trimmed)
      } else {
        # Try v3d root
        end_tag <- "</v3d>"
        m <- regexpr(end_tag, txt, fixed = TRUE)
        if (m > 0) {
          trimmed <- substr(txt, 1, m + nchar(end_tag) - 1)
          read_xml(trimmed)
        } else stop(e)
      }
    }
  )
}

# ---------- Extract athlete info from session.xml ----------
extract_athlete_info <- function(path) {
  doc <- tryCatch(read_xml_robust(path), error = function(e) NULL)
  if (is.null(doc)) return(NULL)
  
  root <- xml_root(doc)
  if (!identical(xml_name(root), "Subject")) return(NULL)
  
  # Extract subject fields
  fields <- xml_find_first(root, "./Fields")
  if (inherits(fields, "xml_missing")) return(NULL)
  
  # Core fields
  id <- nzchr(xml_text(xml_find_first(fields, "./ID")))
  name <- nzchr(xml_text(xml_find_first(fields, "./Name")))
  dob <- nzchr(xml_text(xml_find_first(fields, "./Date_of_birth")))
  gender <- nzchr(xml_text(xml_find_first(fields, "./Gender")))
  height <- nzchr(xml_text(xml_find_first(fields, "./Height")))
  weight <- nzchr(xml_text(xml_find_first(fields, "./Weight")))
  creation_date <- nzchr(xml_text(xml_find_first(fields, "./Creation_date")))
  creation_time <- nzchr(xml_text(xml_find_first(fields, "./Creation_time")))
  
  # Extract all other fields (excluding core fields to avoid duplicates)
  core_field_names <- c("ID", "Name", "Date_of_birth", "Gender", "Height", "Weight", 
                        "Creation_date", "Creation_time")
  all_fields <- xml_children(fields)
  field_list <- list()
  for (f in all_fields) {
    fname <- xml_name(f)
    # Skip core fields that we've already extracted
    if (fname %in% core_field_names) next
    fval <- nzchr(trimws(xml_text(f)))
    if (!is.na(fval) && fval != "") {
      field_list[[fname]] <- fval
    }
  }
  
  # Calculate age if DOB available (current age)
  age <- NA_real_
  dob_date <- NA
  if (!is.na(dob) && dob != "") {
    tryCatch({
      dob_date <- as.Date(dob, format = "%m/%d/%Y")
      if (!is.na(dob_date)) {
        age <- as.numeric(difftime(Sys.Date(), dob_date, units = "days")) / 365.25
      }
    }, error = function(e) NULL)
  }
  
  # Calculate age_at_collection based on creation_date
  age_at_collection <- NA_real_
  if (!is.na(creation_date) && creation_date != "" && !is.na(dob_date)) {
    tryCatch({
      # Try to parse creation_date (format might vary)
      collection_date <- as.Date(creation_date, format = "%m/%d/%Y")
      if (is.na(collection_date)) {
        # Try alternative format
        collection_date <- as.Date(creation_date, format = "%Y-%m-%d")
      }
      if (!is.na(collection_date) && !is.na(dob_date)) {
        age_at_collection <- as.numeric(difftime(collection_date, dob_date, units = "days")) / 365.25
        # Log for debugging if age seems wrong
        if (!is.na(age_at_collection) && (age_at_collection < 10 || age_at_collection > 50)) {
          log_progress("  [WARNING] Calculated age_at_collection seems unusual:", round(age_at_collection, 2))
          log_progress("    DOB:", dob, "| Creation date:", creation_date, "| Calculated age:", round(age_at_collection, 2))
        }
      }
    }, error = function(e) NULL)
  }
  
  tibble(
    athlete_id = id,
    name = name,
    date_of_birth = dob,
    age = age,
    age_at_collection = age_at_collection,
    gender = gender,
    height = nznum(height),
    weight = nznum(weight),
    creation_date = creation_date,
    creation_time = creation_time,
    source_file = basename(path),
    source_path = path,
    !!!field_list
  )
}

# ---------- Extract time series data from session_data.xml ----------
parse_comma_data <- function(data_str) {
  if (is.na(data_str) || data_str == "") return(numeric(0))
  vals <- strsplit(data_str, ",", fixed = TRUE)[[1]]
  suppressWarnings(as.numeric(trimws(vals)))
}

# ---------- Extract ALL metric variables from session_data.xml ----------
extract_metric_data <- function(doc, owner_name) {
  root <- xml_root(doc)
  if (!identical(xml_name(root), "v3d")) {
    return(tibble())
  }
  
  owners <- xml_find_all(root, "./owner")
  if (!length(owners)) {
    return(tibble())
  }
  
  all_data <- list()
  folders_seen <- character()
  fastball_folders_found <- 0
  skipped_folders <- character()
  
  for (own in owners) {
    own_val <- xml_attr(own, "value")
    if (own_val != owner_name) next
    
    metric_types <- xml_find_all(own, "./type[@value='METRIC']")
    if (!length(metric_types)) next
    
    for (mt in metric_types) {
      folders <- xml_find_all(mt, "./folder")
      for (fol in folders) {
        folder_val <- xml_attr(fol, "value")
        
        # Track all folders we see
        if (!is.na(folder_val)) {
          folders_seen <- c(folders_seen, folder_val)
        }
        
        # Skip AT_EVENT folder
        if (!is.na(folder_val) && folder_val == "AT_EVENT") {
          next
        }
        
        # NOTE: We no longer filter by folder name - folders are metric categories (PROCESSED, BALLSPEED, etc.)
        # Trial type filtering is done at the owner level (Fastball vs Static)
        # Just skip AT_EVENT folder as before
        if (!is.na(folder_val) && folder_val == "AT_EVENT") {
          next
        }
        
        # Track folders for debugging
        if (!is.na(folder_val)) {
          folders_seen <- c(folders_seen, folder_val)
        }
        
        names <- xml_find_all(fol, "./name")
        
        for (nm in names) {
          metric_name <- xml_attr(nm, "value")
          
          # Skip variables containing excluded strings
          excluded_patterns <- c(
            "Back_Foot_wrt_Lab",
            "Back_Ankle_Angle",
            "Lead_Ankle_Angle",
            "Back_Hip_Angle",
            "Glove_Elbow_Angle",
            "Glove_Shoulder_Angle",
            "Lead_Hip_Angle",
            "Back_Knee_Ang_Vel",
            "COM wrt Lead Heel_vel",
            "Combined COP wrt Lead Heel"
          )
          
          skip_variable <- FALSE
          for (pattern in excluded_patterns) {
            if (grepl(pattern, metric_name, ignore.case = TRUE)) {
              skip_variable <- TRUE
              break
            }
          }
          
          if (skip_variable) {
            next
          }
          
          # Extract metrics (filtered)
          
          comps <- xml_find_all(nm, "./component")
          for (comp in comps) {
            frames_attr <- xml_attr(comp, "frames")
            data_attr <- xml_attr(comp, "data") %||% xml_text(comp)
            
            if (is.na(frames_attr) || frames_attr == "") next
            
            frames <- suppressWarnings(as.integer(frames_attr))
            if (is.na(frames)) next
            
            # Handle both single values and time series
            values <- parse_comma_data(data_attr)
            if (length(values) == 0) next
            
            # Ensure lengths match
            n_vals <- length(values)
            if (n_vals != frames) {
              if (n_vals > frames) {
                values <- values[1:frames]
              } else {
                values <- c(values, rep(NA_real_, frames - n_vals))
              }
            }
            
            # Store time series data as separate columns (one per frame)
            # Create column names for values
            value_cols <- paste0("value_", 1:frames)
            
            # Create a list with all the data
            row_data <- list(
              owner = own_val,
              folder = folder_val,
              variable = metric_name
            )
            
            # Add value columns
            for (i in 1:frames) {
              row_data[[value_cols[i]]] <- values[i]
            }
            
            all_data[[length(all_data) + 1]] <- as_tibble(row_data)
          }
        }
      }
    }
  }
  
  # Debug output for first few calls
  if (length(all_data) == 0) {
    # Log what we found for debugging
    if (length(folders_seen) > 0 && length(folders_seen) <= 20) {
      # Force output with cat for debugging
      cat("  [DEBUG extract_metric_data] Folders seen:", paste(unique(folders_seen), collapse = ", "), "\n")
      cat("  [DEBUG extract_metric_data] Fastball folders found:", fastball_folders_found, "\n")
      cat("  [DEBUG extract_metric_data] Skipped folders (first 10):", paste(unique(skipped_folders)[1:min(10, length(unique(skipped_folders)))], collapse = ", "), "\n")
      flush.console()
      
      log_progress("      [DEBUG extract_metric_data] Folders seen:", paste(unique(folders_seen), collapse = ", "))
      log_progress("      [DEBUG extract_metric_data] Fastball folders found:", fastball_folders_found)
      log_progress("      [DEBUG extract_metric_data] Skipped folders:", paste(unique(skipped_folders), collapse = ", "))
    }
    return(tibble())
  }
  
  result <- bind_rows(all_data)
  if (nrow(result) > 0 && length(folders_seen) <= 20) {
    log_progress("      [DEBUG extract_metric_data] Successfully extracted", nrow(result), "rows from", fastball_folders_found, "Fastball folder(s)")
  }
  return(result)
}

# ---------- Main processing function ----------
process_all_files <- function(data_root = NULL) {
  # Determine root directory
  # If data_root parameter is provided, use it; otherwise use DATA_ROOT global variable
  root_dir <- if (!is.null(data_root)) {
    data_root
  } else if (is.null(DATA_ROOT)) {
    # Default to Pitching folder in current directory
    pitching_dir <- file.path(getwd(), "Pitching")
    if (dir.exists(pitching_dir)) {
      pitching_dir
    } else {
      getwd()
    }
  } else {
    DATA_ROOT
  }
  
  # Validate that the directory exists and is accessible
  if (!is.null(data_root) || !is.null(DATA_ROOT)) {
    if (!dir.exists(root_dir)) {
      stop("Data directory does not exist or is not accessible: ", root_dir, 
           "\nPlease check:\n",
           "  1. The path is correct\n",
           "  2. The drive is mapped (if it's a network drive)\n",
           "  3. You have permission to access the directory")
    }
    
    # Try to list files to verify access
    test_list <- tryCatch({
      list.files(root_dir, recursive = FALSE, full.names = FALSE)
    }, error = function(e) {
      stop("Cannot access directory: ", root_dir, 
           "\nError: ", conditionMessage(e),
           "\nThis might be a network drive issue. Try:\n",
           "  1. Checking if the network drive is mapped\n",
           "  2. Reconnecting to the network drive\n",
           "  3. Using a local path instead")
    })
    
    if (length(test_list) == 0) {
      log_progress("  [WARNING] Directory exists but appears to be empty")
    } else {
      log_progress("  Directory accessible, found", length(test_list), "items")
    }
  }
  
  log_progress("Scanning for XML files in:", root_dir)
  
  # Find all session.xml and session_data.xml files (including .gz)
  log_progress("Searching for session.xml files...")
  session_files <- tryCatch({
    list.files(root_dir, pattern = "(?i)session\\.xml$", recursive = TRUE, full.names = TRUE)
  }, error = function(e) {
    log_progress("  [ERROR] Failed to search for session.xml files:", conditionMessage(e))
    log_progress("  This might be a network drive issue. Trying non-recursive search...")
    tryCatch({
      list.files(root_dir, pattern = "(?i)session\\.xml$", recursive = FALSE, full.names = TRUE)
    }, error = function(e2) {
      stop("Cannot access directory: ", root_dir, "\nError: ", conditionMessage(e2))
    })
  })
  
  log_progress("Searching for session_data.xml files...")
  session_data_files <- tryCatch({
    list.files(root_dir, pattern = "(?i)session_data\\.xml$", recursive = TRUE, full.names = TRUE)
  }, error = function(e) {
    log_progress("  [ERROR] Failed to search for session_data.xml files:", conditionMessage(e))
    log_progress("  Trying non-recursive search...")
    tryCatch({
      list.files(root_dir, pattern = "(?i)session_data\\.xml$", recursive = FALSE, full.names = TRUE)
    }, error = function(e2) {
      stop("Cannot access directory: ", root_dir, "\nError: ", conditionMessage(e2))
    })
  })
  
  # Also check for .gz files
  log_progress("Searching for compressed .gz files...")
  session_files_gz <- tryCatch({
    list.files(root_dir, pattern = "(?i)session\\.xml\\.gz$", recursive = TRUE, full.names = TRUE)
  }, error = function(e) {
    log_progress("  [WARNING] Could not search for .gz files:", conditionMessage(e))
    character(0)
  })
  session_files <- c(session_files, session_files_gz)
  
  session_data_files_gz <- tryCatch({
    list.files(root_dir, pattern = "(?i)session_data\\.xml\\.gz$", recursive = TRUE, full.names = TRUE)
  }, error = function(e) {
    log_progress("  [WARNING] Could not search for .gz files:", conditionMessage(e))
    character(0)
  })
  session_data_files <- c(session_data_files, session_data_files_gz)
  
  log_progress("Found", length(session_files), "session.xml files")
  log_progress("Found", length(session_data_files), "session_data.xml files")
  
  if (length(session_files) == 0 && length(session_data_files) == 0) {
    log_progress("ERROR: No XML files found in", root_dir)
    log_progress("Trying alternative search...")
    # Try without recursive
    session_files <- list.files(root_dir, pattern = "(?i)session\\.xml$", recursive = FALSE, full.names = TRUE)
    session_data_files <- list.files(root_dir, pattern = "(?i)session_data\\.xml$", recursive = FALSE, full.names = TRUE)
    log_progress("Found (non-recursive):", length(session_files), "session.xml,", length(session_data_files), "session_data.xml")
    
    if (length(session_files) == 0 && length(session_data_files) == 0) {
      stop("No XML files found in ", root_dir)
    }
  }
  
  # Create database connection
  # Use local variable to track warehouse usage (can't modify global from function)
  use_warehouse <- USE_WAREHOUSE
  
  if (use_warehouse) {
    # Connect to PostgreSQL warehouse
    log_progress("Connecting to PostgreSQL warehouse database...")
    con <- tryCatch({
      get_warehouse_connection()
    }, error = function(e) {
      log_progress("ERROR: Could not connect to warehouse database:", conditionMessage(e))
      log_progress("Falling back to local SQLite database")
      use_warehouse <<- FALSE
      # Fall through to SQLite connection
      NULL
    })
    if (is.null(con)) {
      # Fallback to SQLite
      use_warehouse <<- FALSE
      con <- DBI::dbConnect(RSQLite::SQLite(), DB_FILE)
      log_progress("Using local SQLite database as fallback")
    } else {
      log_progress("Connected to warehouse database")
    }
  } else {
    # Use local SQLite database
    log_progress("Using local SQLite database:", DB_FILE)
  # Close any existing connections first
  tryCatch({
    if (file.exists(DB_FILE)) {
      # Try to disconnect any existing connections
      tryCatch({
        existing_con <- dbConnect(RSQLite::SQLite(), DB_FILE)
        dbDisconnect(existing_con)
      }, error = function(e) NULL)
      
      # Now try to remove
      Sys.sleep(0.1)  # Brief pause
      if (file.exists(DB_FILE)) {
        file.remove(DB_FILE)
          log_progress("Removed existing database file")
      }
    }
  }, error = function(e) {
      log_progress("Warning: Could not remove existing database file:", conditionMessage(e))
      log_progress("Will try to overwrite instead...")
  })
  con <- DBI::dbConnect(RSQLite::SQLite(), DB_FILE)
  }
  
  # Fetch athlete UUID mapping from app database
  cat("\n*** STEP 1: FETCHING UUIDs FROM APP DATABASE ***\n")
  log_progress("Fetching athlete UUIDs from app database...")
  uuid_map <- fetch_athlete_uuid_map()
  cat("*** UUID MAP RESULT: Contains", length(uuid_map), "entries ***\n")
  log_progress("UUID map contains", length(uuid_map), "entries")
  if (length(uuid_map) > 0) {
    cat("*** FIRST 5 UUID MAPPINGS ***\n")
    log_progress("First few UUID mappings:")
    for (i in seq_len(min(5, length(uuid_map)))) {
      cat("  '", names(uuid_map)[i], "' -> '", uuid_map[[i]], "'\n", sep = "")
      log_progress("  ", names(uuid_map)[i], " -> ", uuid_map[[i]])
    }
  } else {
    cat("*** CRITICAL WARNING: UUID MAP IS EMPTY! ***\n")
    cat("*** All athletes will get NEW UUIDs instead of matching existing ones ***\n")
    log_progress("WARNING: UUID map is empty! Will generate new UUIDs for all athletes.")
  }
  cat("\n")
  
  # Process session.xml files to get athlete info
  athlete_list <- list()
  owner_mapping <- list()  # Map owner names to athlete IDs
  
  total_session_files <- length(session_files)
  log_progress("=", rep("=", 60), sep = "")
  log_progress("PHASE 1: Processing", total_session_files, "session.xml files for athlete info")
  log_progress("=", rep("=", 60), sep = "")
  
  for (i in seq_along(session_files)) {
    sf <- session_files[i]
    log_progress("[", i, "/", total_session_files, "] Processing athlete info from:", basename(sf))
    doc_athlete <- tryCatch(read_xml_robust(sf), error = function(e) NULL)
    if (is.null(doc_athlete)) {
      log_progress("  [WARNING] Could not read file")
      next
    }
    
    athlete_info <- extract_athlete_info(sf)
    if (!is.null(athlete_info) && nrow(athlete_info) > 0) {
      # Use athlete_manager to get or create athlete in warehouse
      athlete_name <- athlete_info$name[1]
      
      # Convert date format for athlete_manager (expects YYYY-MM-DD)
      dob_for_manager <- NULL
      if (!is.na(athlete_info$date_of_birth[1]) && athlete_info$date_of_birth[1] != "") {
        dob_date <- tryCatch({
          as.Date(athlete_info$date_of_birth[1], format = "%m/%d/%Y")
        }, error = function(e) NULL)
        if (!is.na(dob_date)) {
          dob_for_manager <- format(dob_date, "%Y-%m-%d")
        }
      }
      
      # Get or create athlete UUID using athlete_manager
      # This checks warehouse database first, then app database, then creates new
      if (exists("get_or_create_athlete")) {
        tryCatch({
          log_progress("  [WAREHOUSE] Calling get_or_create_athlete for:", athlete_name)
          log_progress("    DOB:", dob_for_manager, "| Age:", athlete_info$age[1], "| Age at collection:", athlete_info$age_at_collection[1])
          athlete_uuid <- get_or_create_athlete(
            name = athlete_name,
            date_of_birth = dob_for_manager,
            age = athlete_info$age[1],
            age_at_collection = athlete_info$age_at_collection[1],
            gender = athlete_info$gender[1],
            height = athlete_info$height[1],
            weight = athlete_info$weight[1],
            source_system = "pitching",
            source_athlete_id = athlete_info$athlete_id[1],
            check_app_db = TRUE
          )
          athlete_info$uid <- athlete_uuid
          log_progress("  [WAREHOUSE] Got/created athlete UUID for", athlete_name, ":", athlete_uuid)
        }, error = function(e) {
          log_progress("  [WARNING] Failed to get UUID from warehouse:", conditionMessage(e))
          log_progress("  Falling back to local UUID generation")
      athlete_info$uid <- uuid::UUIDgenerate()
        })
      } else {
        # Fallback: Try to get UUID from app database by matching name
        normalized_name <- normalize_name_for_matching(athlete_name)
        matched_uuid <- get_uuid_by_name(athlete_name, uuid_map)
        
        if (!is.na(matched_uuid)) {
          athlete_info$uid <- matched_uuid
          log_progress("  [MATCHED] Found existing UUID for", athlete_name)
        } else {
          # Generate new UUID if no match found
          athlete_info$uid <- uuid::UUIDgenerate()
          log_progress("  [NEW] Generated new UUID for", athlete_name)
        }
      }
      
      # Safety check: ensure uid was set
      if (!"uid" %in% names(athlete_info) || is.na(athlete_info$uid[1])) {
        log_progress("  [ERROR] uid was not set for athlete", athlete_name, "- generating now")
        athlete_info$uid <- uuid::UUIDgenerate()
      }
      
      dir_path <- dirname(sf)
      athlete_list[[length(athlete_list) + 1]] <- athlete_info
      
      # Map by directory path - this will be used to match session_data.xml files
      dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
      owner_mapping[[dir_path_normalized]] <- athlete_info$uid[1]
      
      # Also try to extract measurement filenames from session.xml to map owners
      # Look for Measurement elements with Filename attributes
      root_athlete <- xml_root(doc_athlete)
      measurements <- xml_find_all(root_athlete, ".//Measurement")
      if (length(measurements) > 0) {
        for (meas in measurements) {
          meas_filename <- xml_attr(meas, "Filename")
          if (!is.na(meas_filename) && meas_filename != "") {
            owner_mapping[[meas_filename]] <- athlete_info$uid[1]
            # Also map without extension
            meas_no_ext <- tools::file_path_sans_ext(meas_filename)
            owner_mapping[[meas_no_ext]] <- athlete_info$uid[1]
            log_progress("    Mapped measurement:", meas_filename)
          }
        }
      }
      
      log_progress("  [SUCCESS] Mapped athlete", athlete_info$name[1], "to UID:", athlete_info$uid[1])
      log_progress("  Directory:", dir_path_normalized)
    }
  }
  
  # Note: Athletes are now stored in analytics.d_athletes via athlete_manager
  # We still keep a local athletes table for reference if using SQLite
  if (!use_warehouse) {
  if (length(athlete_list) > 0) {
    athletes_df <- bind_rows(athlete_list)
      log_progress("Writing athletes table to local database...")
    DBI::dbWriteTable(con, "athletes", athletes_df, overwrite = TRUE)
      log_progress("[SUCCESS] Created athletes table with", nrow(athletes_df), "rows")
  } else {
      # Create empty athletes table with age_at_collection column
    athletes_df <- tibble(
      uid = character(),
      athlete_id = character(),
      name = character(),
      date_of_birth = character(),
      age = numeric(),
        age_at_collection = numeric(),
      gender = character(),
      height = numeric(),
      weight = numeric(),
      creation_date = character(),
      creation_time = character(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "athletes", athletes_df, overwrite = TRUE)
    }
  } else {
    log_progress("Note: Athletes are stored in analytics.d_athletes (via athlete_manager)")
    log_progress("Skipping local athletes table creation for warehouse mode")
  }
  
  # Create uid -> athlete_id mapping for adding to metrics
  uid_to_athlete_id <- list()
  if (length(athlete_list) > 0) {
    for (athlete_info in athlete_list) {
      if (nrow(athlete_info) > 0 && "uid" %in% names(athlete_info) && "athlete_id" %in% names(athlete_info)) {
        uid_to_athlete_id[[athlete_info$uid[1]]] <- athlete_info$athlete_id[1]
      }
    }
  }
  
  # Process session_data.xml files
  metric_data_list <- list()
  
  total_data_files <- length(session_data_files)
  total_owners_processed <- 0
  total_owners_matched <- 0
  total_owners_skipped <- 0
  total_rows_extracted <- 0
  
  log_progress("")
  log_progress("PHASE 2: Processing", total_data_files, "session_data.xml files for metric data")
  log_progress("")
  
  # Force output immediately
  cat("\n*** PHASE 2 STARTING ***\n")
  cat("Total session_data.xml files:", total_data_files, "\n")
  flush.console()
  
  for (i in seq_along(session_data_files)) {
    sdf <- session_data_files[i]
    if (i <= 3 || i %% 10 == 0) {  # Log first 3 files and every 10th file
      log_progress("[", i, "/", total_data_files, "] Processing:", basename(sdf))
      cat("Processing file", i, "of", total_data_files, ":", basename(sdf), "\n")
      flush.console()
    }
    doc <- tryCatch(read_xml_robust(sdf), error = function(e) {
      if (i <= 3) {
        log_progress("  [ERROR] Error reading file:", conditionMessage(e))
        cat("ERROR reading file:", conditionMessage(e), "\n")
        flush.console()
      }
      NULL
    })
    if (is.null(doc)) {
      if (i <= 3) {
        cat("  File", i, "failed to load, skipping\n")
        flush.console()
      }
      next
    }
    
    # Try to find owner names
    root <- xml_root(doc)
    if (identical(xml_name(root), "v3d")) {
      owners <- xml_find_all(root, "./owner")
      owner_names <- xml_attr(owners, "value")
      
      if (i <= 3) {
        log_progress("  Found", length(owner_names), "owners:", paste(owner_names, collapse = ", "))
        cat("  Found", length(owner_names), "owners in file", i, "\n")
        flush.console()
      }
      
      # Try to match owner to athlete
      dir_path <- dirname(sdf)
      
      # Look for matching athlete based on directory structure
      matched_uid <- NA_character_
      
      for (owner_name in owner_names) {
        total_owners_processed <- total_owners_processed + 1
        matched_uid <- NA_character_
        
        # PRIORITY: Match by finding session.xml in the same directory as session_data.xml
        # This is the most reliable method since each session_data.xml is in a specific athlete's directory
        dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
        
        # First check if we already mapped this directory (from a previous owner in same file)
        if (dir_path_normalized %in% names(owner_mapping)) {
          matched_uid <- owner_mapping[[dir_path_normalized]]
          if (i <= 3) {
            cat("    [MATCH-CACHED] Owner", owner_name, "-> Directory already mapped to UID:", matched_uid, "\n")
            flush.console()
          }
        } else {
          # Find session.xml in same directory (most reliable)
          session_xml <- file.path(dir_path, "session.xml")
          if (file.exists(session_xml)) {
            if (i <= 3) {
              cat("    [DEBUG] Found session.xml in same directory:", session_xml, "\n")
              flush.console()
            }
            # Look up this session.xml in athlete_list (from Phase 1) to get the correct UUID
            session_xml_normalized <- normalizePath(session_xml, winslash = "/", mustWork = FALSE)
            found_athlete <- NULL
            for (athlete_info in athlete_list) {
              if (nrow(athlete_info) > 0) {
                athlete_path_normalized <- normalizePath(athlete_info$source_path[1], winslash = "/", mustWork = FALSE)
                if (athlete_path_normalized == session_xml_normalized) {
                  found_athlete <- athlete_info
                  break
                }
              }
            }
            
            if (!is.null(found_athlete) && nrow(found_athlete) > 0 && "uid" %in% names(found_athlete) && !is.na(found_athlete$uid[1])) {
              matched_uid <- found_athlete$uid[1]
              # Cache this mapping for future owners in same directory
              owner_mapping[[dir_path_normalized]] <- matched_uid
              if (i <= 3) {
                cat("    [MATCH-SESSION] Owner", owner_name, "matched via session.xml in same directory\n")
                cat("      Athlete:", found_athlete$name[1], "| UID:", matched_uid, "\n")
                flush.console()
              }
            } else {
              if (i <= 3) {
                cat("    [WARNING] session.xml found but not in athlete_list:", session_xml, "\n")
                flush.console()
              }
            }
          } else {
            # Try parent directory
            parent_dir <- dirname(dir_path)
            session_xml <- file.path(parent_dir, "session.xml")
            if (file.exists(session_xml)) {
              if (i <= 3) {
                cat("    [DEBUG] Found session.xml in parent directory:", session_xml, "\n")
                flush.console()
              }
              # Look up this session.xml in athlete_list
              session_xml_normalized <- normalizePath(session_xml, winslash = "/", mustWork = FALSE)
              found_athlete <- NULL
              for (athlete_info in athlete_list) {
                if (nrow(athlete_info) > 0) {
                  athlete_path_normalized <- normalizePath(athlete_info$source_path[1], winslash = "/", mustWork = FALSE)
                  if (athlete_path_normalized == session_xml_normalized) {
                    found_athlete <- athlete_info
                    break
                  }
                }
              }
              
              if (!is.null(found_athlete) && nrow(found_athlete) > 0 && "uid" %in% names(found_athlete) && !is.na(found_athlete$uid[1])) {
                matched_uid <- found_athlete$uid[1]
                owner_mapping[[dir_path_normalized]] <- matched_uid
                if (i <= 3) {
                  cat("    [MATCH-SESSION] Owner", owner_name, "matched via session.xml in parent directory\n")
                  cat("      Athlete:", found_athlete$name[1], "| UID:", matched_uid, "\n")
                  flush.console()
                }
              } else {
                if (i <= 3) {
                  cat("    [WARNING] session.xml in parent found but not in athlete_list:", session_xml, "\n")
                  flush.console()
                }
              }
            } else {
              if (i <= 3) {
                cat("    [WARNING] No session.xml found in", dir_path, "or parent\n")
                flush.console()
              }
            }
          }
        }
        
        # Fallback: Try direct match by owner name (less reliable)
        if (is.na(matched_uid)) {
        owner_base <- basename(owner_name)
        owner_no_ext <- tools::file_path_sans_ext(owner_base)
        
        if (owner_base %in% names(owner_mapping)) {
          matched_uid <- owner_mapping[[owner_base]]
            if (i <= 3) {
              cat("    [MATCH-NAME] Owner", owner_name, "matched via direct name match\n")
              flush.console()
            }
        } else if (owner_no_ext %in% names(owner_mapping)) {
          matched_uid <- owner_mapping[[owner_no_ext]]
            if (i <= 3) {
              cat("    [MATCH-NAME] Owner", owner_name, "matched via base name match\n")
              flush.console()
            }
          }
        }
        
        # Last resort: Try to match by directory similarity (only if session.xml not found)
        # This should rarely be needed if session.xml files are in the right places
        if (is.na(matched_uid) && length(athlete_list) > 0) {
          dir_path_normalized <- normalizePath(dir_path, winslash = "/", mustWork = FALSE)
          
          # Check all athletes to see if any match this directory
          best_match <- NULL
          best_match_score <- 0
          
          for (athlete_info in athlete_list) {
            if (nrow(athlete_info) > 0) {
              athlete_dir <- dirname(athlete_info$source_path[1])
              athlete_dir_normalized <- normalizePath(athlete_dir, winslash = "/", mustWork = FALSE)
              
              # Calculate match score - exact match gets highest score
              match_score <- 0
              if (dir_path_normalized == athlete_dir_normalized) {
                match_score <- 100  # Exact match
              } else if (grepl(paste0("^", gsub("([^/])$", "\\1/", athlete_dir_normalized)), dir_path_normalized)) {
                match_score <- 50  # session_data.xml is in athlete's directory
          } else {
                # Check if they share a common parent directory
                dir_parts <- strsplit(dir_path_normalized, "/")[[1]]
                athlete_parts <- strsplit(athlete_dir_normalized, "/")[[1]]
                common_parts <- min(length(dir_parts), length(athlete_parts))
                for (j in 1:common_parts) {
                  if (dir_parts[j] == athlete_parts[j]) {
                    match_score <- match_score + 1
                  } else {
                  break
                }
              }
            }
              
              if (match_score > best_match_score) {
                best_match_score <- match_score
                best_match <- athlete_info
              }
            }
          }
          
          # Only use match if score is high enough (at least 2 common directory parts to avoid false matches)
          if (!is.null(best_match) && best_match_score >= 2 && "uid" %in% names(best_match) && !is.na(best_match$uid[1])) {
            matched_uid <- best_match$uid[1]
            if (i <= 3) {
              cat("    [MATCH] Owner", owner_name, "matched via directory similarity (score:", best_match_score, ")\n")
              cat("      Athlete:", best_match$name[1], "| UID:", matched_uid, "\n")
              flush.console()
            }
          }
        }
        
        if (is.na(matched_uid)) {
          total_owners_skipped <- total_owners_skipped + 1
          if (i <= 3 || total_owners_skipped <= 5) {
            log_progress("    [SKIPPED] Could not match owner", owner_name, "- skipping")
            log_progress("    Directory:", dir_path)
          }
          next  # Skip this owner instead of using wrong UUID
        }
        
        # Filter: Only process Fastball trials, skip Static and other trial types
        # Check owner name (not folder name) for trial type
        owner_name_lower <- tolower(owner_name)
        is_static <- grepl("static", owner_name_lower)
        is_fastball <- grepl("fastball", owner_name_lower)
        
        if (is_static) {
          if (i <= 3) {
            cat("    [SKIP] Static trial owner:", owner_name, "\n")
            flush.console()
          }
          next  # Skip static trials
        }
        
        if (!is_fastball) {
          if (i <= 3) {
            cat("    [SKIP] Non-Fastball trial owner:", owner_name, "\n")
            flush.console()
          }
          next  # Skip non-Fastball trials
        }
        
        total_owners_matched <- total_owners_matched + 1
        
        # Extract ALL metric data for this owner
        if (i <= 3) {
          log_progress("    [MATCHED] Owner:", owner_name, "-> UID:", matched_uid)
          cat("    [PROCESSING] Fastball owner:", owner_name, "\n")
          flush.console()
        }
        
        # Call extract_metric_data and get detailed feedback
        if (i <= 3) {
          cat("    Calling extract_metric_data for owner:", owner_name, "\n")
          flush.console()
        }
        metric_data <- extract_metric_data(doc, owner_name)
        
        if (i <= 3) {
          cat("    extract_metric_data returned", nrow(metric_data), "rows\n")
          flush.console()
        }
        
        if (nrow(metric_data) > 0) {
          total_rows_extracted <- total_rows_extracted + nrow(metric_data)
          if (i <= 3) {
            log_progress("      Extracted", nrow(metric_data), "rows of METRIC data")
            cat("    [SUCCESS] Extracted", nrow(metric_data), "rows for owner", owner_name, "\n")
            flush.console()
          }
          metric_data$uid <- matched_uid
          # Add athlete_id if we have a mapping
          if (!is.na(matched_uid) && matched_uid %in% names(uid_to_athlete_id)) {
            metric_data$athlete_id <- uid_to_athlete_id[[matched_uid]]
          } else {
            metric_data$athlete_id <- NA_character_
          }
          metric_data$source_file <- basename(sdf)
          metric_data$source_path <- sdf
          metric_data_list[[length(metric_data_list) + 1]] <- metric_data
        } else {
          if (i <= 3) {
            log_progress("      [WARNING] No METRIC data extracted for", owner_name)
            cat("    [WARNING] No data extracted for owner:", owner_name, "\n")
            flush.console()
          }
        }
        
        # Periodic progress update every 10 files
        if (i %% 10 == 0) {
          log_progress("  [PROGRESS] Processed", i, "/", total_data_files, "files")
          log_progress("    Owners matched:", total_owners_matched, "| Skipped:", total_owners_skipped, "| Rows extracted:", total_rows_extracted)
        }
      }
    }
  }
  
  # Summary of Phase 2
  log_progress("")
  log_progress("PHASE 2 SUMMARY:")
  log_progress("  Total owners processed:", total_owners_processed)
  log_progress("  Owners matched:", total_owners_matched)
  log_progress("  Owners skipped:", total_owners_skipped)
  log_progress("  Total metric rows extracted:", total_rows_extracted)
  log_progress("  Metric data lists created:", length(metric_data_list))
  
  # Force output with cat
  cat("\n*** PHASE 2 SUMMARY ***\n")
  cat("Total owners processed:", total_owners_processed, "\n")
  cat("Owners matched:", total_owners_matched, "\n")
  cat("Owners skipped:", total_owners_skipped, "\n")
  cat("Total metric rows extracted:", total_rows_extracted, "\n")
  cat("Metric data lists created:", length(metric_data_list), "\n")
  flush.console()
  
  # Combine and write to database
  # For time series data, we need to ensure all rows have the same columns
  # First, find maximum frame count across all data before binding
  
  if (length(metric_data_list) > 0) {
    log_progress("")
    log_progress("PHASE 3: Consolidating metric data and writing to database...")
    # Find max frames before binding
    max_frames <- 0
    log_progress("  Finding maximum frame count across all data...")
    for (df in metric_data_list) {
      if (nrow(df) > 0) {
        # Find the maximum value column index
        value_cols <- grep("^value_\\d+$", names(df), value = TRUE)
        if (length(value_cols) > 0) {
          # Extract numbers from column names
          frame_nums <- suppressWarnings(as.integer(gsub("value_", "", value_cols)))
          frame_nums <- frame_nums[!is.na(frame_nums)]
          if (length(frame_nums) > 0) {
            max_frames <- max(max_frames, max(frame_nums), na.rm = TRUE)
          }
        }
      }
    }
    log_progress("  Maximum frames in METRIC data:", max_frames)
    
    # Pad each dataframe to max_frames before binding
    log_progress("  Padding dataframes to", max_frames, "frames...")
    meta_cols <- c("uid", "athlete_id", "owner", "folder", "variable", "source_file", "source_path")
    value_cols <- paste0("value_", 1:max_frames)
    
    padded_list <- list()
    for (idx in seq_along(metric_data_list)) {
      df <- metric_data_list[[idx]]
      if (idx %% 50 == 0) {
        log_progress("    Padding dataframe", idx, "of", length(metric_data_list))
      }
      # Add missing columns with NA
      for (j in 1:max_frames) {
        value_col <- paste0("value_", j)
        if (!value_col %in% names(df)) {
          df[[value_col]] <- NA_real_
        }
      }
      
      # Reorder columns
      df <- df %>% select(any_of(c(meta_cols, value_cols)))
      padded_list[[length(padded_list) + 1]] <- df
    }
    
    log_progress("  Binding all dataframes together...")
    log_progress("  Number of dataframes to bind:", length(padded_list))
    if (length(padded_list) > 0) {
      log_progress("  First dataframe dimensions:", nrow(padded_list[[1]]), "rows,", ncol(padded_list[[1]]), "columns")
      log_progress("  First dataframe columns:", paste(names(padded_list[[1]]), collapse = ", "))
    }
    cat("  [PROGRESS] Binding", length(padded_list), "dataframes (this may take a while)...\n")
    flush.console()
    metric_df <- bind_rows(padded_list)
    log_progress("  metric_df after binding:", nrow(metric_df), "rows,", ncol(metric_df), "columns")
    cat("  [SUCCESS] Binding complete!\n")
    flush.console()
    
    if (use_warehouse) {
      # Write to PostgreSQL warehouse f_kinematics_pitching table
      log_progress("  Writing metrics to warehouse f_kinematics_pitching table...")
      if (nrow(metric_df) == 0) {
        log_progress("  [WARNING] metric_df is EMPTY - no data was extracted!")
        log_progress("  This could mean:")
        log_progress("    1. No Fastball trials were found")
        log_progress("    2. No metric data was extracted from session_data.xml files")
        log_progress("    3. All data was filtered out")
        log_progress("  Check the Phase 2 summary above for owners matched/skipped")
      }
      
      # Extract session_date from source_path (try to get date from directory structure or creation_date)
      log_progress("  Setting session_date for", nrow(metric_df), "rows...")
      cat("  [PROGRESS] Creating session_date mapping (optimized)...\n")
      flush.console()
      
      # OPTIMIZED: Create a mapping from source_path directory to creation_date first
      # This avoids the nested loop which is O(n*m) - very slow!
      path_to_date_map <- list()
      for (athlete_info in athlete_list) {
        if (nrow(athlete_info) > 0) {
          athlete_dir <- dirname(athlete_info$source_path[1])
          athlete_dir_normalized <- normalizePath(athlete_dir, winslash = "/", mustWork = FALSE)
          
          if (!is.na(athlete_info$creation_date[1]) && athlete_info$creation_date[1] != "") {
            creation_date <- tryCatch({
              as.Date(athlete_info$creation_date[1], format = "%m/%d/%Y")
            }, error = function(e) NULL)
            if (!is.na(creation_date)) {
              path_to_date_map[[athlete_dir_normalized]] <- creation_date
            }
          }
        }
      }
      
      # Now use the map to assign dates (much faster - O(n))
      default_date <- Sys.Date()
      metric_df$session_date <- default_date
      
      # Get unique source_path directories
      unique_paths <- unique(metric_df$source_path[!is.na(metric_df$source_path)])
      log_progress("  Found", length(unique_paths), "unique source paths")
      
      # Create a vectorized assignment
      for (path in unique_paths) {
        path_dir <- normalizePath(dirname(path), winslash = "/", mustWork = FALSE)
        if (path_dir %in% names(path_to_date_map)) {
          metric_df$session_date[metric_df$source_path == path] <- path_to_date_map[[path_dir]]
        }
      }
      
      cat("  [SUCCESS] Session dates assigned!\n")
      flush.console()
      
      # Prepare data for warehouse - pivot to long format
      log_progress("  Transforming data for warehouse format...")
      cat("  [PROGRESS] Pivoting to long format (this may take a while with", nrow(metric_df), "rows)...\n")
      flush.console()
      
      # Get all value columns
      value_cols <- grep("^value_\\d+$", names(metric_df), value = TRUE)
      log_progress("  Found", length(value_cols), "value columns to pivot")
      
      # Pivot to long format
      warehouse_df <- metric_df %>%
        select(uid, athlete_id, owner, folder, variable, source_file, source_path, session_date, all_of(value_cols)) %>%
        pivot_longer(
          cols = all_of(value_cols),
          names_to = "frame",
          values_to = "value",
          names_prefix = "value_"
        ) %>%
        filter(!is.na(value)) %>%  # Remove NA values
        mutate(
          frame = as.integer(frame),
          metric_name = paste(folder, variable, sep = "."),
          source_system = "pitching",
          created_at = Sys.time()
        ) %>%
        select(
          athlete_uuid = uid,
          session_date,
          source_system,
          source_athlete_id = athlete_id,
          metric_name,
          frame,
          value,
          created_at
        )
      
      log_progress("  Pivot complete! warehouse_df has", nrow(warehouse_df), "rows")
      cat("  [SUCCESS] Pivot complete! Final row count:", nrow(warehouse_df), "\n")
      flush.console()
      
      # Write to warehouse
      log_progress("  ===== STARTING DATABASE WRITE =====")
      log_progress("  Writing", nrow(warehouse_df), "rows to f_kinematics_pitching...")
      cat("  [INFO] Starting database write operation...\n")
      flush.console()
      
      # Check if table exists, create if not
      log_progress("  Checking if table exists...")
      table_exists <- tryCatch({
        DBI::dbExistsTable(con, Id(schema = "public", table = "f_kinematics_pitching"))
      }, error = function(e) {
        log_progress("  [ERROR] Failed to check if table exists:", conditionMessage(e))
        stop("Cannot check table existence: ", conditionMessage(e))
      })
      log_progress("  Table exists:", table_exists)
      
      if (!table_exists) {
        log_progress("  Creating f_kinematics_pitching table...")
        # Create table with long format structure (one row per metric per frame)
        DBI::dbExecute(con, '
          CREATE TABLE IF NOT EXISTS f_kinematics_pitching (
            id SERIAL PRIMARY KEY,
            athlete_uuid VARCHAR(36) NOT NULL,
            session_date DATE NOT NULL,
            source_system VARCHAR(50) NOT NULL,
            source_athlete_id VARCHAR(100),
            metric_name TEXT NOT NULL,
            frame INTEGER NOT NULL,
            value NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT idx_f_pitching_unique UNIQUE (athlete_uuid, session_date, metric_name, frame)
          )
        ')
        
        # Create indexes
        DBI::dbExecute(con, 'CREATE INDEX IF NOT EXISTS idx_f_pitching_uuid ON f_kinematics_pitching(athlete_uuid)')
        DBI::dbExecute(con, 'CREATE INDEX IF NOT EXISTS idx_f_pitching_date ON f_kinematics_pitching(session_date)')
        DBI::dbExecute(con, 'CREATE INDEX IF NOT EXISTS idx_f_pitching_metric ON f_kinematics_pitching(metric_name)')
        log_progress("  [SUCCESS] Created f_kinematics_pitching table")
      } else {
        # Check if table has the right structure
        log_progress("  Table exists, checking structure...")
        table_info <- DBI::dbGetQuery(con, "
          SELECT column_name, data_type 
          FROM information_schema.columns 
          WHERE table_schema = 'public' 
          AND table_name = 'f_kinematics_pitching'
          ORDER BY ordinal_position
        ")
        log_progress("  Current columns:", paste(table_info$column_name, collapse = ", "))
        
        # Check if metric_name column exists
        if (!"metric_name" %in% table_info$column_name) {
          log_progress("  [WARNING] Table exists but has wrong structure!")
          log_progress("  Table needs metric_name, frame, and value columns")
          log_progress("  You may need to drop and recreate the table, or use a different table name")
          stop("f_kinematics_pitching table exists but has incompatible structure. Please drop and recreate it.")
        }
        
        # Check if unique constraint exists
        constraint_check <- DBI::dbGetQuery(con, "
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE table_schema = 'public'
          AND table_name = 'f_kinematics_pitching'
          AND constraint_type = 'UNIQUE'
          AND constraint_name = 'idx_f_pitching_unique'
        ")
        
        if (nrow(constraint_check) == 0) {
          log_progress("  Unique constraint missing, adding it...")
          tryCatch({
            DBI::dbExecute(con, "
              ALTER TABLE public.f_kinematics_pitching
              ADD CONSTRAINT idx_f_pitching_unique UNIQUE (athlete_uuid, session_date, metric_name, frame)
            ")
            log_progress("  [SUCCESS] Added unique constraint")
          }, error = function(e) {
            log_progress("  [WARNING] Could not add unique constraint:", conditionMessage(e))
            log_progress("  This may cause issues with ON CONFLICT. You may need to manually add the constraint.")
          })
        } else {
          log_progress("  Unique constraint exists")
        }
      }
      
      # Write data (append mode for warehouse)
      log_progress("  Attempting to write data...")
      log_progress("  warehouse_df dimensions:", nrow(warehouse_df), "rows,", ncol(warehouse_df), "columns")
      log_progress("  warehouse_df columns:", paste(names(warehouse_df), collapse = ", "))
      if (nrow(warehouse_df) > 0) {
        log_progress("  First few rows sample:")
        print(head(warehouse_df, 3))
      } else {
        log_progress("  [WARNING] warehouse_df is EMPTY - no data to write!")
        log_progress("  This means no metric data was extracted from the XML files")
        log_progress("  Check if metric_data_list has data before transformation")
      }
      
      if (nrow(warehouse_df) > 0) {
        tryCatch({
          # Check if we should clear existing data first
          # Option: Add a flag to control this behavior, or check for duplicates
          existing_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count FROM f_kinematics_pitching")$count
          log_progress("  Existing rows in table:", existing_count)
          
          if (existing_count > 0) {
            if (TRUNCATE_BEFORE_INSERT) {
              log_progress("  Truncating existing data before inserting new data...")
              DBI::dbExecute(con, "TRUNCATE TABLE f_kinematics_pitching")
              log_progress("  [SUCCESS] Table truncated")
            } else {
              log_progress("  [WARNING] Table already contains", existing_count, "rows!")
              log_progress("  This will APPEND new data, potentially creating duplicates")
              log_progress("  To avoid duplicates, set TRUNCATE_BEFORE_INSERT <- TRUE")
              
              # Check for potential duplicates based on unique combination
              # We'll use a simple check: if the new data size matches existing, warn
              if (nrow(warehouse_df) == existing_count) {
                log_progress("  [WARNING] New data count matches existing count - possible duplicate run!")
              }
            }
          }
          
          log_progress("  Writing", nrow(warehouse_df), "rows to database...")
          
          # Use INSERT with ON CONFLICT to prevent duplicates
          # First, let's try to identify potential duplicates before inserting
          # Check for existing data with same athlete_uuid, session_date, metric_name, and frame
          log_progress("  Checking for potential duplicates...")
          
          # Get unique combinations from new data
          new_combos <- warehouse_df %>%
            distinct(athlete_uuid, session_date, metric_name, frame) %>%
            mutate(check_key = paste(athlete_uuid, session_date, metric_name, frame, sep = "|"))
          
          # Check existing data
          existing_combos <- DBI::dbGetQuery(con, "
            SELECT DISTINCT athlete_uuid, session_date, metric_name, frame
            FROM f_kinematics_pitching
          ")
          
          if (nrow(existing_combos) > 0) {
            existing_combos$check_key <- paste(existing_combos$athlete_uuid, 
                                               existing_combos$session_date, 
                                               existing_combos$metric_name, 
                                               existing_combos$frame, sep = "|")
            
            # Find duplicates
            duplicates <- new_combos$check_key %in% existing_combos$check_key
            n_duplicates <- sum(duplicates)
            
            if (n_duplicates > 0) {
              log_progress("  [WARNING] Found", n_duplicates, "potential duplicate rows!")
              log_progress("  Filtering out duplicates before insert...")
              
              # Filter out duplicates from warehouse_df
              warehouse_df$check_key <- paste(warehouse_df$athlete_uuid, 
                                               warehouse_df$session_date, 
                                               warehouse_df$metric_name, 
                                               warehouse_df$frame, sep = "|")
              warehouse_df <- warehouse_df[!warehouse_df$check_key %in% existing_combos$check_key, ]
              warehouse_df$check_key <- NULL  # Remove temporary column
              
              log_progress("  After filtering:", nrow(warehouse_df), "unique rows to insert")
            } else {
              log_progress("  No duplicates found - all rows are new")
            }
          } else {
            log_progress("  No existing data - all rows are new")
          }
          
          if (nrow(warehouse_df) > 0) {
            # Prisma creates a unique constraint on (athlete_uuid, session_date, metric_name, frame)
            # Use dbWriteTable but wrap in tryCatch to catch constraint violations
            # If it fails, we'll use a workaround
            log_progress("  Inserting", nrow(warehouse_df), "rows to f_kinematics_pitching...")
            
            # Ensure data types match Prisma schema
            warehouse_df$athlete_uuid <- as.character(warehouse_df$athlete_uuid)
            warehouse_df$session_date <- as.Date(warehouse_df$session_date)
            warehouse_df$source_system <- as.character(warehouse_df$source_system)
            if (!is.null(warehouse_df$source_athlete_id)) {
              warehouse_df$source_athlete_id <- as.character(warehouse_df$source_athlete_id)
            }
            warehouse_df$metric_name <- as.character(warehouse_df$metric_name)
            warehouse_df$frame <- as.integer(warehouse_df$frame)
            warehouse_df$value <- as.numeric(warehouse_df$value)
            warehouse_df$created_at <- as.POSIXct(warehouse_df$created_at)
            
            # Use temp table + INSERT ... ON CONFLICT DO NOTHING to handle duplicates gracefully
            log_progress("  Inserting data with ON CONFLICT DO NOTHING (skips duplicates)...")
            cat("  [INFO] Writing data to database (this may take a moment)...\n")
            flush.console()
            
            rows_inserted <- tryCatch({
              # Create a temporary table with the same structure
              temp_table_name <- "temp_pitching_insert"
              
              log_progress("  Creating temporary table for batch insert...")
              DBI::dbExecute(con, paste0("
                CREATE TEMP TABLE ", temp_table_name, " (
                  athlete_uuid VARCHAR(36) NOT NULL,
                  session_date DATE NOT NULL,
                  source_system VARCHAR(50) NOT NULL,
                  source_athlete_id VARCHAR(100),
                  metric_name TEXT NOT NULL,
                  frame INTEGER NOT NULL,
                  value NUMERIC,
                  created_at TIMESTAMP
                )
              "))
              
              # Write data to temp table (fast)
              log_progress("  Writing", nrow(warehouse_df), "rows to temp table...")
              DBI::dbWriteTable(con, temp_table_name, warehouse_df, append = TRUE, row.names = FALSE)
              
              # Insert from temp table to actual table with ON CONFLICT DO NOTHING
              log_progress("  Inserting from temp table to f_kinematics_pitching (skipping duplicates)...")
              result <- DBI::dbExecute(con, paste0("
                INSERT INTO public.f_kinematics_pitching 
                (athlete_uuid, session_date, source_system, source_athlete_id, metric_name, frame, value, created_at)
                SELECT athlete_uuid, session_date, source_system, source_athlete_id, metric_name, frame, value, created_at
                FROM ", temp_table_name, "
                ON CONFLICT (athlete_uuid, session_date, metric_name, frame) DO NOTHING
              "))
              
              # Drop temp table
              DBI::dbExecute(con, paste0("DROP TABLE IF EXISTS ", temp_table_name))
              
              log_progress("  [SUCCESS] Inserted", result, "new rows (skipped", nrow(warehouse_df) - result, "duplicates)")
              cat("  [SUCCESS] Insert complete!\n")
              flush.console()
              
              result
            }, error = function(e) {
              error_msg <- conditionMessage(e)
              log_progress("  [ERROR] Insert failed:", error_msg)
              cat("  [ERROR] Insert failed:", error_msg, "\n")
              flush.console()
              
              # Clean up temp table if it exists
              tryCatch({
                DBI::dbExecute(con, "DROP TABLE IF EXISTS temp_pitching_insert")
              }, error = function(e) NULL)
              
              # Check if it's a foreign key violation
              if (grepl("foreign key|violates foreign key|insert or update on table.*violates foreign key constraint", error_msg, ignore.case = TRUE)) {
                log_progress("  [ERROR] FOREIGN KEY VIOLATION - athlete_uuid not found in analytics.d_athletes!")
                log_progress("  Checking which UUIDs are missing...")
                
                unique_uuids <- unique(warehouse_df$athlete_uuid)
                log_progress("  Attempting to insert", length(unique_uuids), "unique athlete UUIDs")
                
                uuid_check <- DBI::dbGetQuery(con, paste0("
                  SELECT athlete_uuid 
                  FROM analytics.d_athletes 
                  WHERE athlete_uuid IN ('", paste(unique_uuids, collapse = "','"), "')
                "))
                
                missing_uuids <- setdiff(unique_uuids, uuid_check$athlete_uuid)
                if (length(missing_uuids) > 0) {
                  log_progress("  [ERROR] Missing", length(missing_uuids), "UUIDs in d_athletes:")
                  log_progress("  First 5 missing:", paste(head(missing_uuids, 5), collapse = ", "))
                  stop("Foreign key violation: ", length(missing_uuids), " athlete UUIDs not found in analytics.d_athletes")
                }
              }
              
              stop("Insert failed: ", error_msg)
            })
            
            if (rows_inserted > 0) {
              log_progress("[SUCCESS] Inserted", rows_inserted, "new rows to f_kinematics_pitching table")
            } else {
              log_progress("  [INFO] No new rows inserted (all were duplicates)")
            }
          } else {
            log_progress("  [SKIPPED] No new rows to insert (all were duplicates)")
          }
          
          # Verify write by counting rows
          row_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count FROM f_kinematics_pitching")$count
          log_progress("  Total rows in table now:", row_count)
          
          # Check for duplicates by athlete and session
          duplicate_check <- DBI::dbGetQuery(con, "
            SELECT athlete_uuid, session_date, COUNT(*) as cnt
            FROM f_kinematics_pitching
            GROUP BY athlete_uuid, session_date
            HAVING COUNT(*) > 50000
            ORDER BY cnt DESC
            LIMIT 5
          ")
          if (nrow(duplicate_check) > 0) {
            log_progress("  [WARNING] Found athletes with unusually high row counts (possible duplicates):")
            print(duplicate_check)
          }
          
          # Also verify with a sample query
          sample_data <- DBI::dbGetQuery(con, "SELECT * FROM f_kinematics_pitching LIMIT 5")
          if (nrow(sample_data) > 0) {
            log_progress("  Sample data from table:")
            print(sample_data)
          } else {
            log_progress("  [WARNING] Table exists but query returned 0 rows!")
          }
          
          # Update athlete data flags and session counts
          log_progress("")
          log_progress("Updating athlete data flags and session counts...")
          tryCatch({
            update_result <- update_athlete_flags(con, verbose = TRUE)
            if (update_result$success) {
              log_progress("  [SUCCESS] Athlete flags updated successfully")
            } else {
              log_progress("  [WARNING] Failed to update athlete flags:", update_result$message)
            }
          }, error = function(e) {
            log_progress("  [WARNING] Error updating athlete flags:", conditionMessage(e))
            log_progress("  You can manually update flags by running: SELECT update_athlete_data_flags();")
          })
          
          # Check for duplicate athletes and prompt to merge
          log_progress("")
          log_progress("=", rep("=", 60), sep = "")
          log_progress("CHECKING FOR DUPLICATE ATHLETES")
          log_progress("=", rep("=", 60), sep = "")
          tryCatch({
            # Get list of processed athlete UUIDs
            processed_uuids <- sapply(athlete_list, function(a) {
              if (nrow(a) > 0 && "uid" %in% names(a) && !is.na(a$uid[1])) {
                return(a$uid[1])
              }
              return(NULL)
            })
            processed_uuids <- unique(unlist(processed_uuids))
            processed_uuids <- processed_uuids[!is.null(processed_uuids)]
            
            log_progress("  Checking", length(processed_uuids), "processed athlete UUID(s) for duplicates...")
            if (length(processed_uuids) > 0) {
              log_progress("  Athlete UUIDs to check:", paste(processed_uuids, collapse = ", "))
            }
            
            if (length(processed_uuids) > 0 && exists("check_and_merge_duplicates")) {
              log_progress("  Calling check_and_merge_duplicates()...")
              duplicate_result <- check_and_merge_duplicates(
                athlete_uuids = processed_uuids,
                min_similarity = 0.80
              )
              log_progress("  Duplicate check completed")
              if (!is.null(duplicate_result)) {
                log_progress("  Result:", paste(names(duplicate_result), "=", duplicate_result, collapse = ", "))
                if (!is.null(duplicate_result$matches_found)) {
                  log_progress("  [DUPLICATE CHECK] Found", duplicate_result$matches_found, "potential matches")
                  log_progress("  [DUPLICATE CHECK] Merged:", duplicate_result$merged, "| Skipped:", duplicate_result$skipped)
                }
              } else {
                log_progress("  [WARNING] Duplicate check returned NULL result")
              }
            } else if (length(processed_uuids) > 0) {
              log_progress("  [WARNING] check_and_merge_duplicates function not found - skipping duplicate check")
              log_progress("  Processed", length(processed_uuids), "athlete UUIDs")
              log_progress("  Function exists check:", exists("check_and_merge_duplicates"))
            } else {
              log_progress("  [INFO] No athlete UUIDs to check (athlete_list is empty)")
            }
            log_progress("=", rep("=", 60), sep = "")
          }, error = function(e) {
            log_progress("  [ERROR] Could not check for duplicates:", conditionMessage(e))
            log_progress("  Error details:", toString(e))
            traceback()
          })
        }, error = function(e) {
          log_progress("[ERROR] Failed to write data:", conditionMessage(e))
          log_progress("  Error details:", toString(e))
          log_progress("  warehouse_df structure:")
          print(str(warehouse_df))
          stop("Failed to write to warehouse: ", conditionMessage(e))
        })
      } else {
        log_progress("  [SKIPPED] No data to write - warehouse_df is empty")
      }
      
    } else {
      # Write to local SQLite database
      log_progress("  Writing metrics table to local database (this may take a while)...")
    DBI::dbWriteTable(con, "metrics", metric_df, overwrite = TRUE)
      log_progress("[SUCCESS] Created metrics table with", nrow(metric_df), "rows and", ncol(metric_df), "columns")
    
    # Create indexes for faster queries
      log_progress("  Creating indexes for faster queries...")
    tryCatch({
        log_progress("    Creating index on uid...")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_uid ON metrics(uid)")
        log_progress("    Creating index on variable...")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_variable ON metrics(variable)")
        log_progress("    Creating index on folder...")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_folder ON metrics(folder)")
        log_progress("    Creating composite index on (uid, variable)...")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_metrics_uid_variable ON metrics(uid, variable)")
        log_progress("  [SUCCESS] Created indexes on uid, variable, folder, and (uid, variable)")
    }, error = function(e) {
        log_progress("  [WARNING] Could not create all indexes:", conditionMessage(e))
    })
    }
  } else {
    if (!use_warehouse) {
    metric_df <- tibble(
      uid = character(),
        athlete_id = character(),
      owner = character(),
      folder = character(),
      variable = character(),
      source_file = character(),
      source_path = character()
    )
    DBI::dbWriteTable(con, "metrics", metric_df, overwrite = TRUE)
      log_progress("Created empty metrics table")
    } else {
      warehouse_df <- tibble(
        athlete_uuid = character(),
        session_date = as.Date(character()),
        source_system = character(),
        source_athlete_id = character(),
        metric_name = character(),
        frame = integer(),
        value = numeric(),
        created_at = as.POSIXct(character())
      )
    }
  }
  
  # Create index on athletes table (local SQLite only)
  if (!use_warehouse && length(athlete_list) > 0) {
    log_progress("  Creating indexes on athletes table...")
    tryCatch({
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_athletes_uid ON athletes(uid)")
      DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_athletes_name ON athletes(name)")
      log_progress("  [SUCCESS] Created indexes on athletes table")
    }, error = function(e) {
      log_progress("  [WARNING] Could not create athlete indexes:", conditionMessage(e))
    })
  }
  
  # Close database connection
  log_progress("")
  log_progress("=", rep("=", 60), sep = "")
  log_progress("FINALIZING: Closing database connection...")
  DBI::dbDisconnect(con)
  
  # Get final database info
  use_warehouse_final <- use_warehouse
  
  if (use_warehouse_final) {
    log_progress("")
    log_progress("=", rep("=", 60), sep = "")
    log_progress("PROCESSING COMPLETE!")
    log_progress("=", rep("=", 60), sep = "")
    log_progress("Database: PostgreSQL warehouse (uais_warehouse)")
    log_progress("Athletes stored in: analytics.d_athletes")
    log_progress("Metrics stored in: f_kinematics_pitching")
    log_progress("Total athletes processed:", length(athlete_list))
    if (exists("warehouse_df") && !is.null(warehouse_df) && nrow(warehouse_df) > 0) {
      log_progress("Total metric records:", nrow(warehouse_df))
    } else {
      log_progress("Total metric records: 0")
    }
    log_progress("=", rep("=", 60), sep = "")
  } else {
    # Get final database size (local SQLite)
    db_size <- if (file.exists(DB_FILE)) file.info(DB_FILE)$size else 0
    
    log_progress("")
    log_progress("=", rep("=", 60), sep = "")
    log_progress("PROCESSING COMPLETE!")
    log_progress("=", rep("=", 60), sep = "")
    log_progress("Database file:", DB_FILE)
    log_progress("Database size:", round(db_size / 1024 / 1024, 2), "MB")
    log_progress("Total athletes:", length(athlete_list))
    if (exists("metric_df") && !is.null(metric_df) && nrow(metric_df) > 0) {
      log_progress("Total metric records:", nrow(metric_df))
    } else {
      log_progress("Total metric records: 0")
    }
    log_progress("=", rep("=", 60), sep = "")
  }
}

# ---------- Auto-run (only if not sourced from main.R) ----------
# Check if this script is being run directly (not sourced)
if (!exists("MAIN_R_SOURCING", envir = .GlobalEnv)) {
  cat("\n")
  cat("=", rep("=", 80), "\n", sep = "")
  cat("*** VERSION 2.0 - UUID MATCHING ENABLED ***\n")
  cat("=", rep("=", 80), "\n", sep = "")
  cat("\n")
  
  log_progress("=", rep("=", 60), sep = "")
  log_progress("STARTING PITCHING DATA EXTRACTION - VERSION WITH UUID MATCHING")
  log_progress("=", rep("=", 60), sep = "")
  log_progress("Database file:", DB_FILE)
  log_progress("Data root:", if (is.null(DATA_ROOT)) "Current directory (Pitching folder)" else DATA_ROOT)
  log_progress("Working directory:", getwd())
  log_progress("")
  
  start_time <- Sys.time()
  
  tryCatch({
    process_all_files()
    end_time <- Sys.time()
    duration <- difftime(end_time, start_time, units = "secs")
    log_progress("")
    log_progress("=", rep("=", 60), sep = "")
    log_progress("TOTAL PROCESSING TIME:", round(duration, 2), "seconds (", round(duration / 60, 2), "minutes)")
    log_progress("=", rep("=", 60), sep = "")
  }, error = function(e) {
    end_time <- Sys.time()
    duration <- difftime(end_time, start_time, units = "secs")
    log_progress("")
    log_progress("=", rep("=", 60), sep = "")
    log_progress("ERROR during processing (after", round(duration, 2), "seconds):")
    log_progress(conditionMessage(e))
    log_progress("=", rep("=", 60), sep = "")
    traceback()
  })
}

