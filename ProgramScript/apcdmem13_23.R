#------------------------------------------------------------------------------#
#
#                       APCD Member Data Processing Script
#
# Purpose:
# This script connects to an APCD (All-Payer Claims Database), extracts member
# data for a specified range of years, cleans the data by removing duplicates
# and members with inconsistent birth dates, and saves the final dataset.
#------------------------------------------------------------------------------#

# 1. SETUP
#------------------------------------------------------------------------------#

# Load required libraries
library(odbc)
library(DBI)
library(dplyr)
library(foreach)
library(doParallel)

# --- Configuration ---
# Set script parameters here for easy modification.

# Set the years for which member files will be processed.
YEARS_TO_PROCESS <- 13:23

# Define the DSN (Data Source Name) for your database connection.
# NOTE: Replace "your_dsn_here" with the actual DSN for your environment.
DATABASE_DSN <- "your_dsn_here"

# Specify the number of CPU cores to use for parallel processing.
# Using more cores can significantly speed up the data extraction process.
NUM_CORES <- 56

# Define the output file name for the cleaned data.
OUTPUT_FILENAME <- "cleaned_apcd_member_data_2013-2023.Rds"


# 2. PARALLEL PROCESSING SETUP
#------------------------------------------------------------------------------#

# Register a parallel backend to execute the data extraction loop across
# multiple CPU cores. This will create a "cluster" of worker processes.
message(paste("Setting up parallel cluster with", NUM_CORES, "cores..."))
cl <- makeCluster(NUM_CORES)
registerDoParallel(cl)

# Verify that the parallel backend is registered.
# This should return TRUE.
print(paste("Parallel backend registered:", getDoParRegistered()))


# 3. DATA EXTRACTION
#------------------------------------------------------------------------------#

# Use a 'foreach' loop to query and combine member data for each year in parallel.
#
# - .combine='rbind': Combines the results from each loop iteration into a
#   single data frame by row-binding.
# - .packages=c('odbc', 'DBI'): Ensures that the necessary packages are loaded
#   on each worker core, as they operate in separate environments.
#
message("Starting data extraction from APCD...")

member_data_raw <- foreach(
  year_suffix = YEARS_TO_PROCESS,
  .combine = 'rbind',
  .packages = c('odbc', 'DBI')
) %dopar% {
  
  # Note: A new database connection is established within each worker process.
  # This is necessary because database connection objects cannot be shared
  # across different parallel processes.
  con <- dbConnect(odbc::odbc(), DATABASE_DSN)
  
  # Construct the SQL query for the given year.
  # This query selects members who:
  #  - Were born between 1993 and 2019 (to target an age range of 4-19 during the study period).
  #  - Are not in dental ('DNT') or workers' compensation ('AW') plans.
  #  - Had at least one day of coverage between Jan 2013 and Dec 2023.
  query <- paste0(
    "SELECT ME998, ME013, ME014_Year_Month ",
    "FROM member_20", year_suffix, " ",
    "WHERE ME014_Year_Month BETWEEN '1993-01-01' AND '2019-12-31' ",
    "AND ME003 NOT IN ('DNT', 'AW') ",
    "AND ME162A <= '2023-12-31' AND ME163A >= '2013-01-01'"
  )
  
  # Execute the query and fetch the results.
  data <- dbGetQuery(con, query)
  
  # It's crucial to disconnect from the database within each worker.
  dbDisconnect(con)
  
  # Return the data frame for this year.
  data
}

message("Data extraction complete.")


# 4. DATA CLEANING AND PROCESSING
#------------------------------------------------------------------------------#

message("Starting data cleaning...")

# This pipeline performs several cleaning steps using dplyr:
# 1. Rename columns to be more descriptive.
# 2. Remove exact duplicate rows.
# 3. Create a unique ID for each member by combining their study and member IDs.
# 4. Identify and remove members who have multiple, conflicting birth dates
#    recorded in the database, as their data is considered unreliable.

cleaned_member_data <- member_data_raw %>%
  # Rename columns for clarity. Assuming ME998 is a study ID, ME013 is a
  # member ID, and ME014 is the birth date.
  rename(
    study_id = ME998,
    member_id = ME013,
    birth_year_month = ME014_Year_Month
  ) %>%
  
  # Remove complete duplicate rows.
  distinct() %>%
  
  # Create a single, unique identifier for each person.
  mutate(unique_member_id = paste0(study_id, member_id)) %>%
  
  # Group by the unique member ID to check for data inconsistencies.
  group_by(unique_member_id) %>%
  
  # Filter to keep only members who have exactly one distinct birth date.
  # n_distinct() counts the number of unique values in a column for each group.
  filter(n_distinct(birth_year_month) == 1) %>%
  
  # Ungroup to remove the grouping structure for subsequent operations.
  ungroup() %>%
  
  # Remove any remaining duplicate rows that might have been introduced
  # if a member had the same birth date listed multiple times.
  distinct(unique_member_id, .keep_all = TRUE)


message(paste(
  "Cleaning complete. Removed",
  nrow(member_data_raw) - nrow(cleaned_member_data),
  "rows."
))


# 5. SAVE AND CLEAN UP
#------------------------------------------------------------------------------#

# Save the final, cleaned data frame to an R-specific data file (.Rds).
# Using saveRDS is efficient for storing single R objects.
message(paste("Saving cleaned data to", OUTPUT_FILENAME, "..."))
saveRDS(cleaned_member_data, file = OUTPUT_FILENAME)

# Stop the parallel cluster to release the allocated CPU cores.
# This is a critical step to free up system resources.
stopCluster(cl)

message("Script finished successfully.")
