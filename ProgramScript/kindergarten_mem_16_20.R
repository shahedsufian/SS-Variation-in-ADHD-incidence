#------------------------------------------------------------------------------#
#
#           APCD Monthly Member Enrollment & Benefits Analysis Script
#
# Purpose:
# This script connects to an All-Payer Claims Database (APCD), extracts member
# enrollment data for a specific cohort of children, and then transforms the
# data from date ranges into a monthly panel format. It calculates whether
# each member had medical and pharmacy benefits for each month over a defined
# period, handling complex cases of overlapping or partial-month coverage.
#
# Key Logic:
# 1. Extracts member plan data for a cohort defined by birth date and coverage period.
# 2. Creates a complete timeline of all months to be analyzed.
# 3. For each member plan, it identifies all months that overlap with the plan's
#    coverage period.
# 4. A member is considered covered in a given month if they have an active plan
#    with the relevant benefit (medical or pharmacy) for at least 15 days
#    of that month.
# 5. Aggregates this data to create a single coverage status per member per month.
# 6. Finally, it creates binary string summaries representing the member's
#    coverage history over the entire period.
#
# Requirements:
# - odbc, DBI, dplyr, foreach, doParallel, tidyr
#
#------------------------------------------------------------------------------#

# 1. SETUP
#------------------------------------------------------------------------------#

# Load required libraries
library(odbc)
library(DBI)
library(dplyr)
library(foreach)
library(doParallel)
library(tidyr) # Used for pivot_wider and unite

# --- Configuration ---
# Set script parameters here for easy modification.

# Database and Parallelism
DATABASE_DSN <- "your_dsn_here" # <-- Replace with your DSN
NUM_CORES <- 56

# Data Extraction Parameters
YEAR_SUFFIXES_TO_PROCESS <- 13:23 # For tables 'member_2013' to 'member_2023'
MIN_BIRTH_DATE <- "2006-06-01"
MAX_BIRTH_DATE <- "2016-07-31"
STUDY_PERIOD_START <- "2014-09-01"
STUDY_PERIOD_END <- "2020-06-30"

# Monthly Flag Generation Parameters
ANALYSIS_START_YEAR <- 2013
ANALYSIS_END_YEAR <- 2023
MIN_COVERAGE_DAYS <- 15 # Min days of coverage to count for a month

# Output File
OUTPUT_FILENAME <- "member_enrollment_strings_2014-2020.Rds"


# 2. PARALLEL PROCESSING SETUP
#------------------------------------------------------------------------------#

message(paste("Setting up parallel cluster with", NUM_CORES, "cores..."))
cl <- makeCluster(NUM_CORES)
registerDoParallel(cl)
message(paste("Parallel backend registered:", getDoParRegistered()))


# 3. DATA EXTRACTION
#------------------------------------------------------------------------------#

message("Starting data extraction from APCD...")

member_data_raw <- foreach(
  year_suffix = YEAR_SUFFIXES_TO_PROCESS,
  .combine = 'rbind',
  .packages = c('odbc', 'DBI')
) %dopar% {
  
  # A new database connection is established within each worker process.
  con <- dbConnect(odbc::odbc(), DATABASE_DSN)
  
  # This query selects members based on the configured cohort parameters.
  query <- paste0(
    "SELECT ME998, ME013, ME107, ME001, ME014_Year_Month, ME018, ME019, ME162A, ME163A ",
    "FROM member_20", year_suffix, " ",
    "WHERE ME014_Year_Month BETWEEN '", MIN_BIRTH_DATE, "' AND '", MAX_BIRTH_DATE, "' ",
    "AND ME003 NOT IN ('DNT', 'AW') ",
    "AND ME162A <= '", STUDY_PERIOD_END, "' AND ME163A >= '", STUDY_PERIOD_START, "'"
  )
  
  data <- dbGetQuery(con, query)
  dbDisconnect(con)
  data
}

message("Data extraction complete.")


# 4. INITIAL DATA CLEANING & PREPARATION
#------------------------------------------------------------------------------#

message("Performing initial data cleaning...")

# This pipeline renames columns, creates unique IDs, converts data types,
# and removes exact duplicate plan records.
member_plans <- member_data_raw %>%
  rename(
    study_id = ME998,
    gender_id = ME013,
    member_submitter_id = ME107,
    submitter_id = ME001,
    birth_ym = ME014_Year_Month,
    has_medical_benefit = ME018,
    has_pharmacy_benefit = ME019,
    plan_start_date = ME162A,
    plan_end_date = ME163A
  ) %>%
  # Remove fully duplicated rows
  distinct() %>%
  # Ensure dates are in Date format for calculations
  mutate(
    plan_start_date = as.Date(plan_start_date),
    plan_end_date = as.Date(plan_end_date),
    # Create unique identifiers for a person and a specific plan
    unique_person_id = paste(study_id, gender_id, sep = "_"),
    unique_plan_id = paste(member_submitter_id, submitter_id, sep = "_")
  )


# 5. GENERATE MONTHLY ENROLLMENT FLAGS (OPTIMIZED)
#------------------------------------------------------------------------------#

message("Generating monthly enrollment flags (optimized method)...")

# --- Step 5.1: Create a data frame of all months to analyze ---
# This creates a reference table with the start and end date for every
# month in the analysis period.
analysis_months <- data.frame(
  month_start = seq(as.Date(paste0(ANALYSIS_START_YEAR, "-01-01")),
                    as.Date(paste0(ANALYSIS_END_YEAR, "-12-01")),
                    by = "month")
) %>%
  mutate(
    # Calculate the end of the month
    month_end = ceiling_date(month_start, "month") - 1,
    # Create a year-month label for joining and naming later
    year_month_label = format(month_start, "%Y.%m")
  )

# --- Step 5.2: Find all overlaps between member plans and months ---
# This is the core of the optimization. Instead of looping, we find all
# plan-month combinations that have overlapping dates.
# A plan [S, E] overlaps with a month [MS, ME] if S <= ME and E >= MS.
monthly_coverage <- member_plans %>%
  # Use a cross-join to create all possible combinations of plans and months
  crossing(analysis_months) %>%
  # Filter to keep only the real overlaps
  filter(plan_start_date <= month_end & plan_end_date >= month_start) %>%
  # --- Step 5.3: Calculate coverage days and apply the 15-day rule ---
  mutate(
    # Find the actual start and end of coverage within the month
    coverage_start_in_month = pmax(plan_start_date, month_start),
    coverage_end_in_month = pmin(plan_end_date, month_end),
    # Calculate the number of covered days
    days_in_month = as.numeric(coverage_end_in_month - coverage_start_in_month) + 1,
    # Apply the 15-day rule to determine if the benefit counts for the month
    med_covered = ifelse(has_medical_benefit == 1 & days_in_month >= MIN_COVERAGE_DAYS, 1, 0),
    rx_covered = ifelse(has_pharmacy_benefit == 1 & days_in_month >= MIN_COVERAGE_DAYS, 1, 0)
  )

# --- Step 5.4: Aggregate to get one coverage status per person per month ---
# A person may have multiple plans in a month. We take the maximum coverage
# status (if any plan provides coverage, they are covered).
person_monthly_summary <- monthly_coverage %>%
  group_by(unique_person_id, birth_ym, year_month_label) %>%
  summarise(
    med_flag = max(med_covered, na.rm = TRUE),
    rx_flag = max(rx_covered, na.rm = TRUE),
    .groups = 'drop'
  )


# 6. CREATE FINAL DATASET WITH ENROLLMENT STRINGS
#------------------------------------------------------------------------------#

message("Aggregating flags and creating final enrollment strings...")

# --- Step 6.1: Pivot data to wide format ---
# This transforms the data from long (one row per person-month) to wide
# (one row per person, with columns for each month's flag).
panel_data <- person_monthly_summary %>%
  pivot_wider(
    id_cols = c(unique_person_id, birth_ym),
    names_from = year_month_label,
    values_from = c(med_flag, rx_flag),
    names_prefix = "flag_",
    names_sep = "_",
    values_fill = 0 # Fill non-covered months with 0
  )

# --- Step 6.2: Create binary strings from the monthly flags ---
# This concatenates the monthly flags into a single string for easy summary.
final_member_data <- panel_data %>%
  # Unite columns that start with 'med_flag_' into a single 'med_string'
  unite(
    "med_string",
    starts_with("med_flag"),
    sep = "",
    remove = TRUE
  ) %>%
  # Unite columns that start with 'rx_flag_' into a single 'rx_string'
  unite(
    "rx_string",
    starts_with("rx_flag"),
    sep = "",
    remove = TRUE
  )

message("Data processing and aggregation complete.")


# 7. SAVE AND CLEAN UP
#------------------------------------------------------------------------------#

message(paste("Saving final data to", OUTPUT_FILENAME, "..."))
saveRDS(final_member_data, file = OUTPUT_FILENAME)

# Stop the parallel cluster to release system resources.
stopCluster(cl)

message("Script finished successfully.")
