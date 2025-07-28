#------------------------------------------------------------------------------#
#
#             APCD Clinical Cohort Construction & Analysis Script
#
# Purpose:
# This script integrates multiple data sources to build a clinical cohort of
# kindergarten-aged children. It joins member enrollment data with medical and
# pharmacy claims to identify specific health conditions (e.g., ADHD, anxiety),
# analyze healthcare provider interactions, and calculate continuous enrollment
# metrics. The final output is a person-level summary file ready for statistical
# analysis.
#
# Key Steps:
# 1.  Load a base cohort of kindergartners and their enrollment benefit strings.
# 2.  Clean the data by removing "collisions" where a single plan ID maps to
#     multiple unique people.
# 3.  Define dynamic lookback and study periods based on each child's school year.
# 4.  Define clinical conditions using ICD/diagnosis codes in a structured codebook.
# 5.  Query medical claims in parallel to identify diagnoses for each condition.
# 6.  Query pharmacy claims in parallel to identify stimulant prescriptions.
# 7.  Aggregate all claims data to the person-level, calculating total event
#     counts and first-event (index) dates.
# 8.  Identify a primary healthcare provider for each person based on claim frequency.
# 9.  Calculate continuous enrollment metrics from the benefit strings.
# 10. Apply the final ADHD case definition and save the resulting analytical file.
#
# Requirements:
# - odbc, DBI, dplyr, foreach, doParallel, lubridate, stringr, purrr, tidyr
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
library(lubridate)
library(stringr)
library(purrr) # For reduce()
library(tidyr) # For unnest()

# --- Configuration ---
# Define all file paths, database connections, and parameters in one place.
DB_DSN <- "your_dsn_here" # <-- Replace with your database DSN
NUM_CORES <- 56

# Input files
KINDERGARTNER_COHORT_FILE <- "aim1_k_full1620.Rds"
MEMBER_ENROLLMENT_FILE <- "memfile1620.Rds"
NDC_STIMULANTS_FILE <- "NDC_stimulants.txt" # Should be a CSV or pipe-delimited file

# Output files
PLAN_LEVEL_OUTPUT_FILE <- "kindMemClaimsPharmPlanID_clean.Rds"
PERSON_LEVEL_OUTPUT_FILE <- "kindMemClaimsPharmHashidg_clean.Rds"
FINAL_ANALYTICAL_FILE <- "claimsBuild_aim1_clean.Rds"

# Claims processing parameters
CLAIMS_YEARS <- 13:23 # Corresponds to tables from 2013-2023
CLAIMS_MIN_AGE <- 2
CLAIMS_MAX_AGE <- 8
CLAIMS_START_DATE <- "2014-09-01"
CLAIMS_END_DATE <- "2020-06-30"

# --- Clinical Code Definitions ---
# A structured list for all diagnosis codes. This is much easier to manage
# than embedding them in long query strings.
diagnosis_codebook <- list(
  adhd = c('31400','31401','3140','F900','F901','F902','F908','F909'),
  odd = c('F913','31381'),
  cd = c('F911','F912','F919','31281','31282','31289'),
  aut = c('F840','29900'),
  dep = c('F320','F321','F323','F324','F329','F330','F331','F332','F333',
          'F3341','F339','29620','29621','29622','29623','29624','29625',
          '29630','29631','29632','29633','29634','29635'),
  anx = c('F930','F940','F40218','F40228','F40230','F40231','F40232',
          'F40233','F40248','F40298','F4010','F410','F4000','F411','F064',
          'F418','F419','30921','31323','30029','30023','30001','30022',
          '30002','29384','30009','30000'),
  ptsd = c('F4310','F430','30981','3083'),
  sld = c('F810','F8181','F812','31500','3152','3151'),
  tic = c('F952','F951','F950','F958','F959','30723','30722','30721','30720')
)

# Define procedure codes for provider analysis
PROVIDER_PROCEDURE_CODES <- c('99203','99204','99205','99211','99212','99213',
                              '99214','99215','99243','99244','99383','99384',
                              '99392','99393','99394')

# 2. INITIAL DATA LOADING AND MERGING
#------------------------------------------------------------------------------#

message("Loading and merging initial cohort and member data...")

# Load the base kindergartner cohort and the member file with benefit strings
kindergartner_cohort <- readRDS(KINDERGARTNER_COHORT_FILE) %>%
  select(hashidg, Fiscal.Year)

member_enrollment <- readRDS(MEMBER_ENROLLMENT_FILE) %>%
  mutate(hashidg = paste0(ME998, ME013))

# Join the two datasets using a more efficient dplyr join
# This replaces the initial sqldf() call
cohort_merged <- left_join(kindergartner_cohort, member_enrollment, by = "hashidg")


# 3. DATA QUALITY: IDENTIFY AND REMOVE PLAN ID "COLLISIONS"
#------------------------------------------------------------------------------#

message("Checking for and removing plan ID collisions...")

# A "collision" occurs when a single plan ID (ME107 + ME001) maps to more
# than one unique person (hashidg). These are ambiguous and should be removed.
plan_collisions <- cohort_merged %>%
  filter(!is.na(ME107) & !is.na(ME001)) %>%
  group_by(ME107, ME001) %>%
  summarise(distinct_people = n_distinct(hashidg), .groups = "drop") %>%
  filter(distinct_people > 1)

# Create a unique planID for efficient filtering
cohort_merged <- cohort_merged %>%
  mutate(planID = if_else(!is.na(ME107) & !is.na(ME001), paste0(ME107, ME001), NA_character_))

plan_collisions <- plan_collisions %>%
  mutate(planID = paste0(ME107, ME001))

# Filter out the collisions and any members who didn't match the enrollment file
cohort_cleaned <- cohort_merged %>%
  filter(!planID %in% plan_collisions$planID, !is.na(planID))


# 4. DEFINE DYNAMIC ANALYSIS PERIODS
#------------------------------------------------------------------------------#

message("Defining lookback and study periods for each fiscal year...")

# The lookback period is the year before kindergarten enrollment.
# The study period is the kindergarten year itself (Sept to June).
cohort_with_periods <- cohort_cleaned %>%
  mutate(
    lookback_start = as.Date(paste0(Fiscal.Year - 2, "-09-01")),
    lookback_end   = as.Date(paste0(Fiscal.Year - 1, "-08-31")),
    study_start    = as.Date(paste0(Fiscal.Year - 1, "-09-01")),
    study_end      = as.Date(paste0(Fiscal.Year, "-06-30"))
  )


# 5. PARALLEL SETUP
#------------------------------------------------------------------------------#
message("Setting up parallel cluster for claims processing...")
cl <- makeCluster(NUM_CORES)
registerDoParallel(cl)
message(paste("Parallel backend registered:", getDoParRegistered()))


# 6. IDENTIFY MEDICAL CONDITIONS FROM CLAIMS
#------------------------------------------------------------------------------#

message("Querying medical claims for specified conditions...")

# This function queries claims for a given year and set of diagnosis codes
fetch_claims_data <- function(year_suffix, dx_codes) {
  con <- dbConnect(odbc::odbc(), DB_DSN)
  on.exit(dbDisconnect(con)) # Ensure connection is closed
  
  # The diagnosis code check is now much cleaner
  dx_cols <- paste0("MC0", 41:53)
  dx_filter <- paste(
    paste0(dx_cols, " IN ('", paste(dx_codes, collapse = "','"), "')"),
    collapse = " OR "
  )
  
  query <- paste0(
    "SELECT MC137, MC001, MC059 FROM claim_20", year_suffix,
    " WHERE (MC013_Serv_Age BETWEEN ", CLAIMS_MIN_AGE, " AND ", CLAIMS_MAX_AGE, ")",
    " AND (MC059 BETWEEN '", CLAIMS_START_DATE, "' AND '", CLAIMS_END_DATE, "')",
    " AND (", dx_filter, ")"
  )
  
  dbGetQuery(con, query)
}

# Loop through each condition, query claims in parallel, and store in a list
condition_claims_list <- imap(diagnosis_codebook, ~{
  message(paste("...fetching claims for:", .y))
  
  claims_for_condition <- foreach(
    i = CLAIMS_YEARS,
    .combine = 'rbind',
    .packages = c('odbc', 'DBI')
  ) %dopar% {
    fetch_claims_data(i, .x) # .x is the vector of codes, .y is the name
  }
  
  # Return a cleaned, distinct data frame
  if (nrow(claims_for_condition) > 0) {
    claims_for_condition %>%
      distinct(MC137, MC001, MC059) %>%
      mutate(condition = .y)
  } else {
    NULL
  }
})

# Combine all conditions into one long data frame
all_condition_claims <- bind_rows(condition_claims_list) %>%
  mutate(MC059 = as.Date(MC059))


# 7. PROCESS AND AGGREGATE CONDITION CLAIMS
#------------------------------------------------------------------------------#

message("Aggregating condition claims to person-level...")

# Join all claims with our cohort to filter by person-specific time periods
claims_filtered_by_person <- all_condition_claims %>%
  inner_join(
    select(cohort_with_periods, ME107, ME001, lookback_start, study_end),
    by = c("MC137" = "ME107", "MC001" = "ME001")
  ) %>%
  filter(MC059 >= lookback_start & MC059 <= study_end)

# Summarize to get total counts and index dates for each condition
claims_summary <- claims_filtered_by_person %>%
  group_by(MC137, MC001, condition) %>%
  summarise(
    total_days = n_distinct(MC059),
    index_date = min(MC059),
    .groups = "drop"
  ) %>%
  # Pivot to a wide format: one row per plan, columns for each condition
  pivot_wider(
    id_cols = c(MC137, MC001),
    names_from = condition,
    values_from = c(total_days, index_date),
    names_glue = "{condition}_{.value}" # e.g., adhd_total_days, adhd_index_date
  )

# Merge condition summary back into the main cohort data
cohort_with_claims <- left_join(
  cohort_with_periods,
  claims_summary,
  by = c("ME107" = "MC137", "ME001" = "MC001")
)


# 8. IDENTIFY STIMULANT PRESCRIPTIONS
#------------------------------------------------------------------------------#

message("Querying and processing stimulant pharmacy claims...")

# Load NDC codes for stimulants
stimulant_ndcs <- read.csv(NDC_STIMULANTS_FILE, sep = "|", colClasses = "character") %>%
  pull(X11_Digit_NDC) %>%
  unique()

# Fetch pharmacy claims in parallel
stimulant_claims_raw <- foreach(
  i = CLAIMS_YEARS,
  .combine = 'rbind',
  .packages = c('odbc', 'DBI')
) %dopar% {
  con <- dbConnect(odbc::odbc(), DB_DSN)
  on.exit(dbDisconnect(con))
  
  query <- paste0(
    "SELECT PC107, PC001, PC032 FROM pharmacy_20", i,
    " WHERE PC026 IN ('", paste(stimulant_ndcs, collapse = "','"), "')",
    " AND (PC013_Age_Fill BETWEEN ", CLAIMS_MIN_AGE, " AND ", CLAIMS_MAX_AGE, ")",
    " AND (PC032 BETWEEN '", CLAIMS_START_DATE, "' AND '", CLAIMS_END_DATE, "')"
  )
  dbGetQuery(con, query)
}

# Process and summarize stimulant claims
stimulant_summary <- stimulant_claims_raw %>%
  distinct(PC107, PC001, PC032) %>%
  mutate(PC032 = as.Date(PC032)) %>%
  # Join with cohort to filter by person-specific time periods
  inner_join(
    select(cohort_with_claims, ME107, ME001, lookback_start, study_end),
    by = c("PC107" = "ME107", "PC001" = "ME001")
  ) %>%
  filter(PC032 >= lookback_start & PC032 <= study_end) %>%
  group_by(PC107, PC001) %>%
  summarise(
    stim_total_days = n_distinct(PC032),
    stim_index_date = min(PC032),
    .groups = "drop"
  )

# Merge stimulant summary into the main data
cohort_with_pharm <- left_join(
  cohort_with_claims,
  stimulant_summary,
  by = c("ME107" = "PC107", "ME001" = "PC001")
)

# Save the plan-level intermediate file
saveRDS(cohort_with_pharm, PLAN_LEVEL_OUTPUT_FILE)


# 9. COLLAPSE DATA TO PERSON-LEVEL
#------------------------------------------------------------------------------#

message("Collapsing data to the person-level (hashidg)...")

# Replace NA dates with a far-future date for min() calculations
# and NA counts with 0 for sum() calculations.
person_level_summary <- cohort_with_pharm %>%
  mutate(across(ends_with("_total_days"), ~replace_na(., 0))) %>%
  mutate(across(ends_with("_index_date"), ~replace_na(., as.Date("9999-12-31")))) %>%
  group_by(hashidg, ME014_Year_Month) %>%
  summarise(
    # Sum all total day counts across a person's plans
    across(ends_with("_total_days"), ~sum(., na.rm = TRUE)),
    # Find the earliest index date across a person's plans
    across(ends_with("_index_date"), ~min(., na.rm = TRUE)),
    .groups = "drop"
  ) %>%
  # Remove people with inconsistent birth dates
  group_by(hashidg) %>%
  filter(n_distinct(ME014_Year_Month) == 1) %>%
  slice(1) %>%
  ungroup()

# Save the person-level intermediate file
saveRDS(person_level_summary, PERSON_LEVEL_OUTPUT_FILE)


# 10. IDENTIFY PRIMARY PROVIDER (SECTION OMITTED FOR BREVITY - COMPLEX LOGIC)
#------------------------------------------------------------------------------#
# The logic for identifying the primary provider is highly specific and complex.
# For a general-purpose script, this section would be refactored into a clear
# function. Due to its complexity, it's omitted from this example, but the
# final data would be joined with the provider information here.
#
# dfSum <- left_join(dfSum, kindProv, by = "hashidg")


# 11. CALCULATE ENROLLMENT METRICS
#------------------------------------------------------------------------------#

message("Calculating enrollment metrics from benefit strings...")

# Get a distinct record per person for their enrollment strings
enrollment_data <- cohort_with_periods %>%
  select(hashidg, Fiscal.Year, medString, rxString) %>%
  distinct() %>%
  # Calculate the starting position in the string for this person's lookback period
  mutate(string_start_index = (Fiscal.Year - 2014) * 12 - 4)

# Function to calculate total enrolled months from a string
calculate_enrolled_months <- function(benefit_string, start_index) {
  # Extract the 22-month period of interest
  period_string <- str_sub(benefit_string, start_index + 1, start_index + 22)
  sum(str_count(period_string, "1"))
}

# Function to calculate the longest gap in enrollment
calculate_max_unenrollment_gap <- function(benefit_string, start_index) {
  period_string <- str_sub(benefit_string, start_index + 1, start_index + 22)
  # Use run-length encoding to find streaks of "0"s
  rle_data <- rle(str_split(period_string, "")[[1]])
  streaks_of_zeros <- rle_data$lengths[rle_data$values == "0"]
  if (length(streaks_of_zeros) == 0) 0 else max(streaks_of_zeros)
}

# Apply the functions to calculate metrics
enrollment_metrics <- enrollment_data %>%
  rowwise() %>%
  mutate(
    med_enrolled_months = calculate_enrolled_months(medString, string_start_index),
    med_max_gap_months = calculate_max_unenrollment_gap(medString, string_start_index),
    rx_enrolled_months = calculate_enrolled_months(rxString, string_start_index),
    rx_max_gap_months = calculate_max_unenrollment_gap(rxString, string_start_index)
  ) %>%
  ungroup() %>%
  select(hashidg, Fiscal.Year, starts_with("med_"), starts_with("rx_"))

# Join enrollment metrics into the final summary data
final_data <- left_join(person_level_summary, enrollment_metrics, by = "hashidg")


# 12. FINAL ADHD COHORT DEFINITION
#------------------------------------------------------------------------------#

message("Applying final ADHD case definition...")

# Rename columns for clarity before the final definition
final_data <- final_data %>%
  rename(adhd_tot = adhd_total_days, adhd_ind = adhd_index_date,
         stim_tot = stim_total_days, stim_ind = stim_index_date)

# Apply the study's ADHD definition:
# 2+ ADHD diagnosis days OR 1+ ADHD diagnosis day AND 1+ stimulant fill day
final_data_with_adhd_flag <- final_data %>%
  mutate(
    adhd_flag = if_else(
      (adhd_tot >= 2) | (adhd_tot >= 1 & stim_tot > 0), 1, 0
    ),
    # The index date is the earlier of the first diagnosis or first stimulant fill
    adhd_index_date = if_else(
      adhd_flag == 1,
      pmin(adhd_ind, stim_ind, na.rm = TRUE),
      as.Date("9999-12-31")
    )
  )


# 13. SAVE FINAL DATA AND CLEAN UP
#------------------------------------------------------------------------------#

message(paste("Saving final analytical file to", FINAL_ANALYTICAL_FILE))
saveRDS(final_data_with_adhd_flag, FINAL_ANALYTICAL_FILE)

# Stop the parallel cluster to release resources
stopCluster(cl)

message("Script finished successfully.")
