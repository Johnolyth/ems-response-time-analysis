## 05_01_response_time_core.R
## Baseline response-time metrics + geographic stratification
## Inputs: data/clean/04_canonical/nemsis_times, nemsis_geo (+ optional delays)
## Output: output/tables/analysis/response_time_core_by_geo.csv

source("scripts/05_00_analysis_setup.R")

# -----------------------------
# Load canonical datasets
# -----------------------------
times <- canon_ds("nemsis_times")
geo <- canon_ds("nemsis_geo")

# Optional delay datasets (present per your schema printout)
dispatch_delays <- if (canon_exists("nemsis_dispatch_delays")) canon_ds("nemsis_dispatch_delays") else NULL
response_delays <- if (canon_exists("nemsis_response_delays")) canon_ds("nemsis_response_delays") else NULL

# -----------------------------
# Build analysis base (Arrow lazy)
# -----------------------------
base <- times %>%
    left_join(geo, by = "pcr_key")

if (!is.null(dispatch_delays)) {
    base <- base %>% left_join(dispatch_delays, by = "pcr_key")
}
if (!is.null(response_delays)) {
    base <- base %>% left_join(response_delays, by = "pcr_key")
}

# -----------------------------
# Validate required columns exist (check source datasets, not the lazy join)
# -----------------------------
req_times <- c(
    "pcr_key",
    "ems_system_response_time_min",
    "ems_scene_response_time_min",
    "ems_total_call_time_min"
)
req_geo <- c(
    "pcr_key",
    "us_census_region",
    "us_census_division",
    "nasemso_region",
    "urbancity"
)

times_cols <- canon_cols("nemsis_times")
geo_cols <- canon_cols("nemsis_geo")

missing_times <- setdiff(req_times, times_cols)
missing_geo <- setdiff(req_geo, geo_cols)

if (length(missing_times) > 0) fail("nemsis_times missing columns: {paste(missing_times, collapse = ', ')}")
if (length(missing_geo) > 0) fail("nemsis_geo missing columns: {paste(missing_geo, collapse = ', ')}")

# -----------------------------
# Helper: core summary expression (Arrow compatible)
# -----------------------------
# NOTE: keep to Arrow-friendly aggregates: n, mean, min/max, proportions, missingness.
summarise_core <- function(df) {
    df %>%
        summarise(
            n = n(),

            # Missingness (system response time as primary)
            n_missing_sys = sum(is.na(ems_system_response_time_min)),
            pct_missing_sys = mean(is.na(ems_system_response_time_min)),

            # Means (will ignore NA by default in Arrow? We make explicit NA-safe filters below)
            mean_sys = mean(ems_system_response_time_min, na.rm = TRUE),
            mean_scene = mean(ems_scene_response_time_min, na.rm = TRUE),
            mean_total = mean(ems_total_call_time_min, na.rm = TRUE),

            # Basic ranges
            min_sys = min(ems_system_response_time_min, na.rm = TRUE),
            max_sys = max(ems_system_response_time_min, na.rm = TRUE),

            # Coverage-style thresholds (modifiable later)
            pct_sys_le_4 = mean(ems_system_response_time_min <= 4, na.rm = TRUE),
            pct_sys_le_8 = mean(ems_system_response_time_min <= 8, na.rm = TRUE),
            pct_sys_le_15 = mean(ems_system_response_time_min <= 15, na.rm = TRUE)
        )
}

# -----------------------------
# Overall + by-geo summaries
# -----------------------------
overall <- base %>%
    mutate(group_level = "overall") %>%
    summarise_core() %>%
    collect()

by_geo <- base %>%
    group_by(us_census_region, us_census_division, nasemso_region, urbancity) %>%
    summarise_core() %>%
    collect()

# Bind with consistent columns
overall_row <- overall %>%
    mutate(
        us_census_region = NA_character_,
        us_census_division = NA_character_,
        nasemso_region = NA_character_,
        urbancity = NA_character_
    ) %>%
    select(us_census_region, us_census_division, nasemso_region, urbancity, everything())

out <- bind_rows(
    overall_row %>% mutate(group_level = "overall"),
    by_geo %>% mutate(group_level = "geo")
) %>%
    relocate(group_level, us_census_region, us_census_division, nasemso_region, urbancity)

# -----------------------------
# Write output
# -----------------------------
write_csv(out, "response_time_core_by_geo.csv")
