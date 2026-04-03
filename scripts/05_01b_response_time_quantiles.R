## 05_01b_response_time_quantiles.R
## Distributional response-time metrics (median / p90 / p95) — Urban/Suburban focus
## Inputs: nemsis_times, nemsis_geo
## Output: output/tables/analysis/response_time_quantiles_urban_suburban_by_geo.csv

source("scripts/05_00_analysis_setup.R")

# -----------------------------
# Load canonical datasets
# -----------------------------
times <- canon_ds("nemsis_times")
geo <- canon_ds("nemsis_geo")

# -----------------------------
# Validate required columns
# -----------------------------
req_times <- c("pcr_key", "ems_system_response_time_min")
req_geo <- c("pcr_key", "us_census_region", "us_census_division", "nasemso_region", "urbancity")

missing_times <- setdiff(req_times, canon_cols("nemsis_times"))
missing_geo <- setdiff(req_geo, canon_cols("nemsis_geo"))

if (length(missing_times) > 0) fail("nemsis_times missing columns: {paste(missing_times, collapse = ', ')}")
if (length(missing_geo) > 0) fail("nemsis_geo missing columns: {paste(missing_geo, collapse = ', ')}")

# -----------------------------
# Build base (Arrow lazy) — Urban/Suburban only
# -----------------------------
base <- times %>%
    select(pcr_key, ems_system_response_time_min) %>%
    filter(!is.na(ems_system_response_time_min)) %>%
    left_join(geo, by = "pcr_key") %>%
    filter_urb_sub()

msg("Computing response-time quantiles (Urban/Suburban only; this may take several minutes)...")

# -----------------------------
# Quantile summaries
# -----------------------------
summarise_quantiles <- function(df) {
    df %>%
        summarise(
            n = n(),
            median_sys = quantile(ems_system_response_time_min, 0.50),
            p90_sys = quantile(ems_system_response_time_min, 0.90),
            p95_sys = quantile(ems_system_response_time_min, 0.95)
        )
}

overall <- base %>%
    summarise_quantiles() %>%
    collect()

by_geo <- base %>%
    group_by(us_census_region, us_census_division, nasemso_region, urbancity) %>%
    summarise_quantiles() %>%
    collect()

overall_row <- overall %>%
    mutate(
        us_census_region   = NA_character_,
        us_census_division = NA_character_,
        nasemso_region     = NA_character_,
        urbancity          = NA_character_,
        group_level        = "overall_urb_sub"
    )

out <- bind_rows(
    overall_row,
    by_geo %>% mutate(group_level = "geo_urb_sub")
) %>%
    relocate(group_level, us_census_region, us_census_division, nasemso_region, urbancity)

# -----------------------------
# Write output
# -----------------------------
write_csv(out, "response_time_quantiles_urban_suburban_by_geo.csv")
