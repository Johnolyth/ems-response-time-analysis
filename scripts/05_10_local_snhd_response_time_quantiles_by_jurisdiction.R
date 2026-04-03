## 05_10_local_snhd_response_time_quantiles_by_jurisdiction.R
## SNHD response-time distribution metrics (median / p90 / p95) by jurisdiction
## Inputs:
##   - data/clean/04_canonical/local_snhd_times
##   - data/clean/04_canonical/local_snhd_geo
## Output:
##   - output/tables/analysis/local_snhd_response_time_quantiles_by_jurisdiction.csv

source("scripts/05_00_analysis_setup.R")

# -----------------------------
# Load canonical datasets
# -----------------------------
times <- canon_ds("local_snhd_times")
geo <- canon_ds("local_snhd_geo")

# -----------------------------
# Validate columns
# -----------------------------
req_times <- c("pcr_key", "ems_system_response_time_min")
req_geo <- c("pcr_key", "jurisdiction_name", "is_operational_noise", "urb_sub_focus")

missing_times <- setdiff(req_times, times$schema$names)
missing_geo <- setdiff(req_geo, geo$schema$names)

if (length(missing_times) > 0) fail("local_snhd_times missing columns: {paste(missing_times, collapse = ', ')}")
if (length(missing_geo) > 0) fail("local_snhd_geo missing columns: {paste(missing_geo, collapse = ', ')}")

# -----------------------------
# Base (Arrow lazy)
# -----------------------------
base <- times %>%
    select(pcr_key, ems_system_response_time_min) %>%
    filter(!is.na(ems_system_response_time_min)) %>%
    left_join(geo, by = "pcr_key") %>%
    # drop known operational noise + NA jurisdictions
    filter(is_operational_noise == FALSE) %>%
    filter(!is.na(jurisdiction_name))

msg("Computing SNHD response-time quantiles by jurisdiction (this may take a few minutes)...")

summarise_quantiles <- function(df) {
    df %>%
        summarise(
            n = n(),
            median_sys = quantile(ems_system_response_time_min, 0.50),
            p90_sys = quantile(ems_system_response_time_min, 0.90),
            p95_sys = quantile(ems_system_response_time_min, 0.95)
        )
}

# -----------------------------
# Overall (all non-noise)
# -----------------------------
overall_all <- base %>%
    summarise_quantiles() %>%
    collect() %>%
    mutate(group_level = "overall_all")

# -----------------------------
# By jurisdiction (all non-noise)
# -----------------------------
by_juris_all <- base %>%
    group_by(jurisdiction_name) %>%
    summarise_quantiles() %>%
    collect() %>%
    mutate(group_level = "by_jurisdiction_all")

# -----------------------------
# Urban/Suburban focus subset
# -----------------------------
base_focus <- base %>% filter(urb_sub_focus == TRUE)

overall_focus <- base_focus %>%
    summarise_quantiles() %>%
    collect() %>%
    mutate(group_level = "overall_urb_sub_focus")

by_juris_focus <- base_focus %>%
    group_by(jurisdiction_name) %>%
    summarise_quantiles() %>%
    collect() %>%
    mutate(group_level = "by_jurisdiction_urb_sub_focus")

# -----------------------------
# Combine + write
# -----------------------------
out <- bind_rows(
    overall_all %>% mutate(jurisdiction_name = NA_character_),
    by_juris_all,
    overall_focus %>% mutate(jurisdiction_name = NA_character_),
    by_juris_focus
) %>%
    relocate(group_level, jurisdiction_name)

write_csv(out, "local_snhd_response_time_quantiles_by_jurisdiction.csv")
