## 05_11_local_snhd_tail_component_breakdown.R
## SNHD p95 tail component breakdown (dispatch/chute/scene shares of p95 system response)
##
## Inputs:
##   - data/clean/04_canonical/local_snhd_times
##   - data/clean/04_canonical/local_snhd_geo
## Output:
##   - output/tables/analysis/local_snhd_p95_tail_component_breakdown.csv

source("scripts/05_00_analysis_setup.R")

# -----------------------------
# Load canonical datasets
# -----------------------------
times <- canon_ds("local_snhd_times")
geo <- canon_ds("local_snhd_geo")

# -----------------------------
# Validate columns
# -----------------------------
req_times <- c(
    "pcr_key",
    "ems_dispatch_center_time_sec",
    "ems_chute_time_min",
    "ems_system_response_time_min",
    "ems_scene_response_time_min"
)

req_geo <- c(
    "pcr_key",
    "jurisdiction_name",
    "is_operational_noise",
    "urb_sub_focus"
)

missing_times <- setdiff(req_times, times$schema$names)
missing_geo <- setdiff(req_geo, geo$schema$names)

if (length(missing_times) > 0) fail("local_snhd_times missing columns: {paste(missing_times, collapse = ', ')}")
if (length(missing_geo) > 0) fail("local_snhd_geo missing columns: {paste(missing_geo, collapse = ', ')}")

# -----------------------------
# Base (Arrow lazy)
# -----------------------------
base <- times %>%
    select(all_of(req_times)) %>%
    # only keep calls where we have a system response time (our p95 reference)
    filter(!is.na(ems_system_response_time_min)) %>%
    left_join(geo, by = "pcr_key") %>%
    # drop known operational noise + NA jurisdictions
    filter(is_operational_noise == FALSE) %>%
    filter(!is.na(jurisdiction_name))

msg("Computing SNHD p95 tail component breakdown (this may take a few minutes)...")

# -----------------------------
# Arrow-safe summarise
# (only compute raw quantiles here; no “derived from derived” math)
# -----------------------------
summarise_tail_components <- function(df) {
    df %>%
        summarise(
            n = n(),

            # reference tail metric
            p95_sys_min = quantile(ems_system_response_time_min, 0.95),

            # component tails
            p95_dispatch_center_sec = quantile(ems_dispatch_center_time_sec, 0.95),
            p95_chute_min = quantile(ems_chute_time_min, 0.95),
            p95_scene_response_min = quantile(ems_scene_response_time_min, 0.95)
        )
}

# -----------------------------
# Post-collect math (regular dplyr)
# -----------------------------
add_shares <- function(df) {
    df %>%
        mutate(
            p95_dispatch_center_min = p95_dispatch_center_sec / 60,
            share_dispatch_center = if_else(p95_sys_min > 0, p95_dispatch_center_min / p95_sys_min, NA_real_),
            share_chute = if_else(p95_sys_min > 0, p95_chute_min / p95_sys_min, NA_real_),
            share_scene_response = if_else(p95_sys_min > 0, p95_scene_response_min / p95_sys_min, NA_real_)
        )
}

# -----------------------------
# Overall (all non-noise)
# -----------------------------
overall_all <- base %>%
    summarise_tail_components() %>%
    collect() %>%
    add_shares() %>%
    mutate(
        group_level = "overall_all",
        jurisdiction_name = NA_character_
    )

# -----------------------------
# By jurisdiction (all non-noise)
# -----------------------------
by_juris_all <- base %>%
    group_by(jurisdiction_name) %>%
    summarise_tail_components() %>%
    collect() %>%
    add_shares() %>%
    mutate(group_level = "by_jurisdiction_all")

# -----------------------------
# Urban/Suburban focus subset
# -----------------------------
base_focus <- base %>%
    filter(urb_sub_focus == TRUE)

overall_focus <- base_focus %>%
    summarise_tail_components() %>%
    collect() %>%
    add_shares() %>%
    mutate(
        group_level = "overall_urb_sub_focus",
        jurisdiction_name = NA_character_
    )

by_juris_focus <- base_focus %>%
    group_by(jurisdiction_name) %>%
    summarise_tail_components() %>%
    collect() %>%
    add_shares() %>%
    mutate(group_level = "by_jurisdiction_urb_sub_focus")

# -----------------------------
# Combine + write
# -----------------------------
out <- bind_rows(
    overall_all,
    by_juris_all,
    overall_focus,
    by_juris_focus
) %>%
    relocate(group_level, jurisdiction_name) %>%
    arrange(group_level, jurisdiction_name)

write_csv(out, "local_snhd_p95_tail_component_breakdown.csv")
