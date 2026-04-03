## 05_01d_response_time_no_delay_by_urbanicity.R
## Response-time distributions for NO-DELAY calls, stratified by urbanicity
## Output: output/tables/analysis/response_time_no_delay_by_urbanicity.csv

source("scripts/05_00_analysis_setup.R")

times <- canon_ds("nemsis_times")
geo <- canon_ds("nemsis_geo")

delay_flags <- open_dataset(
    file.path("data", "processed", "feature_tables", "delay_flags")
)

# -----------------------------
# Build base (Arrow lazy)
# -----------------------------
base <- times %>%
    select(pcr_key, ems_system_response_time_min) %>%
    filter(!is.na(ems_system_response_time_min)) %>%
    left_join(geo, by = "pcr_key") %>%
    left_join(
        delay_flags %>% select(pcr_key, dispatch_delay_flag, response_delay_flag),
        by = "pcr_key"
    ) %>%
    mutate(
        dispatch_delay_flag = case_when(is.na(dispatch_delay_flag) ~ 0L, TRUE ~ dispatch_delay_flag),
        response_delay_flag = case_when(is.na(response_delay_flag) ~ 0L, TRUE ~ response_delay_flag)
    ) %>%
    # KEEP ONLY NO-DELAY CASES
    filter(dispatch_delay_flag == 0L, response_delay_flag == 0L)

msg("Computing response-time quantiles for NO-DELAY calls by urbanicity...")

# -----------------------------
# Quantile helper
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

# -----------------------------
# Stratified summary
# -----------------------------
out <- base %>%
    group_by(
        us_census_region,
        us_census_division,
        nasemso_region,
        urbancity
    ) %>%
    summarise_quantiles() %>%
    collect() %>%
    arrange(
        us_census_region,
        urbancity
    )

write_csv(out, "response_time_no_delay_by_urbanicity.csv")
