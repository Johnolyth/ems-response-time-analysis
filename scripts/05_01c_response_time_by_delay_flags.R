## 05_01c_response_time_by_delay_flags.R
## Response-time quantiles stratified by delay flags
## Urban / Suburban focus
## Output: output/tables/analysis/response_time_quantiles_urban_suburban_by_delay_flag.csv

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
  filter_urb_sub() %>% # 🔒 Urban/Suburban filter
  left_join(
    delay_flags %>%
      select(pcr_key, dispatch_delay_flag, response_delay_flag),
    by = "pcr_key"
  ) %>%
  mutate(
    dispatch_delay_flag = case_when(
      is.na(dispatch_delay_flag) ~ 0L,
      TRUE ~ dispatch_delay_flag
    ),
    response_delay_flag = case_when(
      is.na(response_delay_flag) ~ 0L,
      TRUE ~ response_delay_flag
    )
  )

msg("Computing response-time quantiles by delay flags (Urban/Suburban only)...")

summarise_quantiles <- function(df) {
  df %>%
    summarise(
      n = n(),
      median_sys = quantile(ems_system_response_time_min, 0.50),
      p90_sys = quantile(ems_system_response_time_min, 0.90),
      p95_sys = quantile(ems_system_response_time_min, 0.95)
    )
}

out <- base %>%
  group_by(
    dispatch_delay_flag,
    response_delay_flag,
    us_census_region,
    us_census_division,
    nasemso_region,
    urbancity
  ) %>%
  summarise_quantiles() %>%
  collect() %>%
  arrange(
    dispatch_delay_flag,
    response_delay_flag,
    us_census_region,
    urbancity
  )

write_csv(out, "response_time_quantiles_urban_suburban_by_delay_flag.csv")


msg("Wrote: response_time_quantiles_urban_suburban_by_delay_flag.csv")
