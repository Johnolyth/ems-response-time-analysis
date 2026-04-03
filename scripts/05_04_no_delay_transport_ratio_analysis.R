## 05_04_no_delay_transport_ratio_analysis.R
## Transport ratio analysis (NO delay calls)
## Output: output/tables/analysis/no_delay_transport_ratio_by_geo.csv

source("scripts/05_00_analysis_setup.R")

library(dplyr)
library(arrow)
library(readr)

# -----------------------------
# Load datasets
# -----------------------------

times <- canon_ds("nemsis_times")
geo   <- canon_ds("nemsis_geo")

delay_flags <- open_dataset(
  file.path("data", "processed", "feature_tables", "delay_flags")
)

# -----------------------------
# Build base (Arrow lazy)
# -----------------------------

base <- times %>%
  select(
    pcr_key,
    ems_transport_time_min,
    ems_total_call_time_min
  ) %>%
  filter(
    !is.na(ems_transport_time_min),
    !is.na(ems_total_call_time_min),
    ems_total_call_time_min > 0
  ) %>%
  left_join(geo, by = "pcr_key") %>%
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
  ) %>%
  filter(
    dispatch_delay_flag == 0L,
    response_delay_flag == 0L
  ) %>%
  mutate(
    transport_ratio = ems_transport_time_min / ems_total_call_time_min
  )

msg("Computing transport ratio quantiles for NO-delay calls...")

# -----------------------------
# Quantile summary
# -----------------------------

summarise_ratio <- function(df) {
  df %>%
    summarise(
      n = n(),
      median_ratio = quantile(transport_ratio, 0.50),
      p90_ratio    = quantile(transport_ratio, 0.90),
      p95_ratio    = quantile(transport_ratio, 0.95)
    )
}

out <- base %>%
  group_by(
    urbancity,
    us_census_region,
    us_census_division
  ) %>%
  summarise_ratio() %>%
  collect()

# -----------------------------
# Write output
# -----------------------------

write_csv(out, "no_delay_transport_ratio_by_geo.csv")

msg("Wrote: output/tables/analysis/no_delay_transport_ratio_by_geo.csv")
write_csv(out, "no_delay_transport_ratio_by_geo.csv")

msg("Wrote: output/tables/analysis/no_delay_transport_ratio_by_geo.csv")
