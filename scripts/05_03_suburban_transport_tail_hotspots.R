## 05_03_suburban_transport_tail_hotspots.R
## Identify suburban NO-DELAY transport-time tail hotspots
## Output: output/tables/analysis/suburban_no_delay_transport_tail_hotspots.csv

source("scripts/05_00_analysis_setup.R")

ce <- open_dataset("data/clean/03b_typed_tables/ComputedElements")

delay_flags <- open_dataset(
    file.path("data", "processed", "feature_tables", "delay_flags")
)

# -----------------------------
# Build base (Arrow lazy)
# -----------------------------
base <- ce %>%
    select(
        pcr_key,
        us_census_region,
        us_census_division,
        nasemso_region,
        urbancity = urbanicity,
        ems_transport_time_min
    ) %>%
    left_join(
        delay_flags %>% select(pcr_key, dispatch_delay_flag, response_delay_flag),
        by = "pcr_key"
    ) %>%
    mutate(
        dispatch_delay_flag = if_else(is.na(dispatch_delay_flag), 0L, dispatch_delay_flag),
        response_delay_flag = if_else(is.na(response_delay_flag), 0L, response_delay_flag)
    ) %>%
    filter(
        urbancity == "Suburban",
        dispatch_delay_flag == 0L,
        response_delay_flag == 0L,
        !is.na(ems_transport_time_min)
    )

msg("Computing suburban NO-DELAY transport-time p95 by region...")

# -----------------------------
# Tail summary
# -----------------------------
out <- base %>%
    group_by(
        us_census_region,
        us_census_division,
        nasemso_region
    ) %>%
    summarise(
        n = n(),
        p95_transport_time = quantile(ems_transport_time_min, 0.95)
    ) %>%
    collect() %>%
    arrange(desc(p95_transport_time))

write_csv(out, "suburban_no_delay_transport_tail_hotspots.csv")
