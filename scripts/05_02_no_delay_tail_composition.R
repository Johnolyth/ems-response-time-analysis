## 05_02_no_delay_tail_composition.R
## Tail (p95) composition for NO-DELAY calls
## Output: output/tables/analysis/no_delay_tail_composition_by_urbanicity.csv

source("scripts/05_00_analysis_setup.R")

# Canonical / typed datasets
ce <- open_dataset("data/clean/03b_typed_tables/ComputedElements")
geo <- canon_ds("nemsis_geo")

delay_flags <- open_dataset(
    file.path("data", "processed", "feature_tables", "delay_flags")
)

# -----------------------------
# Build base (Arrow lazy)
# -----------------------------
base <- ce %>%
    select(
        pcr_key,
        urbancity = urbanicity,
        ems_dispatch_center_time_sec,
        ems_chute_time_min,
        ems_scene_response_time_min,
        ems_transport_time_min,
        ems_total_call_time_min
    ) %>%
    left_join(
        delay_flags %>% select(pcr_key, dispatch_delay_flag, response_delay_flag),
        by = "pcr_key"
    ) %>%
    mutate(
        dispatch_delay_flag = if_else(is.na(dispatch_delay_flag), 0L, dispatch_delay_flag),
        response_delay_flag = if_else(is.na(response_delay_flag), 0L, response_delay_flag),
        # convert dispatch center time to minutes for comparability
        dispatch_center_time_min = ems_dispatch_center_time_sec / 60
    ) %>%
    filter(
        dispatch_delay_flag == 0L,
        response_delay_flag == 0L,
        !is.na(ems_total_call_time_min)
    )

msg("Computing p95 tail composition for NO-DELAY calls by urbanicity...")

# -----------------------------
# Tail summary
# -----------------------------
out <- base %>%
    group_by(urbancity) %>%
    summarise(
        n = n(),
        p95_dispatch_center = quantile(dispatch_center_time_min, 0.95, na.rm = TRUE),
        p95_chute_time = quantile(ems_chute_time_min, 0.95, na.rm = TRUE),
        p95_scene_response = quantile(ems_scene_response_time_min, 0.95, na.rm = TRUE),
        p95_transport_time = quantile(ems_transport_time_min, 0.95, na.rm = TRUE),
        p95_total_call = quantile(ems_total_call_time_min, 0.95, na.rm = TRUE)
    ) %>%
    collect() %>%
    arrange(desc(p95_total_call))

write_csv(out, "no_delay_tail_composition_by_urbanicity.csv")
