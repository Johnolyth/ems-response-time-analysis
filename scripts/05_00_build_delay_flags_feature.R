## 05_00_build_delay_flags_feature.R
## Build processed feature table: delay flags (dispatch/response)
## Output: data/processed/feature_tables/delay_flags/ (partitioned parquet)

source("scripts/05_00_analysis_setup.R")

OUT_DIR <- file.path("data", "processed", "feature_tables", "delay_flags")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

dd <- canon_ds("nemsis_dispatch_delays") %>% collect()
rd <- canon_ds("nemsis_response_delays") %>% collect()

# Normalize to character for reliable comparisons
dd <- dd %>% mutate(e_response_08 = as.character(e_response_08))
rd <- rd %>% mutate(e_response_09 = as.character(e_response_09))

# Define "no delay" / "unknown" code set (we can expand this once verified)
# Currently observed in your events sample: "7701003" and "Not Recorded"
NO_DELAY_OR_UNKNOWN <- c("7701003", "Not Recorded", NA_character_)

dd_flags <- dd %>%
    transmute(
        pcr_key,
        dispatch_delay_flag_row = ifelse(e_response_08 %in% NO_DELAY_OR_UNKNOWN, 0L, 1L)
    ) %>%
    group_by(pcr_key) %>%
    summarise(
        dispatch_delay_flag = as.integer(max(dispatch_delay_flag_row, na.rm = TRUE)),
        .groups = "drop"
    )

rd_flags <- rd %>%
    transmute(
        pcr_key,
        response_delay_flag_row = ifelse(e_response_09 %in% NO_DELAY_OR_UNKNOWN, 0L, 1L)
    ) %>%
    group_by(pcr_key) %>%
    summarise(
        response_delay_flag = as.integer(max(response_delay_flag_row, na.rm = TRUE)),
        .groups = "drop"
    )


flags <- dd_flags %>%
    full_join(rd_flags, by = "pcr_key") %>%
    mutate(
        dispatch_delay_flag = ifelse(is.na(dispatch_delay_flag), 0L, dispatch_delay_flag),
        response_delay_flag = ifelse(is.na(response_delay_flag), 0L, response_delay_flag)
    )

msg("Feature rows: {nrow(flags)}")

# Write as Arrow dataset (parquet)
arrow::write_dataset(flags, OUT_DIR, format = "parquet", existing_data_behavior = "overwrite")
msg("Wrote feature table: {OUT_DIR}")
