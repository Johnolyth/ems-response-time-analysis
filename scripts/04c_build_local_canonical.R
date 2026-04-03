## 04c_build_local_canonical.R
## Build canonical "times" dataset for a local EMS source (SNHD now, SB later).

suppressPackageStartupMessages({
    library(dplyr)
    library(arrow)
})

LOCAL_SOURCE <- "snhd"

IN_DIR <- file.path("data", "clean", "03_cleaned_tables", paste0("local_", LOCAL_SOURCE, "_calls"))
OUT_DIR <- file.path("data", "clean", "04_canonical", paste0("local_", LOCAL_SOURCE, "_times"))

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

ds <- open_dataset(IN_DIR)

# Required columns check
req <- c("pcr_key", "sent_to_queue", "x1st_unit_assigned", "x1st_unit_enroute", "x1st_unit_arrived")
missing <- setdiff(req, ds$schema$names)
if (length(missing) > 0) stop("Missing required columns in local dataset: ", paste(missing, collapse = ", "))

times <- ds %>%
    transmute(
        pcr_key,

        # convert to int64 microseconds since epoch (Arrow-friendly)
        queued_us   = cast(sent_to_queue, int64()),
        assigned_us = cast(x1st_unit_assigned, int64()),
        enroute_us  = cast(x1st_unit_enroute, int64()),
        arrived_us  = cast(x1st_unit_arrived, int64())
    ) %>%
    transmute(
        pcr_key,

        # queued -> assigned (seconds)
        ems_dispatch_center_time_sec = (assigned_us - queued_us) / 1e6,

        # assigned -> enroute (minutes)
        ems_chute_time_min = ((enroute_us - assigned_us) / 1e6) / 60,

        # queued -> arrived (minutes)  [proxy "system response"]
        ems_system_response_time_min = ((arrived_us - queued_us) / 1e6) / 60,

        # enroute -> arrived (minutes)
        ems_scene_response_time_min = ((arrived_us - enroute_us) / 1e6) / 60
    ) %>%
    mutate(
        ems_dispatch_center_time_sec = if_else(ems_dispatch_center_time_sec < 0, NA_real_, ems_dispatch_center_time_sec),
        ems_chute_time_min           = if_else(ems_chute_time_min < 0, NA_real_, ems_chute_time_min),
        ems_system_response_time_min = if_else(ems_system_response_time_min < 0, NA_real_, ems_system_response_time_min),
        ems_scene_response_time_min  = if_else(ems_scene_response_time_min < 0, NA_real_, ems_scene_response_time_min)
    )

write_dataset(times, OUT_DIR, format = "parquet")

message("Wrote canonical local times dataset: ", OUT_DIR)
