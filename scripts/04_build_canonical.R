## - 04 Build Canonical (Schema-on-write)
## Inputs: data/clean/03b_typed_tables/<table>/*.parquet
## Outputs: data/clean/04_canonical/<canonical_name>/ (parquet dataset)

suppressPackageStartupMessages({
    library(dplyr)
    library(arrow)
    library(stringr)
    library(readr)
})

stopifnot(requireNamespace("arrow", quietly = TRUE))

# Match inventory stability defaults (Windows-safe)
Sys.setenv(ARROW_NUM_THREADS = "1")

IN_BASE <- "data/clean/03b_typed_tables"
OUT_BASE <- "data/clean/04_canonical"

dir.create(OUT_BASE, recursive = TRUE, showWarnings = FALSE)

# helpers ---------------------------------------------------------------

ds_path <- function(name) file.path(IN_BASE, name)

ensure_exists <- function(path, label) {
    if (!dir.exists(path)) stop(label, " not found: ", path)
}



write_ds <- function(df, out_path, partitioning = NULL) {
    dir.create(out_path, recursive = TRUE, showWarnings = FALSE)
    arrow::write_dataset(
        df,
        path = out_path,
        format = "parquet",
        partitioning = partitioning,
        existing_data_behavior = "overwrite"
    )
}

has_tbl <- function(name) dir.exists(ds_path(name))

# schema-safe column discovery (mirrors “inventory mindset”)
safe_schema_names <- function(ds, tbl_dir, name) {
    # Try direct schema first
    nm <- tryCatch(
        {
            arrow::schema(ds)$names
        },
        error = function(e) NULL
    )

    if (!is.null(nm) && length(nm) > 0) {
        return(nm)
    }

    # Fallback: read schema from a single parquet part (cheap, deterministic)
    parts <- list.files(tbl_dir, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
    if (length(parts) == 0) stop("No parquet parts found for ", name, " at: ", tbl_dir)

    s <- arrow::read_schema(parts[[1]])
    s$names
}

# ------------ open typed datasets -------------------------------------

ensure_exists(ds_path("ComputedElements"), "ComputedElements typed table")
computed <- arrow::open_dataset(ds_path("ComputedElements"), format = "parquet")

fact_time <- if (has_tbl("FACTPCRTIME")) arrow::open_dataset(ds_path("FACTPCRTIME"), format = "parquet") else NULL
dispatch_dl <- if (has_tbl("FACTPCRDISPATCHDELAY")) arrow::open_dataset(ds_path("FACTPCRDISPATCHDELAY"), format = "parquet") else NULL
response_dl <- if (has_tbl("FACTPCRRESPONSEDELAY")) arrow::open_dataset(ds_path("FACTPCRRESPONSEDELAY"), format = "parquet") else NULL
pcr_events <- if (has_tbl("Pub_PCRevents")) arrow::open_dataset(ds_path("Pub_PCRevents"), format = "parquet") else NULL

# canonical: GEO --------------------------------------------------------

canonical_geo <- computed %>%
    transmute(
        pcr_key = pcr_key,
        us_census_region = us_census_region,
        us_census_division = us_census_division,
        nasemso_region = nasemso_region,
        urbancity = urbanicity
    )


write_ds(
    canonical_geo,
    file.path(OUT_BASE, "nemsis_geo")
)

# canonical: TIMES ------------------------------------------------------

canonical_times <- computed %>%
    transmute(
        pcr_key = pcr_key,
        ems_dispatch_center_time_sec = ems_dispatch_center_time_sec,
        ems_chute_time_min = ems_chute_time_min,
        ems_system_response_time_min = ems_system_response_time_min, # fixed name
        ems_scene_response_time_min = ems_scene_response_time_min,
        ems_scene_time_min = ems_scene_time_min,
        ems_scene_to_patient_time_min = ems_scene_to_patient_time_min,
        ems_transport_time_min = ems_transport_time_min,
        ems_total_call_time_min = ems_total_call_time_min
    )

write_ds(
    canonical_times,
    file.path(OUT_BASE, "nemsis_times")
)

# canonical: DELAYS -----------------------------------------------------

canon_delay_from <- function(ds, tbl_name) {
    if (is.null(ds)) {
        return(NULL)
    }

    tbl_dir <- ds_path(tbl_name)
    cols <- safe_schema_names(ds, tbl_dir, tbl_name)

    if (!("pcr_key" %in% cols)) stop("Missing pcr_key in ", tbl_name)

    delay_cols <- setdiff(cols, "pcr_key")
    ds %>% select(pcr_key, all_of(delay_cols))
}

dispatch_delays <- canon_delay_from(dispatch_dl, "FACTPCRDISPATCHDELAY")
response_delays <- canon_delay_from(response_dl, "FACTPCRRESPONSEDELAY")

if (!is.null(dispatch_delays)) {
    write_ds(dispatch_delays, file.path(OUT_BASE, "nemsis_dispatch_delays"))
}

if (!is.null(response_delays)) {
    write_ds(response_delays, file.path(OUT_BASE, "nemsis_response_delays"))
}

# canonical: EVENTS -----------------------------------------------------

if (!is.null(pcr_events)) {
    cols <- safe_schema_names(pcr_events, ds_path("Pub_PCRevents"), "Pub_PCRevents")
    if (!("pcr_key" %in% cols)) {
        message("NOTE: Pub_PCRevents has no pcr_key (unexpected) - skipping nemsis_events.")
    } else {
        write_ds(pcr_events, file.path(OUT_BASE, "nemsis_events"))
    }
}

message("\n04_build_canonical complete -> ", OUT_BASE)
message("Wrote: nemsis_geo, nemsis_times, nemsis_*_delays (and optional nemsis_events)")
