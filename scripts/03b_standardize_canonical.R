## 03b_standardize_canonical.R

suppressPackageStartupMessages({
    library(dplyr)
    library(arrow)
    library(readr)
})

IN_BASE <- "data/clean/03_cleaned_tables"
OUT_BASE <- "data/clean/03b_typed_tables"
dir.create(OUT_BASE, showWarnings = FALSE, recursive = TRUE)

na_sentinels <- c(".", "", "NA", "N/A", "NULL")

norm_missing <- function(df) {
    df %>%
        mutate(across(where(is.character), ~ ifelse(.x %in% na_sentinels, NA_character_, .x)))
}

to_double <- function(x) suppressWarnings(readr::parse_double(x))

type_one_part <- function(tbl, df) {
    df <- norm_missing(df)

    if (tbl == "ComputedElements") {
        df <- df %>%
            mutate(
                ageinyear = to_double(ageinyear),
                ems_dispatch_center_time_sec = to_double(ems_dispatch_center_time_sec),
                ems_chute_time_min = to_double(ems_chute_time_min),
                ems_system_response_time_min = to_double(ems_system_response_time_min),
                ems_scene_response_time_min = to_double(ems_scene_response_time_min),
                ems_scene_time_min = to_double(ems_scene_time_min),
                ems_scene_to_patient_time_min = to_double(ems_scene_to_patient_time_min),
                ems_transport_time_min = to_double(ems_transport_time_min),
                ems_total_call_time_min = to_double(ems_total_call_time_min)
            )
    }

    if (tbl %in% c("FACTPCRDISPATCHDELAY", "FACTPCRRESPONSEDELAY", "FACTPCRTIME")) {
        num_cols <- setdiff(names(df), "pcr_key")
        df <- df %>% mutate(across(all_of(num_cols), to_double))
    }

    df
}

tables <- list.dirs(IN_BASE, recursive = FALSE, full.names = FALSE)

for (tbl in tables) {
    in_dir <- file.path(IN_BASE, tbl)
    out_dir <- file.path(OUT_BASE, tbl)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

    parts <- sort(list.files(in_dir, pattern = "\\.parquet$", full.names = TRUE))
    message("\n03b typing: ", tbl, " | parts: ", length(parts))
    if (length(parts) == 0) next

    for (p in parts) {
        out_path <- file.path(out_dir, basename(p))
        if (file.exists(out_path)) next

        df <- arrow::read_parquet(p)
        df2 <- type_one_part(tbl, df)

        dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

        ok <- tryCatch(
            {
                arrow::write_parquet(df2, out_path)
                TRUE
            },
            error = function(e) {
                message("WRITE ERROR -> ", out_path)
                message("  ", e$message)
                FALSE
            }
        )

        if (!ok) {
            tmp <- tempfile(fileext = ".parquet")
            arrow::write_parquet(df2, tmp)
            file.copy(tmp, out_path, overwrite = TRUE)
            unlink(tmp)
        }
    }
}

message("\n03b complete -> ", OUT_BASE)
