library(tidyverse)
library(janitor)
library(lubridate)

has_arrow <- requireNamespace("arrow", quietly = TRUE)
stopifnot(has_arrow)

clean_dir <- "data/clean/"
processed_dir <- "data/processed"

dir.create(processed_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(file.path(processed_dir, "nemsis"), showWarnings = FALSE)
dir.create(file.path(processed_dir, "generic"), showWarnings = FALSE)

# Input from 02

stream_base <- clean_dir

# Output: cleaned parquet per table

out_base <- file.path(clean_dir, "03_cleaned_tables")
dir.create(out_base, showWarnings = FALSE, recursive = TRUE)

# Find stream directories

nemsis_stream_dirs <- list.dirs(stream_base, recursive = FALSE, full.names = TRUE)
nemsis_stream_dirs <- nemsis_stream_dirs[grepl("_nemsis_stream$", basename(nemsis_stream_dirs), ignore.case = TRUE)]

if (length(nemsis_stream_dirs) == 0) {
    stop("No *_nemsis_stream folders found under: ", stream_base)
}

safe_read_chunk <- function(f) {
    x <- try(
        readr::read_csv(
            f,
            show_col_types = FALSE,
            progress = FALSE,
            col_types = readr::cols(.default = readr::col_character())
        ),
        silent = TRUE
    )
    if (inherits(x, "try-error") || is.null(x) || ncol(x) == 0) {
        return(NULL)
    }
    x
}

clean_chunk <- function(df) {
    df |>
        janitor::clean_names() |>
        janitor::remove_empty(c("rows", "cols"))
}

for (d in nemsis_stream_dirs) {
    table_name <- sub("_nemsis_stream$", "", basename(d), ignore.case = TRUE)
    message("\nProcessing table: ", table_name)

    files <- list.files(d, full.names = TRUE, pattern = "\\.csv$")
    message(" chunks found: ", length(files))
    if (length(files) == 0) next

    out_dir <- file.path(out_base, table_name)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

    written <- 0L

    for (f in files) {
        x <- safe_read_chunk(f)
        if (is.null(x)) next

        x <- clean_chunk(x)
        if (nrow(x) == 0) next

        part_name <- paste0(tools::file_path_sans_ext(basename(f)), ".parquet")
        arrow::write_parquet(x, file.path(out_dir, part_name))

        written <- written + 1L
    }

    message(" wrote parts: ", written, " -> ", out_dir)
}

message("\n03 complete. Cleaned parquet parts written to: ", out_base)
