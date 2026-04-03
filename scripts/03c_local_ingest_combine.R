## 03c_local_ingest_combine.R
## Combine chunked local EMS CSV exports into a single Parquet dataset.
## Designed for SNHD now; plug-and-play for SB later by changing the config block.

suppressPackageStartupMessages({
    library(dplyr)
    library(readr)
    library(arrow)
    library(stringr)
    library(digest)
})

# -----------------------------
# Config (edit per local source)
# -----------------------------
LOCAL_SOURCE <- "snhd"
IN_DIR <- file.path("data", "clean", "SNHD_EMS_CALLS_sheets")
PATTERN <- "^clean_.*\\.csv$"

OUT_DIR <- file.path("data", "clean", "03_cleaned_tables", paste0("local_", LOCAL_SOURCE, "_calls"))
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# -----------------------------
# Helpers
# -----------------------------
make_pcr_key <- function(df) {
    # Deterministic row hash -> stable ID even without a true incident ID.
    # Uses key fields; includes row_number to avoid collisions when rows are identical.
    df %>%
        mutate(
            .row_id = row_number(),
            pcr_key = vapply(
                paste(sent_to_queue, x1st_unit_assigned, x1st_unit_enroute, x1st_unit_arrived, jurisdiction, .row_id, sep = "|"),
                function(x) digest(x, algo = "xxhash64"),
                character(1)
            )
        ) %>%
        select(-.row_id)
}

# -----------------------------
# Read + bind
# -----------------------------
files <- list.files(IN_DIR, pattern = PATTERN, full.names = TRUE)
if (length(files) == 0) stop("No input files found in: ", IN_DIR)

message("Local ingest: ", LOCAL_SOURCE)
message("Found files: ", length(files))

read_one <- function(f) {
    read_csv(f, show_col_types = FALSE) %>%
        mutate(local_source = LOCAL_SOURCE, source_file = basename(f))
}

raw <- bind_rows(lapply(files, read_one))

# Deduplicate exact duplicates (SNHD appears to have some repeated rows)
raw <- raw %>% distinct()

# Add pcr_key
raw <- make_pcr_key(raw)

# Write as Arrow dataset (partitioned lightly for Windows stability)
write_dataset(raw, OUT_DIR, format = "parquet")

message("Wrote local cleaned dataset: ", OUT_DIR)
