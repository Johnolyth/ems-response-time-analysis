## ------------------04 Inventory / QA (Arrow-first)---------------------

suppressPackageStartupMessages({
    library(dplyr)
    library(arrow)
    library(readr)
    library(purrr)
    library(tibble)
})

Sys.setenv(ARROW_NUM_THREADS = "1")

stopifnot(requireNamespace("arrow", quietly = TRUE))

# config

in_base <- "data/clean/03b_typed_tables"
out_dir <- "output/tables/intermediate/04_inventory"
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

tables <- list.dirs(in_base, recursive = FALSE, full.names = FALSE)
if (length(tables) == 0) stop("No typed tables found under: ", in_base)


# write paths

overview_path <- file.path(out_dir, "dataset_overview.csv")
cols_path <- file.path(out_dir, "column_inventory.csv")
keys_path <- file.path(out_dir, "key_integrity.csv")


# remove old outputs

suppressWarnings(
    file.remove(overview_path, cols_path, keys_path)
)



# initialize output files


append_csv <- function(df, path) {
    if (!file.exists(path)) {
        readr::write_csv(df, path)
    } else {
        readr::write_csv(df, path, append = TRUE)
    }
}

# read types

sample_types_from_one_file <- function(part_files) {
    if (length(part_files) == 0 || is.na(part_files[1])) {
        return(NULL)
    }

    df <- arrow::read_parquet(part_files[1])
    tibble(
        column = names(df),
        type   = vapply(df, function(x) class(x)[1], character(1))
    )
}

# ---------- main loop ------------------------------------------------------

for (tbl in tables) {
    message("QA: ", tbl)

    # 1- table directory -----------------------------

    tbl_dir <- file.path(in_base, tbl)

    # 2- file discovery ------------------------------

    part_files <- list.files(
        tbl_dir,
        pattern = "\\.parquet$",
        full.names = TRUE
    )

    # 3- sample schema --------------------------------

    col_sample <- sample_types_from_one_file(part_files)

    # 4- col inv output ------------------------------

    if (!is.null(col_sample)) {
        col_df <- col_sample %>% mutate(table = tbl, .before = 1)
        append_csv(col_df, cols_path)
    } else {
        message("  WARN: no parquete parts found for ", tbl)
    }

    # 5- dataset overview ----------------------------

    overview <- tibble(
        table = tbl,
        parts = length(part_files),
        cols  = if (!is.null(col_sample)) nrow(col_sample) else NA_integer_
    )
    append_csv(overview, overview_path)

    # 6- key integrity --------------------------------

    has_key <- !is.null(col_sample) && ("pcr_key" %in% col_sample$column)

    distinct_val <- NA_real_

    if (has_key && length(part_files) > 0) {
        samp <- arrow::read_parquet(part_files[1]) %>% dplyr::select(pcr_key)
        distinct_val <- dplyr::n_distinct(samp$pcr_key)
        rm(samp)
    }


    key_df <- tibble(
        table = tbl,
        has_pcr_key = has_key,
        distinct_pcr_key_sample = distinct_val
    )

    append_csv(key_df, keys_path)

    rm(col_sample, part_files)
}


message("04 inventory complete -> ", out_dir)
