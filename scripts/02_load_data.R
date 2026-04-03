## ------------------02 Load Data (NEMSIS-ready)---------------------
# Purpose: load CSV / TSV / TXT / Excel (multi-sheet) and lightly clean.
# - Excel sheets -> data_clean/<file>_sheets/clean_<sheet>.csv
# - CSV/TSV/TXT -> data_clean/<file>_sheets/clean_<file>.csv
# - NEMSIS-detected CSVs -> data_clean/<file>_nemsis/ (Parquet if arrow available)

library(tidyverse)
library(readxl)
library(janitor)
library(lubridate)

has_arrow <- requireNamespace("arrow", quietly = TRUE)
has_dt <- requireNamespace("data.table", quietly = TRUE)

message("Starting: Loading raw data...")

raw_dir <- "data/raw/"
clean_dir <- "data/clean/"

if (!dir.exists(clean_dir)) dir.create(clean_dir, recursive = TRUE)

# ---------------- helper: detect NEMSIS -----------------------------------
is_nemsis_file <- function(path) {
    # quick filename hint
    fname <- tolower(basename(path))
    if (grepl("nemsis", fname)) {
        return(TRUE)
    }

    # if header available quickly, check for common NEMSIS fields
    safe_try <- try(
        {
            hdr <- names(readr::read_csv(path, n_max = 0, show_col_types = FALSE))
            hdr_lower <- tolower(hdr)
            nem_keys <- c(
                "pcrkey", "event_id", "emsdispatchcentertimesec", "ems_scene_response_time_min",
                "ems_system_response_time_min", "ems_total_call_time_min", "emstransporttimemin",
                "ambulance", "incident_number"
            )
            any(vapply(nem_keys, function(k) any(grepl(k, hdr_lower, fixed = TRUE)), logical(1)))
        },
        silent = TRUE
    )

    if (inherits(safe_try, "try-error")) {
        return(FALSE)
    } else {
        return(safe_try)
    }
}

# ---------------- helper: fast csv reader (choose best available) ----------
fast_read_csv <- function(path) {
    size_mb <- file.info(path)$size / 1024^2
    # prefer data.table::fread when available for very large files
    if (size_mb > 200 && requireNamespace("data.table", quietly = TRUE)) {
        message(paste0("  (large file: ", round(size_mb, 1), " MB) using data.table::fread"))
        # fread returns data.table; convert to tibble for consistency
        dt <- data.table::fread(path, showProgress = FALSE, sep = ",", data.table = TRUE)
        return(as_tibble(dt))
    }

    # otherwise readr
    message(paste0("  (size: ", round(size_mb, 1), " MB) using readr::read_csv"))
    df <- readr::read_csv(path, show_col_types = FALSE)
    return(df)
}

# ---------------- unified loader (csv, text, tsv, excel) ------------------
load_file <- function(path) {
    ext <- tolower(tools::file_ext(path))
    message(paste("Loading:", basename(path)))

    if (ext == "csv") {
        return(fast_read_csv(path))
    }

    if (ext %in% c("txt", "dat", "asc")) {
        # assume tab-delimited ASCII by default
        return(readr::read_delim(path, delim = "\t", show_col_types = FALSE))
    }

    if (ext == "tsv") {
        return(readr::read_tsv(path, show_col_types = FALSE))
    }

    if (ext %in% c("xls", "xlsx")) {
        sheets <- excel_sheets(path)
        out <- lapply(sheets, function(s) {
            message(paste("  → loading sheet:", s))
            read_excel(path, sheet = s) |> suppressWarnings()
        })
        names(out) <- sheets
        return(out) # list of data.frames
    }

    stop(paste("Unsupported file type:", ext))
}

# ---------------- gather files ------------------------------------------
files <- list.files(raw_dir, full.names = TRUE, recursive = TRUE)
files <- files[file.info(files)$isdir == FALSE]
if (length(files) == 0) stop("No files found in data/raw/. Add files and re-run.")

# containers
data_list <- list() # flattened (filename_sheet or filename)
file_meta <- list() # map base -> type ("sheets" or "single" or "nemsis")

# ---------------- loop through raw files --------------------------------

for (fpath in files) {
    fbase <- tools::file_path_sans_ext(basename(fpath))
    ext <- tolower(tools::file_ext(fpath))

    nemsis_flag <- FALSE

    np <- gsub("\\\\", "/", normalizePath(fpath))

    in_nemsis_folder <- grepl(
        "/data/raw/nemsis/",
        np,
        fixed = TRUE
    )

    # detect NEMSIS (either by filename hint or header)
    if (in_nemsis_folder && ext %in% c("txt", "tsv")) nemsis_flag <- TRUE

    if (nemsis_flag && ext %in% c("csv", "txt", "tsv")) nemsis_flag <- is_nemsis_file(fpath)

    # ---- NEW: Handle NEMSIS via streaming (skip in-memory load) ----
    if (nemsis_flag && ext %in% c("txt", "tsv")) {
        message(paste("Detected NEMSIS TXT/TSV → streaming:", basename(fpath)))

        out_dir <- file.path(clean_dir, paste0(fbase, "_nemsis_stream"))
        if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

        # call your streaming function
        nemsis_stream(fpath, chunk_size = 200000, output_dir = out_dir)

        # register metadata (but don't load chunks into memory)
        file_meta[[fbase]] <- list(type = "nemsis_stream", out_dir = out_dir)
        next
    }

    # ---- ORIGINAL BEHAVIOR FOR ALL OTHER FILES ----

    # load entire file (unless it's an Excel list)
    loaded <- load_file(fpath)

    if (is.list(loaded) && !is.data.frame(loaded)) {
        # Excel multi-sheet flattening
        sheets <- names(loaded)
        for (s in sheets) {
            key <- paste0(fbase, "_", make.names(s))
            data_list[[key]] <- loaded[[s]]
        }
        file_meta[[fbase]] <- list(type = "sheets", sheets = sheets)
    } else {
        # single table: CSV, TSV, TXT, etc
        key <- fbase
        data_list[[key]] <- loaded
        if (nemsis_flag) {
            file_meta[[fbase]] <- list(type = "nemsis")
        } else {
            file_meta[[fbase]] <- list(type = "single")
        }
    }
}
# ---------------- light-clean + save outputs ----------------------------
for (fbase in names(file_meta)) {
    meta <- file_meta[[fbase]]

    if (meta$type == "nemsis_stream") {
        message("Skipping save step (already streamed): ", fbase)
        next
    }

    if (meta$type == "sheets") {
        folder_out <- file.path(clean_dir, paste0(fbase, "_sheets"))
        if (!dir.exists(folder_out)) dir.create(folder_out, recursive = TRUE)

        for (s in meta$sheets) {
            key <- paste0(fbase, "_", make.names(s))
            df <- data_list[[key]]

            clean_df <- df |>
                janitor::clean_names() |>
                remove_empty(c("rows", "cols")) |>
                mutate(across(where(is.character), ~ trimws(.)))

            out_path <- file.path(folder_out, paste0("clean_", make.names(s), ".csv"))
            readr::write_csv(clean_df, out_path)
            message("Saved cleaned sheet: ", out_path)
        }
    } else if (meta$type == "nemsis") {
        # NEMSIS: put into dedicated nemsis folder; prefer Parquet if arrow installed
        folder_out <- file.path(clean_dir, paste0(fbase, "_nemsis"))
        if (!dir.exists(folder_out)) dir.create(folder_out, recursive = TRUE)

        key <- fbase
        df <- data_list[[key]]

        clean_df <- df |>
            janitor::clean_names() |>
            remove_empty(c("rows", "cols")) |>
            mutate(across(where(is.character), ~ trimws(.)))

        # try to save as Parquet if arrow present, otherwise CSV
        if (requireNamespace("arrow", quietly = TRUE)) {
            pq_path <- file.path(folder_out, paste0("clean_", fbase, ".parquet"))
            arrow::write_parquet(clean_df, pq_path)
            message("Saved NEMSIS Parquet: ", pq_path)
        } else {
            out_path <- file.path(folder_out, paste0("clean_", fbase, ".csv"))
            readr::write_csv(clean_df, out_path)
            message("Saved NEMSIS CSV: ", out_path)
            message("Tip: install 'arrow' to store NEMSIS as Parquet for faster downstream queries.")
        }
    } else {
        # generic single file (csv/tsv/txt)
        folder_out <- file.path(clean_dir, paste0(fbase, "_sheets"))
        if (!dir.exists(folder_out)) dir.create(folder_out, recursive = TRUE)

        key <- fbase
        df <- data_list[[key]]

        clean_df <- df |>
            janitor::clean_names() |>
            remove_empty(c("rows", "cols")) |>
            mutate(across(where(is.character), ~ trimws(.)))

        out_path <- file.path(folder_out, paste0("clean_", fbase, ".csv"))
        readr::write_csv(clean_df, out_path)
        message("Saved cleaned file: ", out_path)
    }
}

message("Finished: Data loaded and lightly cleaned.")
