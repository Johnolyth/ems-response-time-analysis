## 05_00_analysis_setup.R
## Purpose: Shared analysis setup for EMS Case Study (05.x scripts)
## Contract:
##   - READ ONLY from data/clean/04_canonical/
##   - Arrow-first (lazy dplyr queries), collect() only on aggregates / small samples
##   - Windows stability and memory safety

suppressPackageStartupMessages({
    library(arrow)
    library(dplyr)
    library(stringr)
    library(readr)
    library(glue)
})

# -----------------------------
# Global options (stability)
# -----------------------------
# Single-thread Arrow improves stability and reproducibility on Windows
Sys.setenv(ARROW_NUM_THREADS = "1")

# dplyr printing sanity
options(dplyr.summarise.inform = FALSE)
options(width = 120)

# -----------------------------
# Paths
# -----------------------------
CANON_BASE <- file.path("data", "clean", "04_canonical")
OUT_BASE <- file.path("output")
OUT_TABLES <- file.path(OUT_BASE, "tables", "analysis")
OUT_FIGS <- file.path(OUT_BASE, "figures", "analysis")

dir.create(OUT_TABLES, recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_FIGS, recursive = TRUE, showWarnings = FALSE)

stopifnot(dir.exists(CANON_BASE))

# -----------------------------
# Small utilities
# -----------------------------
msg <- function(...) {
    message(glue::glue(..., .envir = parent.frame()))
}

fail <- function(...) {
    stop(glue::glue(..., .envir = parent.frame()), call. = FALSE)
}

`%||%` <- function(x, y) if (is.null(x)) y else x

# -----------------------------
# Canonical dataset registry
# -----------------------------
# We keep this small and explicit. Add new canonical datasets here as they appear.
CANONICAL <- list(
    nemsis_geo             = file.path(CANON_BASE, "nemsis_geo"),
    nemsis_times           = file.path(CANON_BASE, "nemsis_times"),
    nemsis_dispatch_delays = file.path(CANON_BASE, "nemsis_dispatch_delays"),
    nemsis_response_delays = file.path(CANON_BASE, "nemsis_response_delays"),
    nemsis_events          = file.path(CANON_BASE, "nemsis_events") # optional
)

canon_exists <- function(name) {
    # 1) explicit registry
    path <- CANONICAL[[name]]
    if (!is.null(path)) {
        return(dir.exists(path))
    }

    # 2) fallback: folder exists under CANON_BASE with same name
    dir.exists(file.path(CANON_BASE, name))
}

canon_path <- function(name) {
    # 1) explicit registry
    path <- CANONICAL[[name]]
    if (!is.null(path)) {
        return(path)
    }

    # 2) fallback: folder exists under CANON_BASE with same name
    candidate <- file.path(CANON_BASE, name)
    if (dir.exists(candidate)) {
        return(candidate)
    }

    fail("Unknown canonical dataset: '{name}'")
}

canon_ds <- function(name) {
    path <- canon_path(name)
    if (!dir.exists(path)) fail("Canonical dataset not found on disk: '{name}' at {path}")
    open_dataset(path)
}

list_canonical_on_disk <- function() {
    dirs <- list.dirs(CANON_BASE, full.names = FALSE, recursive = FALSE)
    sort(dirs)
}

# -----------------------------
# Schema helpers
# -----------------------------
canon_cols <- function(name) {
    ds <- canon_ds(name)
    ds$schema$names
}

print_schema <- function(name) {
    msg("Schema: {name}")
    cols <- canon_cols(name)
    for (nm in cols) message("  - ", nm)
    invisible(cols)
}


# ---------------------------------------
# Analysis focus controls
# ---------------------------------------
URB_FOCUS <- c("Urban", "Suburban")

filter_urb_sub <- function(df) {
    df %>% dplyr::filter(.data$urbancity %in% URB_FOCUS)
}


# -----------------------------
# Safe sampling helpers (Arrow-first)
# -----------------------------
# NOTE: Arrow datasets do not support random sampling consistently across engines.
# We sample deterministically by taking the first N rows after projection.
sample_rows <- function(ds, n = 1000L, cols = NULL) {
    if (!is.null(cols)) ds <- ds %>% select(all_of(cols))
    ds %>%
        head(n) %>%
        collect()
}

# Unique pcr_key sample (safe)
sample_keys <- function(ds, n = 5000L, key = "pcr_key") {
    ds %>%
        select(all_of(key)) %>%
        distinct() %>%
        head(n) %>%
        collect()
}

# -----------------------------
# Join smoke tests (low memory)
# -----------------------------
# Goal: prove the join keys exist and joins do not explode unexpectedly.
join_smoketest <- function(keys_df,
                           ds_left,
                           ds_right,
                           key = "pcr_key",
                           left_name = "left",
                           right_name = "right",
                           n = 1000L) {
    if (!(key %in% names(keys_df))) fail("keys_df must contain '{key}'")

    # Convert keys to Arrow Table for semi-join behavior
    keys_tbl <- arrow_table(keys_df)

    left_small <- ds_left %>%
        inner_join(keys_tbl, by = key) %>%
        head(n) %>%
        collect()

    right_small <- ds_right %>%
        inner_join(keys_tbl, by = key) %>%
        head(n) %>%
        collect()

    msg("Join smoketest '{left_name}' vs '{right_name}':")
    msg("  {left_name} sample rows:  {nrow(left_small)}")
    msg("  {right_name} sample rows: {nrow(right_small)}")

    invisible(list(left = left_small, right = right_small))
}

# -----------------------------
# Output helpers
# -----------------------------
write_csv <- function(df, filename) {
    path <- file.path(OUT_TABLES, filename)
    readr::write_csv(df, path)
    msg("Wrote: {path}")
    invisible(path)
}

# -----------------------------
# Startup report (quick, informative)
# -----------------------------
analysis_startup_report <- function() {
    msg("Analysis setup loaded.")
    msg("Canonical base: {normalizePath(CANON_BASE, winslash = '/', mustWork = FALSE)}")

    on_disk <- list_canonical_on_disk()
    msg("Canonical datasets on disk ({length(on_disk)}): {paste(on_disk, collapse = ', ')}")

    # Confirm required datasets exist
    required <- c("nemsis_geo", "nemsis_times")
    missing_required <- required[!vapply(required, canon_exists, logical(1))]
    if (length(missing_required) > 0) {
        fail("Missing required canonical datasets: {paste(missing_required, collapse = ', ')}")
    }

    # Optional presence
    if (!canon_exists("nemsis_events")) {
        msg("Note: optional dataset 'nemsis_events' not found (this is OK).")
    }

    invisible(on_disk)
}

# -----------------------------
# Optional: quick integrity checks
# -----------------------------
quick_key_check <- function(name, key = "pcr_key", n = 50000L) {
    ds <- canon_ds(name)

    cols <- ds$schema$names
    if (!(key %in% cols)) fail("'{name}' does not contain key column '{key}'")

    # head(n) to avoid full scan; check missingness on sample
    df <- ds %>%
        select(all_of(key)) %>%
        head(n) %>%
        collect()

    miss <- sum(is.na(df[[key]]) | df[[key]] == "")
    msg("Key check '{name}' (first {nrow(df)} rows): missing/blank {key} = {miss}")

    invisible(df)
}

# -----------------------------
# Run on load (safe, no heavy scans)
# -----------------------------
analysis_startup_report()
