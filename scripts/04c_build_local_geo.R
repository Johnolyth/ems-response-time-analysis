## 04c_build_local_geo.R
## Build local SNHD geo surrogate using a jurisdiction lookup (Arrow-safe)

suppressPackageStartupMessages({
    library(dplyr)
    library(arrow)
    library(stringr)
})

LOCAL_SOURCE <- "snhd"

IN_DIR <- file.path("data", "clean", "03_cleaned_tables", paste0("local_", LOCAL_SOURCE, "_calls"))
OUT_DIR <- file.path("data", "clean", "04_canonical", paste0("local_", LOCAL_SOURCE, "_geo"))

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

ds <- open_dataset(IN_DIR)

req <- c("pcr_key", "jurisdiction")
missing <- setdiff(req, ds$schema$names)
if (length(missing) > 0) stop("Missing required columns: ", paste(missing, collapse = ", "))

# -----------------------------
# 1) Build tiny jurisdiction lookup in R
# -----------------------------
jur_lut <- ds %>%
    select(jurisdiction) %>%
    distinct() %>%
    collect() %>%
    mutate(
        jurisdiction_raw = jurisdiction,
        jurisdiction_name = str_squish(str_remove(jurisdiction_raw, "\\s*\\(\\d+\\)\\s*$")),
        jurisdiction_code = str_extract(jurisdiction_raw, "(?<=\\()\\d+(?=\\))"),
        jurisdiction_type = case_when(
            str_detect(str_to_lower(jurisdiction_name), "^city of") ~ "city",
            str_detect(str_to_lower(jurisdiction_name), "county") ~ "county",
            TRUE ~ "other"
        ),
        is_operational_noise = case_when(
            is.na(jurisdiction_raw) ~ TRUE,
            jurisdiction_raw %in% c("Alarm Test", "TOA") ~ TRUE,
            TRUE ~ FALSE
        ),
        urb_sub_focus = case_when(
            jurisdiction_raw %in% c("Clark County (05031)", "City of Las Vegas (05071)", "North Las Vegas (05131)") ~ TRUE,
            jurisdiction_name == "Henderson" ~ TRUE,
            TRUE ~ FALSE
        )
    ) %>%
    select(
        jurisdiction_raw, jurisdiction_name, jurisdiction_code, jurisdiction_type,
        is_operational_noise, urb_sub_focus
    )

# Write lookup as a small parquet so Arrow can join it lazily
lut_dir <- file.path("data", "processed", "feature_tables", paste0("jurisdiction_lut_", LOCAL_SOURCE))
dir.create(lut_dir, recursive = TRUE, showWarnings = FALSE)
write_dataset(arrow_table(jur_lut), lut_dir, format = "parquet")

# -----------------------------
# 2) Join lookup back to calls (Arrow lazy) and write geo dataset
# -----------------------------
lut_ds <- open_dataset(lut_dir)

geo <- ds %>%
    transmute(
        pcr_key,
        jurisdiction_raw = jurisdiction
    ) %>%
    left_join(lut_ds, by = "jurisdiction_raw") %>%
    distinct()

write_dataset(geo, OUT_DIR, format = "parquet")
message("Wrote local geo dataset: ", OUT_DIR)
message("Wrote lookup: ", lut_dir)
