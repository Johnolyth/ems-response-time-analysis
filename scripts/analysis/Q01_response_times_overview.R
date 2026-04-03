# Question 01: response time overview

library(tidyverse)
library(janitor)
library(lubridate)

has_arrow <- requireNamespace("arrow", quietly = TRUE)

processed_dir <- "data/processed/nemsis"

# reader

read_data <- function(name) {
    parquet_path <- file.path(processed_dir, paste0(name, ".parquet"))
    csv_path <- file.path(processed_dir, paste0(name, ".csv"))

    if (file.exists(parquet_path) && has_arrow) {
        arrow::read_parquet(parquet_path)
    } else if (file.exists(csv_path)) {
        readr::read_csv(csv_path, show_col_types = FALSE)
    } else {
        stop("Missing data file: ", name)
    }
}

# load data

nemsis_times <- read_data("nemsis_times")
nemsis_core <- read_data("nemsis_core")


# structural check

glimpse(nemsis_times)

summary(nemsis_times)

# join data

nemsis_times <- nemsis_times |>
    left_join(
        nemsis_core |> select(pcrkey, event_date),
        by = "pcrkey"
    )

# initial filtering

nemsis_times_clean <- nemsis_times |>
    filter(event_date >= as.Date("2018-01-01")) |>
    mutate(
        hour = hour(event_date),
        weekday = wday(event_date, label = TRUE)
    )

# distribution checks

response_vars <- c(
    "ems_scene_response_time_min",
    "ems_system_response_time_min",
    "ems_total_call_time_min",
    "emstransporttimemin"
)

nemsis_times_clean |>
    select(all_of(response_vars)) |>
    pivot_longer(everything()) |>
    ggplot(aes(value)) +
    geom_histogram(bins = 50) +
    facet_wrap(~name, scales = "free") +
    labs(
        title = "Distribution of EMS Response Time Metrics",
        x = "Minutes",
        y = "Count"
    )

# presence checks

nemsis_times_clean |>
    summarise(across(all_of(response_vars), ~ mean(is.na(.)))) |>
    pivot_longer(everything(), names_to = "metric", values_to = "pct_missing")
