#------------------Load Packages---------------------
packages <- c(
    "tidyverse",
    "readxl",
    "janitor",
    "lubridate",
    "ggplot2",
    "scales"
)

installed <- packages %in% installed.packages()[, "Package"]
if (any(!installed)) {
    install.packages(packages[!installed])
}

lapply(packages, library, character.only = TRUE)
message("Packages loaded successfully.")
