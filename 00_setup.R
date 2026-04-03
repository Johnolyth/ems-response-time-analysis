#-----------------------Setup------------------------
library(tidyverse)

message("R project setup syccessful")

folders <- c(
    "data/raw",
    "data/clean",
    "scripts",
    "notebooks",
    "ref",
    "output",
    "admin"
)

for (f in folders) {
    if (!dir.exists(f)) dir.create(f, recursive = TRUE)
}

test_df <- tibble(
    id = 1:5,
    value = rnorm(5)
)

print(test_df)
ggplot(test_df, aes(id, value)) +
    geom_point() +
    geom_line()
