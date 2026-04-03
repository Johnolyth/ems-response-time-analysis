setwd("d:/EMS case study/ems-data-project")

path <- "data/raw/ComputedElements.txt"

stopifnot(file.exists(path))

lines <- readLines(path, n = 2, warn = FALSE)

# con <- file(path, "r", blocking = TRUE)
# on.exit(close(con), add = TRUE)

header_line <- lines[1]
data_line <- if (length(lines) >= 2) lines[2] else ""

# header_line <- readLines(con, n = 1, warn = FALSE)
# data_line <- readLines(con, n = 1, warn = FALSE)

cat("HEADER (first 300 chars):\n", substr(header_line, 1, 300), "\n\n")
cat("ROW1   (first 300 chars):\n", substr(data_line, 1, 300), "\n")
