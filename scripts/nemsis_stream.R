library(readr)
library(stringr)
library(janitor)
library(R.utils)

nemsis_stream <- function(
  file_path, chunk_size = 200000,
  output_dir = "data/clean/nemsis_chunks"
) {
    if (!file.exists(file_path)) {
        stop("File not found: ", file_path)
    }

    if (!dir.exists(output_dir)) {
        dir.create(output_dir, recursive = TRUE)
    }


    # Count total lines (Windows-safe, R.utils)
    total_lines <- tryCatch(
        R.utils::countLines(file_path),
        error = function(e) {
            message("WARN: countLines failed (continuing without ETA): ", e$message)
            NA_integer_
        }
    )

    total_chunks <- if (is.na(total_lines)) NA_integer_ else ceiling(total_lines / chunk_size)

    # For ETA calculations
    chunk_times <- c()
    bar_width <- 30

    # Streaming connection
    con <- file(file_path, "r", blocking = TRUE)
    on.exit(close(con), add = TRUE)

    # read header
    header_line <- readLines(con, n = 1, warn = FALSE)

    token_pattern <- "\\s*~\\|~\\s*"
    to_sep <- "\t"

    header2 <- gsub(token_pattern, to_sep, header_line)

    col_names <- strsplit(header2, to_sep, fixed = TRUE)[[1]]
    col_names <- trimws(col_names)
    col_names <- gsub("^'+|'+$", "", col_names)

    stopifnot(length(col_names) > 1)


    message("Detected columns: ", length(col_names))
    message("First few: ", paste(head(col_names, 6), collapse = ", "))
    message("Header cols: ", length(col_names), " | first: ", paste(head(col_names, 5), collapse = ", "))


    table_tag <- tools::file_path_sans_ext(basename(file_path))
    table_tag <- gsub("[^A-Za-z0-9_]+", "_", table_tag)
    # table_tag <- gsub("[^[:alnum:]_]+", "_", table_tag)

    chunk_index <- 0

    repeat {
        start_time <- Sys.time()

        lines <- readLines(con, n = chunk_size, warn = FALSE)
        if (length(lines) == 0) break

        lines2 <- gsub(token_pattern, to_sep, lines)

        # Stream next chunk

        chunk <- tryCatch(
            readr::read_delim(
                I(paste(lines2, collapse = "\n")),
                delim = to_sep,
                col_names = col_names,
                show_col_types = FALSE,
                quote = "",
                ## con,
                ## sep = sep_token,
                ## nrows = chunk_size,
                ## header = FALSE,
                ## fill = TRUE,
                ## col.names = col_names,
                ## stringsAsFactors = FALSE,
                ## comment.char = ""
            ),
            error = function(e) {
                message("READ ERROR: ", e$message)
                data.frame()
            }
        )

        message("Chunk read: rows = ", nrow(chunk), " cols = ", ncol(chunk))

        if (nrow(chunk) == 0) break # truly finished

        chunk_index <- chunk_index + 1

        out_file <- file.path(
            output_dir,
            sprintf("%s_chunk_%04d.csv", table_tag, chunk_index)
        )

        message("Writing -> ", out_file)

        readr::write_csv(chunk, out_file)

        message("Wrote chunk ", chunk_index)
        # =====================================OLD============================================
        # out_file <- sprintf("%s/nemsis_chunk_%04d.csv", output_dir, chunk_index)
        # write.csv(chunk, out_file, row.names = FALSE)
        # ====================================================================================
        # timing
        elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
        chunk_times <- c(chunk_times, elapsed)

        # Keep only last 10 durations for smoother ETA
        if (length(chunk_times) > 10) {
            chunk_times <- chunk_times[(length(chunk_times) - 9):length(chunk_times)]
        }

        if (is.na(total_chunks)) {
            cat(sprintf("\rChunk %d | ETA disabled    ", chunk_index))
            flush.console()
        } else {
            avg_time <- mean(chunk_times)
            chunks_left <- total_chunks - chunk_index
            time_remaining <- chunks_left * avg_time

            # progress bar

            pct <- chunk_index / total_chunks
            filled <- round(pct * bar_width)
            bar <- paste0(
                "[",
                paste0(rep("#", filled), collapse = ""),
                paste0(rep("-", bar_width - filled), collapse = ""),
                "]"
            )

            # ETA formatting

            eta <- if (time_remaining > 3600) {
                sprintf("%.2f hours", time_remaining / 3600)
            } else if (time_remaining > 60) {
                sprintf("%.1f minutes", time_remaining / 60)
            } else {
                sprintf("%.1f seconds", time_remaining)
            }

            cat(sprintf(
                "\r%s %3.0f%% | Chunk %d/%d | ETA: %s    ",
                bar, pct * 100, chunk_index, total_chunks, eta
            ))
            flush.console()
        }
    }

    cat("\n\nCompleted! All chunks saved to:", output_dir, "\n")
    invisible(output_dir)
}
