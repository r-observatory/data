#!/usr/bin/env Rscript
# merge.R — Merge pipeline SQLite databases into a single observatory.db
#
# Usage:
#   Rscript scripts/merge.R [sources_dir] [output_path]
#
# Defaults:
#   sources_dir = "sources"
#   output_path = "observatory.db"

library(RSQLite)

options(timeout = 60)

# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
sources_dir <- if (length(args) >= 1) args[1] else "sources"
output_path <- if (length(args) >= 2) args[2] else "observatory.db"

cat("=== Observatory DB Merge ===\n")
cat("Sources directory:", sources_dir, "\n")
cat("Output path:      ", output_path, "\n\n")

# ---------------------------------------------------------------------------
# Source databases to merge (in order)
# ---------------------------------------------------------------------------
source_dbs <- c(
  "feed.db",
  "metadata.db",
  "downloads.db",
  "queue.db"
)

# ---------------------------------------------------------------------------
# Remove old output DB if it exists, create fresh
# ---------------------------------------------------------------------------
if (file.exists(output_path)) {
  cat("Removing existing output DB:", output_path, "\n")
  unlink(output_path)
}

con <- dbConnect(SQLite(), output_path)
on.exit(dbDisconnect(con), add = TRUE)

# Set pragmas for performance
dbExecute(con, "PRAGMA journal_mode=WAL")
dbExecute(con, "PRAGMA synchronous=NORMAL")

# ---------------------------------------------------------------------------
# Track merge statistics
# ---------------------------------------------------------------------------
merge_stats <- list()

# ---------------------------------------------------------------------------
# Merge each source database
# ---------------------------------------------------------------------------
for (db_file in source_dbs) {
  src_path <- file.path(sources_dir, db_file)
  cat("--- Processing:", db_file, "---\n")

  if (!file.exists(src_path)) {
    warning("Source DB not found, skipping: ", src_path)
    merge_stats[[db_file]] <- list(
      status = "skipped",
      reason = "file not found"
    )
    next
  }

  file_size <- file.info(src_path)$size
  cat("  File size:", format(file_size, big.mark = ","), "bytes\n")

  tryCatch({
    # Attach source database
    dbExecute(con, "ATTACH DATABASE ? AS src", params = list(src_path))

    # Get list of tables from source
    tables <- dbGetQuery(con,
      "SELECT name, sql FROM src.sqlite_master
       WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    )

    table_stats <- list()

    if (nrow(tables) > 0) {
      dbExecute(con, "BEGIN TRANSACTION")

      for (i in seq_len(nrow(tables))) {
        tbl_name <- tables$name[i]
        tbl_sql  <- tables$sql[i]

        cat("  Table:", tbl_name)

        # Create table if not exists — modify the CREATE TABLE statement
        create_sql <- sub(
          "^CREATE TABLE ",
          "CREATE TABLE IF NOT EXISTS ",
          tbl_sql,
          ignore.case = TRUE
        )
        dbExecute(con, create_sql)

        # Get column list from source table for INSERT
        col_info <- dbGetQuery(con, sprintf('PRAGMA src.table_info("%s")', tbl_name))
        cols <- col_info$name
        cols_str <- paste(sprintf('"%s"', cols), collapse = ", ")

        # Copy data
        insert_sql <- sprintf(
          'INSERT OR REPLACE INTO "%s" (%s) SELECT %s FROM src."%s"',
          tbl_name, cols_str, cols_str, tbl_name
        )
        n_rows <- dbExecute(con, insert_sql)
        cat(" ->", n_rows, "rows\n")

        table_stats[[tbl_name]] <- n_rows
      }

      dbExecute(con, "COMMIT")
    }

    # Copy indexes
    indexes <- dbGetQuery(con,
      "SELECT sql FROM src.sqlite_master
       WHERE type = 'index' AND sql IS NOT NULL"
    )
    if (nrow(indexes) > 0) {
      for (j in seq_len(nrow(indexes))) {
        idx_sql <- sub(
          "^CREATE INDEX ",
          "CREATE INDEX IF NOT EXISTS ",
          indexes$sql[j],
          ignore.case = TRUE
        )
        # Also handle UNIQUE indexes
        idx_sql <- sub(
          "^CREATE UNIQUE INDEX ",
          "CREATE UNIQUE INDEX IF NOT EXISTS ",
          idx_sql,
          ignore.case = TRUE
        )
        tryCatch(
          dbExecute(con, idx_sql),
          error = function(e) {
            cat("  Warning: index creation skipped:", conditionMessage(e), "\n")
          }
        )
      }
      cat("  Copied", nrow(indexes), "indexes\n")
    }

    # Detach source database
    dbExecute(con, "DETACH DATABASE src")

    merge_stats[[db_file]] <- list(
      status = "merged",
      file_size = file_size,
      tables = table_stats
    )

  }, error = function(e) {
    warning("Error processing ", db_file, ": ", conditionMessage(e))
    merge_stats[[db_file]] <<- list(
      status = "error",
      reason = conditionMessage(e)
    )
    # Try to detach if still attached
    tryCatch(dbExecute(con, "DETACH DATABASE src"), error = function(e2) NULL)
    # Try to rollback if in transaction
    tryCatch(dbExecute(con, "ROLLBACK"), error = function(e2) NULL)
  })

  cat("\n")
}

merged_count <- sum(vapply(merge_stats, function(s) {
  !is.null(s) && identical(s$status, "merged")
}, logical(1)))
if (merged_count == 0) {
  stop("No source databases were successfully merged. Aborting.")
}
cat(sprintf("\n%d of %d sources merged successfully\n\n", merged_count, length(source_dbs)))

# ---------------------------------------------------------------------------
# Build FTS5 search index on packages table
# ---------------------------------------------------------------------------
cat("--- Building FTS5 search index ---\n")
tryCatch({
  # Check if packages table exists
  has_packages <- dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM sqlite_master
     WHERE type = 'table' AND name = 'packages'"
  )$n > 0

  if (has_packages) {
    dbExecute(con, "DROP TABLE IF EXISTS packages_fts")
    dbExecute(con, "
      CREATE VIRTUAL TABLE packages_fts USING fts5(
        name, title, description, maintainer,
        content='packages',
        content_rowid='rowid',
        tokenize=\"porter unicode61\"
      )
    ")

    dbExecute(con, "
      INSERT INTO packages_fts (rowid, name, title, description, maintainer)
      SELECT rowid, name, title, description, maintainer FROM packages
    ")

    fts_count <- dbGetQuery(con,
      "SELECT COUNT(*) AS n FROM packages_fts"
    )$n
    cat("  Indexed", fts_count, "packages in FTS5\n")
  } else {
    cat("  Skipped: packages table not found\n")
  }
}, error = function(e) {
  warning("FTS5 index creation failed: ", conditionMessage(e))
})

cat("\n")

# ---------------------------------------------------------------------------
# Enrich packages with URL/bug_reports from packages_enrichment
# ---------------------------------------------------------------------------
dbExecute(con, "BEGIN TRANSACTION")
tryCatch({

cat("--- Enriching packages ---\n")
  has_enrichment <- dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM sqlite_master
     WHERE type = 'table' AND name = 'packages_enrichment'"
  )$n > 0

  has_packages <- dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM sqlite_master
     WHERE type = 'table' AND name = 'packages'"
  )$n > 0

  if (has_enrichment && has_packages) {
    n_updated <- dbExecute(con, "
      UPDATE packages SET
        cran_url = (SELECT url FROM packages_enrichment
                    WHERE packages_enrichment.name = packages.name)
      WHERE EXISTS (
        SELECT 1 FROM packages_enrichment
        WHERE packages_enrichment.name = packages.name
        AND url IS NOT NULL AND url != ''
      )
    ")
    cat("  Updated cran_url for", n_updated, "packages\n")
  } else {
    cat("  Skipped: required tables not found\n")
  }

cat("\n")

# ---------------------------------------------------------------------------
# Enrich package_versions with removal_reasons
# ---------------------------------------------------------------------------
cat("--- Enriching package_versions with removal reasons ---\n")
  has_versions <- dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM sqlite_master
     WHERE type = 'table' AND name = 'package_versions'"
  )$n > 0

  has_removal <- dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM sqlite_master
     WHERE type = 'table' AND name = 'removal_reasons'"
  )$n > 0

  if (has_versions && has_removal) {
    n_updated <- dbExecute(con, "
      UPDATE package_versions SET removal_reason = (
        SELECT reason FROM removal_reasons
        WHERE removal_reasons.package = package_versions.package
      )
      WHERE id = (
        SELECT id FROM package_versions pv2
        WHERE pv2.package = package_versions.package
          AND pv2.event_type = 'removed'
        ORDER BY pv2.detected_at DESC LIMIT 1
      )
      AND event_type = 'removed'
    ")
    cat("  Updated removal reasons for", n_updated, "packages\n")
  } else {
    cat("  Skipped: required tables not found\n")
  }

  dbExecute(con, "COMMIT")
}, error = function(e) {
  tryCatch(dbExecute(con, "ROLLBACK"), error = function(e2) NULL)
  warning("Enrichment failed: ", conditionMessage(e))
})

cat("\n")

cat("--- Running ANALYZE ---\n")
dbExecute(con, "ANALYZE")

# ---------------------------------------------------------------------------
# Write release_notes.md
# ---------------------------------------------------------------------------
cat("--- Writing release notes ---\n")
output_size <- file.info(output_path)$size

notes <- character()
notes <- c(notes, "# Observatory DB Merge Report\n")
notes <- c(notes, sprintf("**Date:** %s\n", Sys.time()))
notes <- c(notes, sprintf(
  "**Output:** `%s` (%s)\n",
  basename(output_path),
  format(output_size, big.mark = ",")
))
notes <- c(notes, "")
notes <- c(notes, "## Sources\n")
notes <- c(notes, "| Source | Status | Size | Tables | Total Rows |")
notes <- c(notes, "|--------|--------|------|--------|------------|")

for (db_file in source_dbs) {
  stats <- merge_stats[[db_file]]
  if (is.null(stats)) {
    notes <- c(notes, sprintf("| %s | unknown | — | — | — |", db_file))
    next
  }

  if (stats$status == "skipped") {
    notes <- c(notes, sprintf(
      "| %s | skipped (%s) | — | — | — |",
      db_file, stats$reason
    ))
  } else if (stats$status == "error") {
    notes <- c(notes, sprintf(
      "| %s | error | — | — | — |",
      db_file
    ))
  } else {
    tbl_names <- names(stats$tables)
    total_rows <- sum(unlist(stats$tables))
    notes <- c(notes, sprintf(
      "| %s | merged | %s | %s (%d) | %s |",
      db_file,
      format(stats$file_size, big.mark = ","),
      paste(tbl_names, collapse = ", "),
      length(tbl_names),
      format(total_rows, big.mark = ",")
    ))
  }
}

notes <- c(notes, "")
notes <- c(notes, "## Combined Tables\n")

# List all tables in the output DB
all_tables <- dbGetQuery(con,
  "SELECT name FROM sqlite_master
   WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
   ORDER BY name"
)$name

for (tbl in all_tables) {
  row_count <- dbGetQuery(con, sprintf(
    'SELECT COUNT(*) AS n FROM "%s"', tbl
  ))$n
  notes <- c(notes, sprintf("- **%s**: %s rows", tbl, format(row_count, big.mark = ",")))
}

notes <- c(notes, "")
notes <- c(notes, sprintf(
  "\n*Total DB size: %s bytes*\n",
  format(output_size, big.mark = ",")
))

writeLines(notes, "release_notes.md")
cat("  Written to release_notes.md\n")

cat("\n=== Merge complete ===\n")
cat("Output:", output_path, "\n")
cat("Size:  ", format(output_size, big.mark = ","), "bytes\n")
