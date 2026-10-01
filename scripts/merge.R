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

# Resolve script directory robustly (Rscript --file=... vs source()).
merge_script_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f    <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(f) else "scripts"
  }
)
source(file.path(merge_script_dir, "merge_helpers.R"))

# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
sources_dir <- if (length(args) >= 1) args[1] else "sources"
output_path <- if (length(args) >= 2) args[2] else "observatory.db"

cat("=== Observatory DB Merge ===\n")
cat("Sources directory:", sources_dir, "\n")
cat("Output path:      ", output_path, "\n\n")

# source_dbs and source_tables are defined in merge_helpers.R (sourced above)
# so they can be unit-tested. See scripts/tests/testthat/test-source-config.R.

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
# Merge each source database
# ---------------------------------------------------------------------------
# One entry per source in source_dbs: merged, skipped (file not found) or
# error. The loop lives in merge_helpers.R so the tests run it, allowlist
# lookup included, against real SQLite files.
merge_stats <- merge_sources(con, sources_dir)

# A source that published without some of its tables must still fail the run,
# so what failed goes to the gate, which runs as a later step.
merge_failed <- merge_failures(merge_stats)
write_merge_failures(file.path(sources_dir, ".merge-failed"), merge_failed)
if (length(merge_failed)) {
  cat(sprintf("%d source(s) did not merge whole:\n", length(merge_failed)))
  cat(sprintf("  %s: %s\n", names(merge_failed), merge_failed), sep = "")
}

merged_count <- sum(vapply(merge_stats, function(s) {
  !is.null(s) && identical(s$status, "merged")
}, logical(1)))
if (merged_count == 0) {
  stop("No source databases were successfully merged. Aborting.")
}
cat(sprintf("\n%d of %d sources merged successfully\n\n", merged_count, length(source_dbs)))

# Fail loud if the task-view source was present but its tables did not land
# (a partial merger edit would otherwise drop them silently; the viewer's
# empty-state would mask it).
ctv_src <- file.path(sources_dir, "cran-task-views.db")
present_tables <- dbGetQuery(con,
  "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")$name
ctv_missing <- missing_expected_tables(
  file.exists(ctv_src),
  c("cran_task_views", "cran_task_view_events", "cran_task_view_membership"),
  present_tables)
if (length(ctv_missing) > 0L) {
  stop("cran-task-views tables missing after merge: ", paste(ctv_missing, collapse = ", "))
}

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
# Build the Fedora COPR availability registry (copr_packages)
# ---------------------------------------------------------------------------
cat("--- Building COPR registry ---\n")
tryCatch({
  mon <- jsonlite::fromJSON(
    "https://copr.fedorainfracloud.org/api_3/monitor?ownername=iucar&projectname=cran",
    simplifyVector = FALSE)
  rows <- copr_rows_from_monitor(mon)
  rows <- rows[!duplicated(rows$name_lower), , drop = FALSE]
  if (nrow(rows) > 0) {
    dbExecute(con, "DROP TABLE IF EXISTS copr_packages")
    dbExecute(con, "CREATE TABLE copr_packages (name TEXT PRIMARY KEY, name_lower TEXT NOT NULL)")
    dbWriteTable(con, "copr_packages", rows, append = TRUE)
    dbExecute(con, "CREATE INDEX idx_copr_packages_lower ON copr_packages(name_lower)")
    cat("  Catalogued", nrow(rows), "COPR packages\n")
  } else {
    cat("  Skipped: no COPR packages fetched\n")
  }
}, error = function(e) warning("COPR registry build failed: ", conditionMessage(e)))
cat("\n")

# ---------------------------------------------------------------------------
# Build the R release-date table (r_versions)
# ---------------------------------------------------------------------------
cat("--- Building R versions table ---\n")
tryCatch({
  lst  <- jsonlite::fromJSON("https://api.r-hub.io/rversions/r-versions", simplifyVector = FALSE)
  rows <- r_versions_from_list(lst)
  rows <- rows[!duplicated(rows$version), , drop = FALSE]
  if (nrow(rows) > 0) {
    dbExecute(con, "DROP TABLE IF EXISTS r_versions")
    dbExecute(con, "CREATE TABLE r_versions (version TEXT PRIMARY KEY, released DATE NOT NULL)")
    dbWriteTable(con, "r_versions", rows, append = TRUE)
    dbExecute(con, "CREATE INDEX idx_r_versions_released ON r_versions(released)")
    cat("  Loaded", nrow(rows), "R versions\n")
  } else {
    cat("  Skipped: no R versions fetched\n")
  }
}, error = function(e) warning("R versions build failed: ", conditionMessage(e)))
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
# Give removal events CRAN's own reason from cran_archive_history
# ---------------------------------------------------------------------------
cat("--- Enriching package_versions with removal reasons ---\n")
  n_updated <- enrich_removal_reasons(con)
  cat("  Gave", n_updated, "removal events CRAN's archive reason\n")

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
  notes <- c(notes, source_note_row(db_file, merge_stats[[db_file]]))
}

if (length(merge_failed)) {
  notes <- c(notes, "", "## Did not merge whole\n")
  notes <- c(notes, sprintf("- **%s**: %s", names(merge_failed), merge_failed))
}

# Index names the output already held elsewhere, so they went on under their
# table's name.
renamed <- do.call(rbind, lapply(merge_stats, function(s) {
  if (is.null(s$indexes)) return(NULL)
  s$indexes[s$indexes$outcome == "renamed", , drop = FALSE]
}))
if (!is.null(renamed) && nrow(renamed)) {
  notes <- c(notes, "", "## Renamed indexes\n")
  notes <- c(notes, sprintf("- `%s` on %s as `%s` (%s)", renamed$index, renamed$table,
                            renamed$created_as, renamed$note))
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
