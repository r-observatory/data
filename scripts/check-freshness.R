#!/usr/bin/env Rscript
# check-freshness.R - run the publish gate over the merged observatory.db.
#
# Usage: Rscript scripts/check-freshness.R [observatory.db] [sources_dir]
#
# Runs after collect-metadata.R (which writes the pipeline_metadata table this
# reads) and before the release is created, so a fatal verdict costs nothing:
# nothing has been deleted or published yet.
#
# Exit codes and side effects:
#   0, no file written        every source passed
#   0, merge-gate-failed      publish, then a later step fails the run
#   1, merge-gate-failed      a fatal source is missing, corrupt or short; do
#      plus merge-gate-refused not publish. The second marker lets the failure
#                             notification say plainly that nothing shipped.
#
# Environment:
#   GATE_OVERRIDE=true        publish despite a fatal verdict. Logged loudly,
#                             and the run still goes red.

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
})

script_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f    <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(f) else "scripts"
  }
)
source(file.path(script_dir, "merge_helpers.R"))
source(file.path(script_dir, "pipeline_metadata.R"))
source(file.path(script_dir, "merge_gate.R"))

args <- commandArgs(trailingOnly = TRUE)
db_path     <- if (length(args) >= 1) args[1] else "observatory.db"
sources_dir <- if (length(args) >= 2) args[2] else "sources"

status_file  <- "merge-gate-failed"
refused_file <- "merge-gate-refused"
for (f in c(status_file, refused_file)) if (file.exists(f)) unlink(f)

now_iso  <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
override <- gate_override_requested(Sys.getenv("GATE_OVERRIDE", ""))

if (!file.exists(db_path)) {
  cat("::error::Merged database not found at", db_path, "\n")
  quit(save = "no", status = 1)
}

con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

table_exists <- function(name) {
  nrow(dbGetQuery(con,
    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
    params = list(name))) > 0
}

# The freshness resolution, manifest fallbacks and all, already happened in
# pipeline_metadata.R. Read its output rather than repeating it.
meta <- NULL
if (table_exists("pipeline_metadata")) {
  meta <- dbGetQuery(con, "SELECT pipeline, last_checked, last_changed,
                                  released_at, expected_max_age_hours
                           FROM pipeline_metadata")
} else {
  cat("::warning::pipeline_metadata table is absent; every source will report",
      "freshness 'unknown', which never fails the run. Presence and row-floor",
      "checks still apply.\n")
}

present_dbs <- basename(Sys.glob(file.path(sources_dir, "*.db")))

# The workflow removes a source that fails PRAGMA integrity_check, which would
# otherwise be indistinguishable from one that never published.
integrity_failed <- character()
if_path <- file.path(sources_dir, ".integrity-failed")
if (file.exists(if_path)) {
  integrity_failed <- unique(trimws(readLines(if_path, warn = FALSE)))
  integrity_failed <- integrity_failed[nzchar(integrity_failed)]
}

# Row counts for the fatal floors only, read from the merged output. A table
# that did not land counts as zero rather than being skipped.
floor_tables <- unique(unlist(lapply(gate_fatal_sources(), function(s) names(s$floors))))
row_counts <- list()
for (tbl in floor_tables) {
  row_counts[[tbl]] <- if (table_exists(tbl)) {
    as.numeric(dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM %s", tbl))$n[1])
  } else 0
}

output_bytes <- as.numeric(file.size(db_path))

res <- evaluate_freshness_gate(
  meta = meta,
  present_dbs = present_dbs,
  all_source_dbs = source_dbs,
  config = pipeline_config(),
  now_iso = now_iso,
  row_counts = row_counts,
  output_bytes = output_bytes,
  integrity_failed = integrity_failed,
  override = override
)

report <- c(
  sprintf("Freshness gate at %s", now_iso),
  sprintf(paste("Stale threshold: %g x each pipeline's declared max_age_h,",
                "with that window floored at %g hours because the merge samples",
                "once a day. The fail_h column is the age each source is",
                "actually judged against."),
          gate_stale_multiplier(), gate_min_stale_window_h()),
  "",
  format_gate_table(res$rows),
  "",
  format_gate_summary(res),
  sprintf("observatory.db is %s bytes (floor %s).",
          format(output_bytes, scientific = FALSE),
          format(gate_min_output_bytes(), scientific = FALSE))
)
cat(paste(report, collapse = "\n"), "\n", sep = "")

# Put the same table on the run's summary page, so a red run explains itself
# without anyone opening the log.
summary_path <- Sys.getenv("GITHUB_STEP_SUMMARY", "")
if (nzchar(summary_path)) {
  cat(c("## Freshness gate", "", "```", report, "```", ""),
      file = summary_path, sep = "\n", append = TRUE)
}

for (p in res$problems)       cat("::warning::", p, "\n", sep = "")
for (p in res$fatal_problems) cat("::error::", p, "\n", sep = "")

if (res$run_failed) {
  writeLines(gate_status_lines(res), status_file)
}

# A second marker so the failure notification can distinguish "nothing shipped
# today" from "the data shipped and something else went wrong".
if (!res$publish_allowed) {
  writeLines("refused", refused_file)
}

if (length(res$fatal_problems) > 0 && res$override_used) {
  cat("::warning::GATE_OVERRIDE was set, so the release will be published",
      "despite the fatal problems above. This run still fails.",
      "Clear the override once the source is healthy.\n")
}

if (!res$publish_allowed) {
  cat("::error::Refusing to publish: a source the site cannot function without",
      "is missing or nearly empty. Re-run the workflow manually with the",
      "'Publish even if a fatal source fails the freshness gate' input set to",
      "true to publish anyway.\n")
  quit(save = "no", status = 1)
}

cat("Gate allows publishing.\n")
quit(save = "no", status = 0)
