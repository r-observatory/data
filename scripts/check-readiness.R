#!/usr/bin/env Rscript
# check-readiness.R — decide whether to run today's merge yet.
#
# Runs before anything is downloaded or merged, so a WAIT verdict costs one
# short job. Writes `should_merge` to GITHUB_OUTPUT for the merge job's `if`,
# and puts the reasoning on the run summary so a skipped merge explains itself
# without anyone opening the log.
#
# The source timestamps come from the same collector the freshness page uses.
# It stats sources/<db>, which is absent here because nothing has been
# downloaded yet; that yields honest NA for the integrity fields and does not
# affect the publish times this reads.

suppressPackageStartupMessages({
  library(jsonlite)
})

here <- function(f) {
  args <- commandArgs(trailingOnly = FALSE)
  d <- sub("--file=", "", grep("--file=", args, value = TRUE))
  file.path(if (length(d) == 1L && nzchar(d)) dirname(d) else "scripts", f)
}
source(here("merge_gate.R"))
source(here("pipeline_metadata.R"))
source(here("merge_readiness.R"))

now_iso <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
force   <- identical(tolower(Sys.getenv("READINESS_FORCE", "false")), "true")

# The most recent merge we published. Absent (a first run, or a deleted
# release) reads as "no previous merge", which never blocks.
last_merge_at <- tryCatch({
  out <- suppressWarnings(system2("gh",
    c("release", "view", "--repo", "r-observatory/data", "--json", "publishedAt"),
    stdout = TRUE, stderr = TRUE))
  if (!identical(as.integer(attr(out, "status") %||% 0L), 0L)) NA_character_
  else jsonlite::fromJSON(paste(out, collapse = "\n"))$publishedAt %||% NA_character_
}, error = function(e) NA_character_)

meta <- build_pipeline_metadata(collect_pipeline_metadata(), now_iso)
res  <- merge_readiness(meta, last_merge_at = last_merge_at, now_iso = now_iso)

report <- format_readiness(res, now_iso)
if (force) {
  report <- c(report,
              "  forced: dispatched by hand, so readiness is reported but not enforced")
}
cat(paste(report, collapse = "\n"), "\n", sep = "")

summary_path <- Sys.getenv("GITHUB_STEP_SUMMARY", "")
if (nzchar(summary_path)) {
  cat(paste(c("```", report, "```"), collapse = "\n"), "\n",
      file = summary_path, append = TRUE, sep = "")
}

# A late source is worth seeing in the log even on a run that proceeds.
if (length(res$not_ready)) {
  cat(sprintf("::warning::merge readiness: waiting on %s\n",
              paste(res$not_ready, collapse = ", ")))
}

decision <- if (force) TRUE else isTRUE(res$should_merge)
out_path <- Sys.getenv("GITHUB_OUTPUT", "")
if (nzchar(out_path)) {
  cat(sprintf("should_merge=%s\n", tolower(as.character(decision))),
      file = out_path, append = TRUE)
}
