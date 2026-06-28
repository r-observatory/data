#!/usr/bin/env Rscript
# collect-metadata.R — write the pipeline_metadata freshness table into
# observatory.db. Runs after merge.R in the merge workflow.
#
# Usage: Rscript scripts/collect-metadata.R [observatory.db]

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(jsonlite)
})

script_dir <- tryCatch(
  dirname(sys.frame(1)$ofile),
  error = function(e) {
    args <- commandArgs(trailingOnly = FALSE)
    f    <- sub("--file=", "", grep("--file=", args, value = TRUE))
    if (length(f) == 1L && nzchar(f)) dirname(f) else "scripts"
  }
)
source(file.path(script_dir, "pipeline_metadata.R"))

args <- commandArgs(trailingOnly = TRUE)
db   <- if (length(args) >= 1) args[1] else "observatory.db"

now_iso <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

cat("Collecting pipeline freshness metadata...\n")
fetched <- collect_pipeline_metadata()
df <- build_pipeline_metadata(fetched, now_iso)

con <- dbConnect(SQLite(), db)
on.exit(dbDisconnect(con), add = TRUE)
write_pipeline_metadata(con, df)

cat("Wrote pipeline_metadata for", nrow(df), "pipelines:\n")
print(df[c("pipeline", "last_checked", "last_changed", "data_through", "behind_upstream")])
