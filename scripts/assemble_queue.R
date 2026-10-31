#!/usr/bin/env Rscript
# assemble_queue.R — rebuild cran-queue's full queue.db from its yearly layout.
#
# cran-queue publishes this year's snapshots in its hourly release as
# queue-current.db.zst, and each closed year once, in its own history-YYYY
# release, as queue-YYYY.db.zst; the manifest beside the current part lists the
# parts with their sha256 and row counts. merge.yml downloads them and calls
#   Rscript scripts/assemble_queue.R sources/queue.db manifest.json parts/
# with sources/queue.db already expanded from queue-current.db.zst.
#
# Every part must match the manifest before and after decompressing, and the
# result must match the manifest's coverage exactly. Anything else stops with a
# non-zero exit and merge.yml aborts the merge: a queue with a year missing would
# publish as history that never existed.

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(jsonlite)
})

here <- function(f) {
  args <- commandArgs(trailingOnly = FALSE)
  d <- sub("--file=", "", grep("--file=", args, value = TRUE))
  file.path(if (length(d) == 1L && nzchar(d)) dirname(d) else "scripts", f)
}
if (!exists("sha256_file")) source(here("pipeline_metadata.R"))

#' Insert every part `manifest` lists into the queue.db at `db_path`, then check
#' the result against the manifest's coverage. Returns the rows inserted.
assemble_queue_parts <- function(db_path, manifest, parts_dir) {
  parts <- manifest$parts
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  total <- 0L
  for (p in parts) {
    zst <- file.path(parts_dir, p$asset)
    if (!file.exists(zst)) stop(sprintf("%s from %s is missing", p$asset, p$tag))
    if (!identical(sha256_file(zst), p$asset_sha256)) {
      stop(sprintf("%s from %s does not match the sha256 the manifest lists", p$asset, p$tag))
    }
    db <- sub("\\.zst$", "", zst)
    status <- system2("zstd", c("-dq", "-f", shQuote(zst), "-o", shQuote(db)))
    if (!identical(status, 0L) || !identical(sha256_file(db), p$db_sha256)) {
      stop(sprintf("%s from %s did not expand to the database the manifest lists", p$asset, p$tag))
    }
    DBI::dbExecute(con, sprintf("ATTACH %s AS part", DBI::dbQuoteString(con, db)))
    held <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM part.queue_snapshots")$n
    if (held != as.integer(p$rows)) {
      DBI::dbExecute(con, "DETACH part")
      stop(sprintf("%s holds %d rows, the manifest lists %d", p$asset, held, as.integer(p$rows)))
    }
    n <- DBI::dbWithTransaction(con, DBI::dbExecute(con, "
      INSERT INTO main.queue_snapshots (id, snapshot_time, package, version, folder, submitted_at)
      SELECT id, snapshot_time, package, version, folder, submitted_at
        FROM part.queue_snapshots ORDER BY id"))
    DBI::dbExecute(con, "DETACH part")
    unlink(db)
    total <- total + as.integer(n)
  }

  got <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n, MIN(snapshot_time) AS lo, MAX(snapshot_time) AS hi FROM queue_snapshots")
  want <- manifest$coverage$queue_snapshots
  if (is.null(want) || got$n != as.integer(want$rows) ||
      !identical(got$lo, want$min) || !identical(got$hi, want$max)) {
    stop(sprintf("assembled queue_snapshots has %d rows %s to %s; the manifest says %s rows %s to %s",
                 got$n, got$lo, got$hi, want$rows %||% "?", want$min %||% "?", want$max %||% "?"))
  }
  invisible(total)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) != 3L) stop("usage: assemble_queue.R <queue.db> <manifest.json> <parts dir>")
  manifest <- jsonlite::fromJSON(args[[2]], simplifyVector = FALSE)
  n <- assemble_queue_parts(args[[1]], manifest, args[[3]])
  cat(sprintf("  queue.db: %d history parts, %d snapshots inserted; %d snapshots %s to %s, as the manifest lists\n",
              length(manifest$parts), n, as.integer(manifest$coverage$queue_snapshots$rows),
              manifest$coverage$queue_snapshots$min, manifest$coverage$queue_snapshots$max))
}
