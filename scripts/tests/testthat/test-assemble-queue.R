# cran-queue publishes this year's snapshots as queue-current.db.zst in its
# hourly release and each closed year once, in a history-YYYY release. The merge
# rebuilds the full queue.db from them; a part that is missing, altered or short
# must abort the merge rather than publish a queue with a year gone. Nothing
# here touches the network or the real sources/ directory.

scripts_dir <- normalizePath(file.path(getwd(), "..", ".."))
source(file.path(scripts_dir, "pipeline_metadata.R"))
source(file.path(scripts_dir, "assemble_queue.R"))

needs_tools <- function() {
  for (tool in c("bash", "jq", "zstd", "sqlite3")) {
    skip_if(!nzchar(Sys.which(tool)), paste(tool, "is not available"))
  }
}

snapshot_ddl <- "
  CREATE TABLE queue_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_time TEXT NOT NULL,
    package TEXT NOT NULL, version TEXT, folder TEXT NOT NULL, submitted_at TEXT)"

write_rows <- function(path, ddl, ids, times) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, ddl)
  DBI::dbWriteTable(con, "queue_snapshots", data.frame(
    id = ids, snapshot_time = times, package = paste0("p", ids), version = "1.0",
    folder = "pending", submitted_at = NA_character_, stringsAsFactors = FALSE), append = TRUE)
}

# A cran-queue yearly release in `dir`: the current part already expanded into
# queue.db, the parts as .zst files and the manifest describing them.
build_release <- function(dir) {
  write_rows(file.path(dir, "queue.db"), snapshot_ddl, 1:2,
             c("2026-03-09 10:00:00", "2026-07-01 08:00:00"))
  parts <- list()
  for (y in c(2024L, 2025L)) {
    db <- file.path(dir, sprintf("queue-%d.db", y))
    ids <- if (y == 2024L) 3:4 else 5L
    times <- if (y == 2024L) c("2024-05-01 00:00:00", "2024-12-31 23:59:59") else "2025-06-01 00:00:00"
    write_rows(db, sub("AUTOINCREMENT", "", snapshot_ddl), ids, times)
    zst <- paste0(db, ".zst")
    system2("zstd", c("-q", "-f", shQuote(db), "-o", shQuote(zst)))
    parts[[length(parts) + 1L]] <- list(
      year = y, tag = sprintf("history-%d", y), asset = basename(zst),
      asset_bytes = file.size(zst), asset_sha256 = sha256_file(zst),
      db_bytes = file.size(db), db_sha256 = sha256_file(db),
      rows = length(ids), min = times[1], max = times[length(times)])
    unlink(db)
  }
  list(layout = "yearly", parts = parts,
       coverage = list(queue_snapshots = list(rows = 5L, min = "2024-05-01 00:00:00",
                                              max = "2026-07-01 08:00:00")))
}

queue_ids <- function(path) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbGetQuery(con, "SELECT id FROM queue_snapshots ORDER BY id")$id
}

test_that("the parts go back in with their ids and match the manifest's coverage", {
  needs_tools()
  dir <- tempfile("queue-release-"); dir.create(dir)
  manifest <- build_release(dir)

  expect_equal(assemble_queue_parts(file.path(dir, "queue.db"), manifest, dir), 3L)
  expect_equal(queue_ids(file.path(dir, "queue.db")), 1:5)
})

test_that("a missing part, an altered part or a short history stops the assembly", {
  needs_tools()
  dir <- tempfile("queue-release-"); dir.create(dir)
  manifest <- build_release(dir)
  pristine <- file.path(dir, "pristine.db")
  file.copy(file.path(dir, "queue.db"), pristine)
  fresh_db <- function() {
    file.copy(pristine, file.path(dir, "queue.db"), overwrite = TRUE)
    file.path(dir, "queue.db")
  }

  wrong_sha <- manifest
  wrong_sha$parts[[1]]$asset_sha256 <- strrep("0", 64)
  expect_error(assemble_queue_parts(fresh_db(), wrong_sha, dir), "does not match the sha256")

  more <- manifest
  more$coverage$queue_snapshots$rows <- 6L
  expect_error(assemble_queue_parts(fresh_db(), more, dir), "the manifest says 6 rows")

  unlink(file.path(dir, "queue-2025.db.zst"))
  expect_error(assemble_queue_parts(fresh_db(), manifest, dir),
               "queue-2025.db.zst from history-2025 is missing")
})

# ---------------------------------------------------------------------------
# assemble_queue() as merge.yml runs it, against a stand-in gh
# ---------------------------------------------------------------------------

workflow_lines <- function() {
  readLines(file.path(scripts_dir, "..", ".github", "workflows", "merge.yml"), warn = FALSE)
}

workflow_function <- function(lines, name) {
  start <- grep(sprintf("^\\s*%s\\(\\) \\{", name), lines)
  if (length(start) != 1L) stop("expected exactly one definition of ", name, "()")
  indent <- sub("^(\\s*).*$", "\\1", lines[start])
  end <- start - 1L + which(lines[start:length(lines)] == paste0(indent, "}"))[1]
  lines[start:end]
}

# Runs assemble_queue() in a scratch workspace whose `gh` serves releases from
# `store/<tag>/<asset>`. `skip` names assets the stand-in refuses to download.
run_assemble <- function(store, work, skip = character(0)) {
  bin <- file.path(work, "bin"); dir.create(bin, showWarnings = FALSE)
  writeLines(c(
    "#!/usr/bin/env bash",
    sprintf("store=%s", shQuote(store)),
    sprintf("skip=%s", shQuote(paste(skip, collapse = " "))),
    "if [ \"$1 $2\" = 'release view' ]; then",
    "  tag=$3; jq=''; prev=''; for a in \"$@\"; do [ \"$prev\" = --jq ] && jq=$a; prev=$a; done",
    "  name=$(printf '%s' \"$jq\" | sed -E 's/.*name == \"([^\"]+)\".*/\\1/')",
    "  wc -c < \"$store/$tag/$name\" | tr -d ' '; exit 0",
    "fi",
    "if [ \"$1 $2\" = 'release download' ]; then",
    "  tag=$3; shift 3; pat=''; dir=.",
    "  while [ $# -gt 0 ]; do case $1 in --pattern) pat=$2; shift 2;; --dir) dir=$2; shift 2;; *) shift;; esac; done",
    "  case \" $skip \" in *\" $pat \"*) echo 'HTTP 502' >&2; exit 1;; esac",
    "  cp \"$store/$tag/$pat\" \"$dir/$pat\"; exit $?",
    "fi",
    "echo \"unexpected gh call: $*\" >&2; exit 1"), file.path(bin, "gh"))
  # `stat --format` in verify_size is GNU-only, as test-dated-metrics-download.R notes.
  writeLines(c("#!/bin/sh", 'for last; do :; done', 'wc -c < "$last" | tr -d " "'),
             file.path(bin, "stat"))
  Sys.chmod(file.path(bin, c("gh", "stat")), "755")
  dir.create(file.path(work, "scripts"), showWarnings = FALSE)
  file.copy(file.path(scripts_dir, c("assemble_queue.R", "pipeline_metadata.R")),
            file.path(work, "scripts"), overwrite = TRUE)
  lines <- workflow_lines()
  script <- c("set -e", "sleep() { :; }",
              trimws(workflow_function(lines, "verify_size"), "right"),
              trimws(workflow_function(lines, "assemble_queue"), "right"),
              "assemble_queue vTEST", "echo ASSEMBLED")
  writeLines(script, file.path(work, "run.sh"))
  out <- suppressWarnings(system2("bash", file.path(work, "run.sh"), stdout = TRUE, stderr = TRUE,
                                  env = c(sprintf("PATH=%s:%s", bin, Sys.getenv("PATH")))))
  list(status = attr(out, "status") %||% 0L, out = out)
}

stage <- function() {
  dir <- tempfile("queue-merge-"); dir.create(dir)
  manifest <- build_release(dir)
  store <- file.path(dir, "store")
  dir.create(file.path(store, "vTEST"), recursive = TRUE)
  jsonlite::write_json(manifest, file.path(store, "vTEST", "manifest.json"), auto_unbox = TRUE)
  for (p in manifest$parts) {
    dir.create(file.path(store, p$tag))
    file.copy(file.path(dir, p$asset), file.path(store, p$tag, p$asset))
  }
  work <- file.path(dir, "work"); dir.create(file.path(work, "sources"), recursive = TRUE)
  file.copy(file.path(dir, "queue.db"), file.path(work, "sources", "queue.db"))
  list(store = store, work = work)
}

test_that("merge.yml's assemble_queue rebuilds the full queue.db from the parts", {
  needs_tools()
  s <- stage()

  old <- setwd(s$work); on.exit(setwd(old), add = TRUE)
  r <- run_assemble(s$store, s$work)

  expect_equal(r$status, 0L, info = paste(r$out, collapse = "\n"))
  expect_true("ASSEMBLED" %in% r$out)
  expect_equal(queue_ids(file.path(s$work, "sources", "queue.db")), 1:5)
})

test_that("merge.yml's assemble_queue aborts when a part will not download", {
  needs_tools()
  s <- stage()

  old <- setwd(s$work); on.exit(setwd(old), add = TRUE)
  r <- run_assemble(s$store, s$work, skip = "queue-2024.db.zst")

  expect_false(identical(r$status, 0L))
  expect_false("ASSEMBLED" %in% r$out)
  expect_true(any(grepl("queue-2024.db.zst from cran-queue@history-2024 did not download", r$out, fixed = TRUE)))
})

test_that("cran-queue's download pattern takes the current part and the older single files", {
  lines <- workflow_lines()
  at <- grep('if [ "$repo" = "cran-queue" ]; then', lines, fixed = TRUE)
  expect_gte(length(at), 1L)
  expect_true(any(grepl('dated_pattern="queue*.db*"', lines, fixed = TRUE)))
  hook <- grep('"$(basename "$compressed")" = "queue-current.db.zst"', lines, fixed = TRUE)
  expect_equal(length(hook), 1L)
  expect_true(grepl("assemble_queue", lines[hook + 1L], fixed = TRUE))
})
