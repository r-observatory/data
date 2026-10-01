for (f in c("fold.R", "check_flavor_fold.R", "series.R", "extract.R", "assets.R")) {
  source(file.path(getwd(), "..", "..", "history", f))
}

test_that("both pairs are built and pass their checks", {
  skip_without_zstd()
  b <- built()
  expect_setequal(list.files(b$out, pattern = "zst$|json$"),
                  c("history-2026-10-01.db.zst", "history-2026-10-01-manifest.json",
                    "history-merge-2026-10-01.db.zst", "history-merge-2026-10-01-manifest.json"))
  expect_equal(validate_history_assets(b$out, "2026-10-01"), character(0))
})

test_that("the merge-only asset holds exactly the tables the merger may allowlist", {
  skip_without_zstd()
  b <- built()
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(b$out, "history-merge-2026-10-01.db"))
  on.exit(DBI::dbDisconnect(con))
  expect_setequal(history_tables(con), HISTORY_MERGE_TABLES)
  expect_false(any(c("cran_check_flavors", "cran_check_flavor_status_history",
                     "cran_check_timing_history", "history_series_observations") %in% history_tables(con)))
  idx <- DBI::dbGetQuery(con, "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL")$name
  expect_true("ux_cran_check_issue_history_open" %in% idx)
})

test_that("each manifest carries what the merge binds an asset by", {
  skip_without_zstd()
  b <- built()
  for (kind in c("history", "history-merge")) {
    man <- jsonlite::read_json(file.path(b$out, sprintf("%s-2026-10-01-manifest.json", kind)))
    expect_equal(man$db_filename, sprintf("%s-2026-10-01.db", kind))
    expect_equal(man$asset_filename, sprintf("%s-2026-10-01.db.zst", kind))
    expect_match(man$asset_sha256, "^[0-9a-f]{64}$")
    expect_match(man$db_sha256, "^[0-9a-f]{64}$")
    expect_equal(man$asset_bytes, file.size(file.path(b$out, man$asset_filename)))
    expect_equal(man$families$`cran-metadata`$last_tag, "v20260902-060000")
    expect_equal(man$families$data$processed, 2L)
    expect_null(man$flavor_handover_tag)
  }
})

# Runs merge.yml's own expand_dated on one pair in `store`, with stand-ins for
# gh (answering from `store`) and GNU stat. Returns its output and status.
run_expand_dated <- function(store, series) {
  yml <- readLines(file.path(getwd(), "..", "..", "..", ".github", "workflows", "merge.yml"),
                   warn = FALSE)
  fun <- function(name) {
    start <- grep(sprintf("^\\s*%s\\(\\) \\{", name), yml)
    stopifnot(length(start) == 1L)
    indent <- sub("^(\\s*).*$", "\\1", yml[start])
    yml[start:(start - 1L + which(yml[start:length(yml)] == paste0(indent, "}"))[1])]
  }
  root <- withr::local_tempdir(.local_envir = parent.frame())
  bin <- file.path(root, "bin")
  work <- file.path(root, "work")
  dir.create(bin)
  dir.create(work)
  file.copy(file.path(store, paste0(series, ".db.zst")), work)
  writeLines(c("#!/usr/bin/env bash", paste0("store=", shQuote(store)),
    'if [ "$1 $2" = "release view" ]; then',
    '  while [ $# -gt 0 ]; do [ "$1" = "--jq" ] && q=$2; shift; done',
    '  for f in "$store"/*; do',
    '    printf \'{"name":"%s","size":%s,"state":"uploaded"}\\n\' "$(basename "$f")" "$(wc -c < "$f" | tr -d " ")"',
    '  done | jq -s "{assets: .}" | jq -r "$q"',
    'elif [ "$1 $2" = "release download" ]; then',
    '  while [ $# -gt 0 ]; do case "$1" in --pattern) p=$2;; --dir) d=$2;; esac; shift; done',
    '  cp "$store/$p" "$d/"',
    'else exit 1; fi'), file.path(bin, "gh"))
  writeLines(c("#!/usr/bin/env bash", 'wc -c < "${@: -1}" | tr -d " "'), file.path(bin, "stat"))
  Sys.chmod(file.path(bin, c("gh", "stat")), "755")
  script <- file.path(root, "run.sh")
  writeLines(c(fun("verify_size"), fun("sha256_of"), fun("expand_dated"),
               sprintf("expand_dated data %s history %s.db %s", series, series, shQuote(work))),
             script)
  out <- withr::with_path(bin, system2("bash", script, stdout = TRUE, stderr = TRUE))
  list(out = out, status = attr(out, "status") %||% 0L, db = file.path(work, paste0(series, ".db")))
}

test_that("the merge binds the merge-only pair by its manifest", {
  skip_without_zstd()
  skip_if(!nzchar(Sys.which("jq")), "jq is not available")
  b <- built()
  got <- run_expand_dated(b$out, "history-merge-2026-10-01")
  expect_equal(got$status, 0L)
  expect_true(any(grepl("manifest OK: history-merge-2026-10-01.db = ", got$out, fixed = TRUE)),
              info = paste(got$out, collapse = "\n"))
  expect_false(any(grepl("relying on", got$out, fixed = TRUE)))
  man <- jsonlite::read_json(file.path(b$out, "history-merge-2026-10-01-manifest.json"))
  expect_equal(history_file_sha256(got$db), man$db_sha256)
})

test_that("a changed asset or an inconsistent history fails the checks", {
  skip_without_zstd()
  b <- built()
  zst <- file.path(b$out, "history-merge-2026-10-01.db.zst")
  bytes <- readBin(zst, "raw", file.size(zst))
  bytes[length(bytes)] <- as.raw(bitwXor(as.integer(bytes[length(bytes)]), 255L))
  writeBin(bytes, zst)
  expect_true(any(grepl("merge: asset sha256 differs", validate_history_assets(b$out, "2026-10-01"))))
  mpath <- file.path(b$out, "history-2026-10-01-manifest.json")
  man <- jsonlite::read_json(mpath)
  man$db_filename <- "history.db"
  jsonlite::write_json(man, mpath, auto_unbox = TRUE, digits = NA)
  expect_true(any(grepl("full: manifest describes history.db", validate_history_assets(b$out, "2026-10-01"))))
  con <- DBI::dbConnect(RSQLite::SQLite(), b$history)
  DBI::dbExecute(con, "UPDATE cran_check_issue_history SET episode_seq = 3")
  DBI::dbDisconnect(con)
  expect_error(build_history_assets(b$history, file.path(b$dir, "out2"), "2026-10-02", "x"),
               "cran_check_issue_history: 1 rows fail the gaps check")
})

test_that("the newest complete pair of each kind is the one read", {
  assets <- data.frame(
    name = c("history-2026-10-01.db.zst", "history-2026-10-01-manifest.json",
             "history-2026-10-05.db.zst", "history-merge-2026-10-03.db.zst",
             "history-merge-2026-10-03-manifest.json", "history-2026-10-04.db.zst",
             "history-2026-10-04-manifest.json"),
    state = c(rep("uploaded", 6), "open"), stringsAsFactors = FALSE)
  expect_equal(newest_history_pair(assets, "full")$db, "history-2026-10-01.db.zst")
  expect_equal(newest_history_pair(assets, "merge")$manifest, "history-merge-2026-10-03-manifest.json")
  expect_null(newest_history_pair(assets[0, ], "full"))
})

test_that("a run with no local history starts fresh only when the release is missing", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  target <- file.path(withr::local_tempdir(), "history.db")
  expect_false(restore_prior_history(release_io(store, 404L), target))
  expect_false(file.exists(target))
  expect_error(restore_prior_history(release_io(store, 502L), target), "HTTP 502")
  expect_error(restore_prior_history(release_io(store, 200L), target), "holds no full pair")
  file.copy(file.path(b$out, c("history-2026-10-01.db.zst", "history-2026-10-01-manifest.json")), store)
  expect_true(restore_prior_history(release_io(store, 200L), target))
  man <- jsonlite::read_json(file.path(b$out, "history-2026-10-01-manifest.json"))
  expect_equal(history_file_sha256(target), man$db_sha256)
})

test_that("a published pair that does not match its manifest is never a fresh start", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  file.copy(file.path(b$out, c("history-2026-10-01.db.zst", "history-2026-10-01-manifest.json")), store)
  writeBin(as.raw(1:10), file.path(store, "history-2026-10-01.db.zst"))
  target <- file.path(withr::local_tempdir(), "history.db")
  expect_error(restore_prior_history(release_io(store, 200L), target), "not starting fresh")
  expect_false(file.exists(target))
})

test_that("a new history may grow but never lose rows or recorded tags", {
  skip_without_zstd()
  b <- built()
  prior <- file.path(b$out, "history-2026-10-01.db")
  expect_equal(history_regressions(prior, prior), character(0))
  smaller <- file.path(b$dir, "smaller.db")
  file.copy(prior, smaller)
  con <- DBI::dbConnect(RSQLite::SQLite(), smaller)
  DBI::dbExecute(con, "DELETE FROM cran_check_timing_history WHERE package = 'cli'")
  DBI::dbExecute(con, "DELETE FROM history_snapshots WHERE tag = 'v2026-09-01'")
  DBI::dbDisconnect(con)
  lost <- history_regressions(prior, smaller)
  expect_true(any(grepl("cran_check_timing_history fell", lost)))
  expect_true(any(grepl("1 recorded tags are missing", lost)))
})
