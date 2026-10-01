# Fixtures shared by the test-history-*.R files.

# The producers' CREATE TABLE statements, by table name.
history_source_ddl <- local({
  path <- file.path(getwd(), "fixtures", "history-source-ddl.sql")
  if (!file.exists(path)) return(character(0))
  text <- readLines(path)
  text <- paste(text[!grepl("^--", text)], collapse = "\n")
  stmts <- trimws(strsplit(text, ";", fixed = TRUE)[[1]])
  stmts <- stmts[nzchar(stmts)]
  names(stmts) <- gsub("`", "", regmatches(stmts, regexpr("(?<=CREATE TABLE )`?[A-Za-z0-9_]+",
                                                            stmts, perl = TRUE)))
  stmts
})

# A db at `path` holding `tables` (name -> data.frame), each made by its
# producer's DDL unless `ddl` names another statement.
write_source_db <- function(path, tables, ddl = list()) {
  unlink(path)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con))
  for (t in names(tables)) {
    DBI::dbExecute(con, ddl[[t]] %||% history_source_ddl[[t]])
    if (nrow(tables[[t]]) > 0L) DBI::dbAppendTable(con, t, tables[[t]])
  }
  path
}

history_test_db <- function(env = parent.frame()) {
  path <- withr::local_tempfile(fileext = ".db", .local_envir = env)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  withr::defer(DBI::dbDisconnect(con), envir = env)
  ensure_history_ledger(con)
  con
}

# A gh release listing.
releases <- function(tags, published, draft = FALSE) {
  data.frame(tag = tags, published_at = published, is_draft = draft, stringsAsFactors = FALSE)
}

# A metadata.db with the three check tables; `statuses` gives one row per
# (package, flavor) in a fixed grid. `seeded` adds cran-metadata's own
# per-flavor tables, as its releases carry them once it has seeded.
metadata_snapshot <- function(dir, name, statuses, deadline = "2026-10-10", seeded = FALSE) {
  path <- file.path(dir, name)
  unlink(path)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con))
  for (t in c("cran_check_results", "cran_check_issues", "cran_check_deadlines")) {
    DBI::dbExecute(con, history_source_ddl[[t]])
  }
  grid <- expand.grid(flavor = c("r-devel-linux-x86_64-debian-clang", "r-release-macos-arm64"),
                      package = c("cli", "polle"), stringsAsFactors = FALSE)
  DBI::dbAppendTable(con, "cran_check_results", data.frame(
    package = grid$package, flavor = grid$flavor, status = statuses,
    tinstall = 1, tcheck = seq_along(statuses) + nchar(name), ttotal = 2))
  DBI::dbExecute(con, "INSERT INTO cran_check_issues (package, version, kind, href)
                       VALUES ('polle', '1.6.4', 'M1mac', 'https://example.org/polle')")
  DBI::dbExecute(con, "INSERT INTO cran_check_deadlines (package, episode_seq, deadline,
                         first_seen, last_seen) VALUES ('polle', 1, ?, '2026-09-01', '2026-09-01')",
                 params = list(deadline))
  if (seeded) ensure_check_flavor_tables(con)
  path
}

# A stand-in for gh: `files` maps tag -> local db path; `fail` maps tag -> the
# number of downloads that deliver a truncated file before one succeeds;
# `asset` is one asset name, or names by tag.
fake_io <- function(listing, files, fail = list(), free = 500, asset = "metadata.db",
                    digest = TRUE) {
  state <- new.env()
  state$downloads <- character(0)
  state$sleeps <- numeric(0)
  io <- default_history_io()
  io$list_releases <- function(repo) listing
  io$asset_info <- function(repo, tag) {
    p <- files[[tag]]
    data.frame(name = if (is.null(names(asset))) asset else asset[[tag]], size = file.size(p),
               digest = if (digest) paste0("sha256:", history_file_sha256(p)) else NA_character_,
               state = "uploaded", stringsAsFactors = FALSE)
  }
  io$download <- function(repo, tag, name, dir) {
    state$downloads <- c(state$downloads, tag)
    left <- fail[[tag]] %||% 0L
    if (left > 0L) {
      fail[[tag]] <<- left - 1L
      writeBin(readBin(files[[tag]], "raw", 100L), file.path(dir, name))
      return(TRUE)
    }
    file.copy(files[[tag]], file.path(dir, name), overwrite = TRUE)
  }
  io$free_gib <- function(path) free
  io$sleep <- function(s) state$sleeps <- c(state$sleeps, s)
  io$today <- function() "2026-10-01"
  io$now <- function() "2026-10-01T00:00:00Z"
  io$state <- state
  io
}

# An observatory.db with the four data tables the merge-only asset carries.
data_snapshot <- function(dir, name, day, total = 10L) {
  write_source_db(file.path(dir, name), list(
    pipeline_metadata = data.frame(
      pipeline = c("cran-feed", "data"), repo = c("r-observatory/cran-feed", "r-observatory/data"),
      release_tag = c(paste0("v", gsub("-", "", day), "-060000"), NA),
      released_at = paste0(day, "T06:00:00Z"), last_checked = paste0(day, "T08:00:00Z"),
      fetched_at = paste0(day, "T08:00:00Z"), stringsAsFactors = FALSE),
    conda_forge_downloads_summary = data.frame(package = "cli", origin = "cran",
                                               total_30d = total, stringsAsFactors = FALSE),
    bioconda_downloads_summary = data.frame(package = "limma", origin = "bioc",
                                            total_30d = total, stringsAsFactors = FALSE),
    bioc_downloads_summary = data.frame(package = "limma", category = "Software",
                                        total_12mo = total * 12L, stringsAsFactors = FALSE)))
}

# history.db folded from two cran-metadata and two observatory.db releases.
small_history <- function(dir) {
  path <- file.path(dir, "history.db")
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con))
  ensure_history_ledger(con)
  files <- list(
    "v20260901-060000" = metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK")),
    "v20260902-060000" = metadata_snapshot(dir, "m2.db", c("OK", "NOTE", "ERROR", "OK"), "2026-10-17"),
    "v2026-09-01" = data_snapshot(dir, "d1.db", "2026-09-01"),
    "v2026-09-02" = data_snapshot(dir, "d2.db", "2026-09-02", 12L))
  io <- fake_io(releases(names(files), c("2026-09-01T06:00:09Z", "2026-09-02T06:00:09Z",
                                         "2026-09-01T08:30:00Z", "2026-09-02T08:30:00Z")),
                files, asset = c("v20260901-060000" = "metadata.db", "v20260902-060000" = "metadata.db",
                                 "v2026-09-01" = "observatory.db", "v2026-09-02" = "observatory.db"))
  run_extraction(con, file.path(dir, "w"), io)
  path
}

skip_without_zstd <- function() skip_if(!nzchar(Sys.which("zstd")), "zstd is not available")

built <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  path <- small_history(dir)
  out <- file.path(dir, "out")
  build_history_assets(path, out, "2026-10-01", "2026-10-01T09:00:00Z")
  list(dir = dir, history = path, out = out)
}

# A stand-in release that keeps uploaded files in `store`.
release_io <- function(store, status = 200L) {
  io <- default_history_io()
  state <- new.env()
  state$created <- NULL
  state$uploaded <- character(0)
  io$release_http_status <- function(repo, tag) status
  io$asset_info <- function(repo, tag) {
    names <- list.files(store)
    data.frame(name = names, size = file.size(file.path(store, names)),
               digest = rep(NA_character_, length(names)),
               state = rep("uploaded", length(names)), stringsAsFactors = FALSE)
  }
  io$download <- function(repo, tag, name, dir) {
    file.exists(file.path(store, name)) &&
      file.copy(file.path(store, name), file.path(dir, name), overwrite = TRUE)
  }
  io$create_release <- function(args) {
    state$created <- args
    TRUE
  }
  io$upload <- function(repo, tag, files) {
    state$uploaded <- c(state$uploaded, basename(files))
    all(file.copy(files, store))
  }
  io$state <- state
  io
}
