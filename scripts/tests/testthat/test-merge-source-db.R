# Edition 3, so expect_warning() lets through any warning it was not written for.
testthat::local_edition(3)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

# A cut-down vcs-signals-summary.db. The real one carries more columns and
# tables; what matters here is that the allowlist, the verbatim CREATE TABLE
# and the index copy all run against a real attached SQLite file.
write_vcs_summary <- function(path, with_links) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "CREATE TABLE vcs_signals_summary (
    package TEXT NOT NULL, origin TEXT NOT NULL, repo_id TEXT,
    PRIMARY KEY (package, origin))")
  DBI::dbExecute(con, "INSERT INTO vcs_signals_summary VALUES
    ('rtika2', 'cran', 'github.com/o/rtika2'), ('limma', 'bioc', 'github.com/b/limma')")
  # repo_packages is today's mapping only, and stays out of observatory.db.
  DBI::dbExecute(con, "CREATE TABLE repo_packages (
    repo_id TEXT NOT NULL, package TEXT NOT NULL, origin TEXT NOT NULL,
    resolved_from TEXT NOT NULL, PRIMARY KEY (repo_id, package, origin))")
  DBI::dbExecute(con, "CREATE INDEX idx_rp_package ON repo_packages(package)")
  DBI::dbExecute(con, "INSERT INTO repo_packages VALUES
    ('github.com/o/rtika2', 'rtika2', 'cran', 'URL')")
  if (with_links) {
    DBI::dbExecute(con, "CREATE TABLE repo_package_links (
      repo_id TEXT NOT NULL, package TEXT NOT NULL, origin TEXT NOT NULL,
      first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
      PRIMARY KEY (repo_id, package, origin))")
    # As vcs-signals declares it: package alone, which is selective enough.
    DBI::dbExecute(con, "CREATE INDEX idx_rpl_package ON repo_package_links(package)")
    # rtika is delisted: no summary row, but its link is still here.
    DBI::dbExecute(con, "INSERT INTO repo_package_links VALUES
      ('github.com/ropensci/rtika', 'rtika', 'cran', '2026-07-07', '2026-08-22'),
      ('github.com/o/rtika2', 'rtika2', 'cran', '2026-07-07', '2026-09-14'),
      ('github.com/b/limma', 'limma', 'bioc', '2026-07-07', '2026-09-14')")
  }
  invisible(path)
}

write_db <- function(path, sql) {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  for (stmt in sql) DBI::dbExecute(con, stmt)
  invisible(path)
}

write_task_views <- function(path) {
  write_db(path, c(
    "CREATE TABLE cran_task_views (name TEXT PRIMARY KEY)",
    "CREATE TABLE cran_task_view_events (name TEXT, event TEXT)",
    "CREATE TABLE cran_task_view_membership (name TEXT, package TEXT)",
    "INSERT INTO cran_task_views VALUES ('Bayesian')"))
}

# The copy narrates each table to the merge log; keep that out of the test run.
quiet_merge_source_db <- function(con, src_path, allow) {
  out <- NULL
  utils::capture.output(out <- merge_source_db(con, src_path, allow))
  out
}

# merge_sources walks the whole source list and a test directory holds only the
# sources the test is about, so the rest are reported missing. Those warnings
# are expected here; any other warning still reaches the test.
quiet_merge_sources <- function(con, sources_dir) {
  out <- NULL
  withCallingHandlers(
    utils::capture.output(out <- merge_sources(con, sources_dir)),
    warning = function(w) {
      if (startsWith(conditionMessage(w), "Source DB not found")) {
        invokeRestart("muffleWarning")
      }
    })
  out
}

output_tables <- function(con) {
  DBI::dbGetQuery(con, "SELECT name FROM main.sqlite_master WHERE type = 'table'")$name
}

test_that("the package-to-repository links land with their rows, key and index", {
  dir <- withr::local_tempdir()
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  # Through the source loop, so the allowlist is the one the merge looks up by
  # file name.
  stats <- quiet_merge_sources(con, dir)[["vcs-signals-summary.db"]]$tables

  expect_true("repo_package_links" %in% output_tables(con))
  expect_equal(stats$repo_package_links, 3)
  got <- DBI::dbGetQuery(con, "SELECT package, last_seen FROM repo_package_links
                               WHERE package = 'rtika'")
  expect_equal(got$last_seen, "2026-08-22")

  info <- DBI::dbGetQuery(con, "PRAGMA main.table_info(repo_package_links)")
  key <- info[info$pk > 0, , drop = FALSE]
  expect_equal(key$name[order(key$pk)], c("repo_id", "package", "origin"))
  expect_error(DBI::dbExecute(con, "INSERT INTO repo_package_links VALUES
    ('github.com/b/limma', 'limma', 'bioc', '2026-09-15', '2026-09-15')"), "UNIQUE")

  idx <- DBI::dbGetQuery(con, "PRAGMA main.index_list(repo_package_links)")
  expect_true("idx_rpl_package" %in% idx$name)
  expect_equal(DBI::dbGetQuery(con, "PRAGMA main.index_info(idx_rpl_package)")$name,
               "package")

  # The live mapping keeps its meaning: it is still not copied.
  expect_false("repo_packages" %in% output_tables(con))
})

test_that("a summary published before the link table existed still merges", {
  dir <- withr::local_tempdir()
  src <- write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = FALSE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  expect_no_error(
    stats <- quiet_merge_source_db(con, src,
                                   tables_to_merge_from("vcs-signals-summary.db", source_tables)))

  expect_equal(stats$vcs_signals_summary, 2)
  expect_false("repo_package_links" %in% output_tables(con))
  expect_null(stats$repo_package_links)
  # Detached again, so the next source in the loop can attach as src.
  expect_false("src" %in% DBI::dbGetQuery(con, "PRAGMA database_list")$name)
})

test_that("a copy that fails partway rolls back and lets go of src", {
  dir <- withr::local_tempdir()
  src <- write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))
  # An earlier source already made a table by this name with other columns, so
  # the insert fails after vcs_signals_summary has been copied.
  DBI::dbExecute(con, "CREATE TABLE repo_package_links (repo_id TEXT)")

  expect_error(
    quiet_merge_source_db(con, src,
                          tables_to_merge_from("vcs-signals-summary.db", source_tables)),
    "no column named package")

  expect_false("src" %in% DBI::dbGetQuery(con, "PRAGMA database_list")$name)
  # The source lands whole or not at all.
  expect_false("vcs_signals_summary" %in% output_tables(con))
})

test_that("a source that fails partway does not cost the sources after it", {
  dir <- withr::local_tempdir()
  # queue.db merges every table it has. One named like a vcs table but with
  # other columns makes the vcs copy fail partway, the source just before the
  # task views.
  write_db(file.path(dir, "queue.db"), "CREATE TABLE repo_package_links (repo_id TEXT)")
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  write_task_views(file.path(dir, "cran-task-views.db"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  expect_warning(stats <- quiet_merge_sources(con, dir),
                 "Error processing vcs-signals-summary.db")

  status <- vapply(stats, function(s) s$status, character(1))
  expect_equal(status[c("queue.db", "vcs-signals-summary.db", "cran-task-views.db")],
               c("queue.db" = "merged", "vcs-signals-summary.db" = "error",
                 "cran-task-views.db" = "merged"))
  # merge.R refuses the release when these are missing.
  expect_equal(
    missing_expected_tables(TRUE, c("cran_task_views", "cran_task_view_events",
                                    "cran_task_view_membership"),
                            output_tables(con)),
    character(0))
})
