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

# The copy narrates each table to the merge log; keep that out of the test run.
quiet_merge_source_db <- function(con, src_path, allow) {
  out <- NULL
  utils::capture.output(out <- merge_source_db(con, src_path, allow))
  out
}

output_tables <- function(con) {
  DBI::dbGetQuery(con, "SELECT name FROM main.sqlite_master WHERE type = 'table'")$name
}

test_that("the package-to-repository links land with their rows, key and index", {
  dir <- withr::local_tempdir()
  src <- write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_source_db(con, src,
                                 tables_to_merge_from("vcs-signals-summary.db", source_tables))

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
