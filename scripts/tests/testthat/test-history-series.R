for (f in c("fold.R", "check_flavor_fold.R", "series.R")) {
  source(file.path(getwd(), "..", "..", "history", f))
}

series_named <- function(name) Filter(function(s) identical(s$name, name), history_series())[[1]]

write_snapshot <- function(tables, ddl = list(), env = parent.frame()) {
  write_source_db(withr::local_tempfile(fileext = ".db", .local_envir = env), tables, ddl)
}

# Runs one series over a snapshot file, attached as `snap` for the call.
apply_on <- function(con, path, name, on, prior = NULL, forced = NULL) {
  DBI::dbExecute(con, "ATTACH DATABASE ? AS snap", params = list(path))
  on.exit(DBI::dbExecute(con, "DETACH DATABASE snap"))
  apply_episode_series(con, series_named(name), on, prior, forced)
}

plan_on <- function(con, path, name) {
  DBI::dbExecute(con, "ATTACH DATABASE ? AS snap", params = list(path))
  on.exit(DBI::dbExecute(con, "DETACH DATABASE snap"))
  series_read_plan(con, series_named(name))
}

empty <- function(table) {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  DBI::dbExecute(con, history_source_ddl[[table]])
  DBI::dbGetQuery(con, sprintf("SELECT * FROM %s", table))
}

test_that("every series reads the columns its producer writes", {
  con <- history_test_db()
  path <- write_snapshot(stats::setNames(lapply(names(history_source_ddl), empty), names(history_source_ddl)))
  plan <- function(name) plan_on(con, path, name)
  expect_equal(plan("check_timing")$key, c("package", "flavor_id"))
  expect_equal(plan("check_timing")$values, c("tinstall", "tcheck", "ttotal"))
  expect_equal(plan("check_issue")$key, c("package", "kind", "href"))
  expect_equal(plan("check_deadline")$key, c("package", "deadline_seq"))
  expect_equal(plan("check_deadline")$values, "deadline")
  expect_false(any(c("pipeline", "fetched_at", "last_checked") %in% plan("pipeline_metadata")$values))
  expect_true(all(c("release_tag", "data_through", "db_sha256", "verified") %in%
                    plan("pipeline_metadata")$values))
  expect_equal(plan("autoobs_summary")$values,
               c("origin", "identity_state", "total_1d", "total_7d", "total_30d", "cnt_total"))
  expect_equal(plan("conda_forge_summary")$values, conda_values)
  expect_equal(plan("bioconda_summary")$values, conda_values)
  expect_equal(plan("bioc_downloads_summary")$key, c("package", "category"))
  expect_equal(length(plan("bioc_packages")$values), 21L)
  expect_false("updated_at" %in% plan("bioc_packages")$values)
  expect_equal(plan("bioc_authors")$columns,
               c("comment", "email", "family", "given", "orcid", "role", "ror_id"))
  expect_equal(plan("vcs_repo_attr")$values, c("license", "topics", "is_archived"))
  dev <- plan("vcs_dev_tooling")$values
  expect_true(all(c("ci_github_actions", "has_pkgdown", "has_ci", "ci_lint") %in% dev))
  expect_false(any(c("last_scanned", "ci_workflow_files", "ci_platforms", "is_fork",
                     "discussions_total", "pages_url") %in% dev))
  expect_equal(plan("coverage_summary")$key, c("package", "version"))
  expect_equal(length(plan("coverage_summary")$values), 25L)
})

test_that("a table the release does not carry leaves its series absent", {
  con <- history_test_db()
  path <- write_snapshot(list(pipeline_metadata = empty("pipeline_metadata")))
  got <- apply_on(con, path, "coverage_summary", "2026-07-01")
  expect_equal(got$outcome, "absent")
  expect_false(history_table_exists(con, "coverage_summary_history"))
})

test_that("bioc_packages ignores its run time", {
  con <- history_test_db()
  day <- function(at) data.frame(name = c("Rhtslib", "limma"), name_lower = c("rhtslib", "limma"),
                                 category = "Software", version = c("3.5.0", "3.65.4"),
                                 updated_at = at, stringsAsFactors = FALSE)
  apply_on(con, write_snapshot(list(bioc_packages = day("2026-09-27T13:10:02Z"))),
           "bioc_packages", "2026-09-27")
  got <- apply_on(con, write_snapshot(list(bioc_packages = day("2026-09-28T13:41:29Z"))),
                  "bioc_packages", "2026-09-28", prior = list(rows = 2L))
  expect_equal(c(got$extended, got$closed, got$opened), c(2L, 0L, 0L))
})

test_that("autoobs keeps counters, not ranks, and records when MirrorCache was read", {
  con <- history_test_db()
  day <- function(rank, snap) data.frame(package = "Rcpp", package_lower = "rcpp", origin = "cran",
                                         identity_state = "live", total_1d = 27L, total_7d = 190L,
                                         total_30d = 812L, cnt_total = 0L, rank_30d = rank,
                                         last_snapshot = snap, stringsAsFactors = FALSE)
  apply_on(con, write_snapshot(list(autoobs_downloads_summary = day(3L, "2026-09-27"))),
           "autoobs_summary", "2026-09-27")
  got <- apply_on(con, write_snapshot(list(autoobs_downloads_summary = day(4L, "2026-09-28"))),
                  "autoobs_summary", "2026-09-28", prior = list(rows = 1L))
  expect_equal(c(got$extended, got$opened), c(1L, 0L))
  expect_equal(got$source_as_of, "2026-09-28")
  expect_false("rank_30d" %in% history_table_columns(con, "autoobs_summary_history")$name)
})

test_that("check timings are keyed by flavor id", {
  con <- history_test_db()
  upsert_check_flavors(con, c("r-release-macos-arm64", "r-devel-linux-x86_64-debian-clang"))
  path <- write_snapshot(list(cran_check_results = data.frame(
    package = "cli", flavor = c("r-devel-linux-x86_64-debian-clang", "r-release-macos-arm64"),
    status = "OK", tinstall = c(19.75, 5), tcheck = c(176.63, 51), ttotal = c(196.38, 56),
    stringsAsFactors = FALSE)))
  got <- apply_on(con, path, "check_timing", "2026-09-30", forced = "applied")
  expect_equal(got$opened, 2L)
  rows <- DBI::dbGetQuery(con, "SELECT flavor_id, ttotal FROM cran_check_timing_history ORDER BY flavor_id")
  expect_equal(rows$flavor_id, c(1L, 2L))
  expect_equal(rows$ttotal, c(196.38, 56))
})

test_that("a deadline moved in place ends one episode and opens the next", {
  con <- history_test_db()
  day <- function(deadline) data.frame(package = "permRand", episode_seq = 1L, deadline = deadline,
                                       first_seen = "2026-09-18", last_seen = "2026-09-28",
                                       stringsAsFactors = FALSE)
  apply_on(con, write_snapshot(list(cran_check_deadlines = day("2026-10-10"))),
           "check_deadline", "2026-09-27")
  got <- apply_on(con, write_snapshot(list(cran_check_deadlines = day("2026-10-24"))),
                  "check_deadline", "2026-09-28", prior = list(rows = 1L))
  expect_equal(c(got$closed, got$opened), c(1L, 1L))
  rows <- DBI::dbGetQuery(con, "SELECT deadline_seq, episode_seq, deadline, ended_on
                                  FROM cran_check_deadline_history ORDER BY episode_seq")
  expect_equal(rows$deadline_seq, c(1L, 1L))
  expect_equal(rows$deadline, c("2026-10-10", "2026-10-24"))
  expect_equal(rows$ended_on, c("2026-09-28", NA))
})

old_authors_ddl <- "CREATE TABLE bioc_authors (package TEXT NOT NULL, given TEXT, family TEXT,
  email TEXT, role TEXT, orcid TEXT)"

authors <- function(package, given, comment = NULL) {
  df <- data.frame(package = package, given = given, family = "Smith",
                   email = "smith@example.org", role = "aut", orcid = NA_character_,
                   stringsAsFactors = FALSE)
  if (!is.null(comment)) {
    df$ror_id <- NA_character_
    df$comment <- comment
  }
  df
}

test_that("a package's author rows are one value, repeated rows included", {
  con <- history_test_db()
  path <- write_snapshot(list(bioc_authors = authors(c("a", "a"), c("Ann", "Ann"))),
                         ddl = list(bioc_authors = old_authors_ddl))
  apply_on(con, path, "bioc_authors", "2026-09-18")
  value <- DBI::dbGetQuery(con, "SELECT authors FROM bioc_authors_history")$authors
  expect_equal(length(jsonlite::fromJSON(value, simplifyVector = FALSE)), 2L)
})

test_that("new author columns re-serialize open episodes instead of opening new ones", {
  con <- history_test_db()
  old <- write_snapshot(list(bioc_authors = authors(c("a", "b", "c"), c("Ann", "Bo", "Cy"))),
                        ddl = list(bioc_authors = old_authors_ddl))
  first <- apply_on(con, old, "bioc_authors", "2026-09-18")
  prior <- list(rows = 3L, columns = strsplit(first$columns, ",")[[1]])
  new <- write_snapshot(list(bioc_authors = authors(c("a", "b", "c"), c("Ann", "Bo", "Cyrus"),
                                                    comment = c(NA, "ORCID pending", NA))))
  got <- apply_on(con, new, "bioc_authors", "2026-09-27", prior = prior)
  expect_equal(c(got$filled, got$extended, got$closed, got$opened), c(2L, 2L, 1L, 1L))
  rows <- DBI::dbGetQuery(con, "SELECT package, episode_seq, authors, ended_on
                                  FROM bioc_authors_history ORDER BY package, episode_seq")
  expect_equal(rows$package, c("a", "b", "c", "c"))
  expect_match(rows$authors[2], "ORCID pending", fixed = TRUE)
  expect_equal(rows$ended_on, c(NA, NA, "2026-09-27", NA))
})

test_that("a snapshot under half the rows last applied changes nothing", {
  con <- history_test_db()
  day <- function(n) data.frame(package = sprintf("p%02d", seq_len(n)), origin = "cran",
                                license = "MIT", stringsAsFactors = FALSE)
  apply_on(con, write_snapshot(list(vcs_signals_summary = day(10))), "vcs_repo_attr", "2026-09-27")
  got <- apply_on(con, write_snapshot(list(vcs_signals_summary = day(4))), "vcs_repo_attr",
                  "2026-09-28", prior = list(rows = 10L))
  expect_equal(got$outcome, "unhealthy")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM vcs_repo_attr_history
                                      WHERE ended_on IS NULL AND last_seen = '2026-09-27'")$n, 10L)
})
