# Edition 3, so expect_warning() lets through any warning it was not written for.
testthat::local_edition(3)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

# The tables vcs-signals adds, as the producer declares them, plus the three it
# keeps for its own next run, which must not be copied.
vcs_added_tables <- c("vcs_dev_tooling_rules", "vcs_ai_search_coverage",
                      "vcs_ai_review_signals", "vcs_ai_outside_prs",
                      "vcs_ai_ruleset_history", "vcs_repo_owner",
                      "vcs_pr_quarterly", "vcs_pr_coverage", "vcs_repo_name_history")
vcs_added_sql <- c(
  "CREATE TABLE IF NOT EXISTS vcs_dev_tooling_rules (col TEXT NOT NULL, source TEXT NOT NULL,
     rule TEXT NOT NULL, ruleset_version TEXT NOT NULL, PRIMARY KEY (col)) WITHOUT ROWID",
  "INSERT INTO vcs_dev_tooling_rules VALUES
     ('has_litedown', 'tree', '_litedown.yml|site/_litedown.yml at root', 'v3 (2026-10-01)')",
  "CREATE TABLE IF NOT EXISTS vcs_ai_search_coverage (
     rule_key TEXT PRIMARY KEY, tool TEXT NOT NULL, channel TEXT NOT NULL,
     rule_rev INTEGER NOT NULL, repos_asked INTEGER NOT NULL, repos_hit INTEGER NOT NULL,
     repos_refused INTEGER NOT NULL, repos_read_whole INTEGER NOT NULL,
     last_asked_on TEXT) WITHOUT ROWID",
  "INSERT INTO vcs_ai_search_coverage VALUES
     ('msg.claude.session', 'claude', 'commit-credit', 1, 40, 3, 0, 12, '2026-10-05')",
  "CREATE TABLE IF NOT EXISTS vcs_ai_review_signals (
     repo_id TEXT NOT NULL, tool TEXT NOT NULL, first_seen_date TEXT,
     first_seen_censored INTEGER NOT NULL DEFAULT 0, evidence_tiers TEXT,
     markers TEXT, assisted_commits INTEGER, assisted_measured_on TEXT,
     last_confirmed_date TEXT,
     PRIMARY KEY (repo_id, tool)) WITHOUT ROWID",
  "INSERT INTO vcs_ai_review_signals VALUES
     ('github.com/o/rtika2', 'coderabbit', '2026-05-01', 0, 'D', '.coderabbit.yaml',
      NULL, NULL, '2026-10-04')",
  "CREATE TABLE IF NOT EXISTS vcs_ai_outside_prs (
     repo_id TEXT NOT NULL, pr_number INTEGER NOT NULL, tool TEXT NOT NULL,
     found_via TEXT NOT NULL, created_at TEXT NOT NULL,
     from_fork INTEGER NOT NULL, author_association TEXT NOT NULL,
     last_confirmed_date TEXT NOT NULL,
     PRIMARY KEY (repo_id, pr_number, tool)) WITHOUT ROWID",
  "INSERT INTO vcs_ai_outside_prs VALUES
     ('github.com/o/rtika2', 12, 'copilot', 'pr-author', '2026-08-01T10:00:00Z', 1,
      'NONE', '2026-10-04')",
  "CREATE TABLE IF NOT EXISTS vcs_ai_ruleset_history (
     ruleset_version TEXT PRIMARY KEY, first_published_on TEXT NOT NULL, change_key TEXT)
     WITHOUT ROWID",
  "INSERT INTO vcs_ai_ruleset_history VALUES ('2026-10-01', '2026-10-06', 'ungated-weekly-read')",
  "CREATE TABLE IF NOT EXISTS vcs_repo_owner (
     repo_id                 TEXT NOT NULL PRIMARY KEY,
     node_id                 TEXT NOT NULL,
     owner_login_current     TEXT NOT NULL,
     owner_type              TEXT NOT NULL CHECK (owner_type IN ('Organization', 'User')),
     owner_node_id           TEXT NOT NULL,
     name_with_owner_current TEXT NOT NULL,
     observed_on             TEXT NOT NULL
   ) WITHOUT ROWID",
  "CREATE INDEX IF NOT EXISTS idx_vro_login      ON vcs_repo_owner(owner_login_current COLLATE NOCASE)",
  "CREATE INDEX IF NOT EXISTS idx_vro_owner_node ON vcs_repo_owner(owner_node_id)",
  "CREATE INDEX IF NOT EXISTS idx_vro_node       ON vcs_repo_owner(node_id)",
  # Two slugs of one repository (the log4r move): both rows land, the viewer counts the node once.
  "INSERT INTO vcs_repo_owner VALUES
     ('github.com/johnmyleswhite/log4r', 'MDEwOlJlcG9zaXRvcnk4NjA1Njc=', 'r-lib', 'Organization',
      'O_rlib', 'r-lib/log4r', '2026-10-04'),
     ('github.com/r-lib/log4r', 'MDEwOlJlcG9zaXRvcnk4NjA1Njc=', 'r-lib', 'Organization',
      'O_rlib', 'r-lib/log4r', '2026-10-04')",
  "CREATE TABLE IF NOT EXISTS vcs_pr_quarterly (
     repo_id TEXT NOT NULL, quarter TEXT NOT NULL, association TEXT NOT NULL,
     author_type TEXT NOT NULL, from_fork INTEGER NOT NULL CHECK (from_fork IN (0,1)),
     fresh INTEGER NOT NULL CHECK (fresh IN (0,1)), prs INTEGER NOT NULL,
     PRIMARY KEY (repo_id, quarter, association, author_type, from_fork, fresh)) WITHOUT ROWID",
  "INSERT INTO vcs_pr_quarterly VALUES
     ('github.com/r-lib/cli', '2026-Q4', 'MEMBER', 'User', 0, 1, 3),
     ('github.com/r-lib/cli', '2026-Q4', 'NONE', 'Bot', 1, 1, 2)",
  "CREATE TABLE IF NOT EXISTS vcs_pr_coverage (
     repo_id TEXT PRIMARY KEY, counted_from TEXT, counted_through TEXT,
     back_complete INTEGER NOT NULL, updated_on TEXT NOT NULL) WITHOUT ROWID",
  "INSERT INTO vcs_pr_coverage VALUES
     ('github.com/r-lib/cli', '2023-01-04T09:12:00Z', '2026-10-03T21:40:00Z', 1, '2026-10-04')",
  # The log4r move of the owner-table fixture above, dated.
  "CREATE TABLE IF NOT EXISTS vcs_repo_name_history (
     node_id TEXT NOT NULL, episode_seq INTEGER NOT NULL, name_with_owner TEXT NOT NULL,
     owner_node_id TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
     first_seen_exact INTEGER NOT NULL, ended_on TEXT,
     PRIMARY KEY (node_id, episode_seq)) WITHOUT ROWID",
  "INSERT INTO vcs_repo_name_history VALUES
     ('MDEwOlJlcG9zaXRvcnk4NjA1Njc=', 1, 'johnmyleswhite/log4r', 'U_jmw',
      '2026-10-01', '2026-10-01', 0, '2026-10-02'),
     ('MDEwOlJlcG9zaXRvcnk4NjA1Njc=', 2, 'r-lib/log4r', 'O_rlib',
      '2026-10-02', '2026-10-04', 1, NULL)",
  "CREATE TABLE IF NOT EXISTS vcs_ai_repo_reads (repo_id TEXT PRIMARY KEY,
     commits_read_on TEXT, prs_count_cursor TEXT) WITHOUT ROWID",
  "INSERT INTO vcs_ai_repo_reads VALUES ('github.com/o/rtika2', '2026-10-04', 'Y3Vyc29yOjUw')",
  "CREATE TABLE IF NOT EXISTS vcs_ai_account_counts (repo_id TEXT NOT NULL, tool TEXT NOT NULL,
     identity_set TEXT NOT NULL, commits INTEGER NOT NULL, measured_on TEXT NOT NULL,
     PRIMARY KEY (repo_id, tool, identity_set)) WITHOUT ROWID",
  "INSERT INTO vcs_ai_account_counts VALUES ('github.com/o/rtika2', 'copilot', 'graphql', 3, '2026-10-04')",
  "CREATE TABLE IF NOT EXISTS vcs_ai_search_log (repo_id TEXT NOT NULL, rule_key TEXT NOT NULL,
     asked_on TEXT NOT NULL, PRIMARY KEY (repo_id, rule_key)) WITHOUT ROWID",
  "INSERT INTO vcs_ai_search_log VALUES ('github.com/o/rtika2', 'msg.claude.session', '2026-10-05')")

# A cut-down vcs-signals-summary.db. The real one carries more columns and
# tables; what matters here is that the allowlist, the verbatim CREATE TABLE
# and the index copy all run against a real attached SQLite file.
write_vcs_summary <- function(path, with_links, with_added = FALSE) {
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
  if (with_added) for (stmt in vcs_added_sql) DBI::dbExecute(con, stmt)
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

# Code-metrics and catalogue DBs with the producers' release-text DDL. Bioconductor
# keeps its per-version DESCRIPTION and release notes history beside the latest-only tables.
write_cran_code_metrics <- function(path) {
  write_db(path, c(
    "CREATE TABLE cran_code_summary (package TEXT NOT NULL, version TEXT NOT NULL,
       PRIMARY KEY (package, version))",
    "INSERT INTO cran_code_summary VALUES ('prova', '1.0.0')",
    'CREATE TABLE IF NOT EXISTS "cran_description_fields" (
       package TEXT NOT NULL, version TEXT NOT NULL, field TEXT NOT NULL,
       value TEXT, value_truncated INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY (package, field))',
    "INSERT INTO cran_description_fields VALUES
       ('prova', '1.0.0', 'Config/testthat/edition', '3', 0)",
    'CREATE TABLE IF NOT EXISTS "cran_release_notes" (
       package TEXT NOT NULL PRIMARY KEY, version TEXT NOT NULL,
       package_version TEXT, news_file TEXT, release_notes_source TEXT,
       release_notes TEXT, release_notes_truncated INTEGER)',
    "INSERT INTO cran_release_notes VALUES
       ('prova', '1.0.0', '1.0.0', 'NEWS.md', 'news_md', '* First release.', 0)"))
}

write_bioc_code_metrics <- function(path, with_text) {
  sql <- c(
    "CREATE TABLE bioc_code_summary (package TEXT NOT NULL, version TEXT NOT NULL,
       PRIMARY KEY (package, version))",
    "INSERT INTO bioc_code_summary VALUES ('limma', '3.23')")
  if (with_text) sql <- c(sql,
    'CREATE TABLE IF NOT EXISTS "bioc_description_fields" (
       package TEXT NOT NULL, version TEXT NOT NULL, field TEXT NOT NULL,
       value TEXT, value_truncated INTEGER NOT NULL DEFAULT 0,
       PRIMARY KEY (package, field))',
    "INSERT INTO bioc_description_fields VALUES
       ('limma', '3.23', 'Date', '2026-04-01', 0),
       ('limma', '3.23', 'LazyData', 'true', 0)",
    'CREATE TABLE IF NOT EXISTS "bioc_release_notes" (
       package TEXT NOT NULL PRIMARY KEY, version TEXT NOT NULL,
       package_version TEXT, news_file TEXT, release_notes_source TEXT,
       release_notes TEXT, release_notes_truncated INTEGER)',
    "INSERT INTO bioc_release_notes VALUES
       ('limma', '3.23', '3.66.0', 'inst/NEWS.Rd', 'news_rd', 'Bug fixes.', 0)",
    'CREATE TABLE IF NOT EXISTS "bioc_description_history" (
       package TEXT NOT NULL, version TEXT NOT NULL, field TEXT NOT NULL, value TEXT,
       PRIMARY KEY (package, version, field)) WITHOUT ROWID',
    "INSERT INTO bioc_description_history VALUES ('limma', '3.22', 'Date', '2025-10-01')",
    'CREATE TABLE IF NOT EXISTS "bioc_release_notes_history" (
       package TEXT NOT NULL, version TEXT NOT NULL, package_version TEXT,
       news_file TEXT, release_notes_source TEXT, release_notes TEXT,
       release_notes_truncated INTEGER,
       PRIMARY KEY (package, version)) WITHOUT ROWID',
    "INSERT INTO bioc_release_notes_history VALUES
       ('limma', '3.22', '3.64.0', 'inst/NEWS.Rd', 'news_rd', 'New features.', 0)",
    'CREATE TABLE IF NOT EXISTS "bioc_release_text_versions" (
       package TEXT NOT NULL, version TEXT NOT NULL, analyzer_version TEXT,
       n_fields INTEGER, has_release_notes INTEGER, read_at TEXT,
       PRIMARY KEY (package, version))',
    "INSERT INTO bioc_release_text_versions VALUES
       ('limma', '3.23', '0.5.0', 20, 1, '2026-10-02')")
  write_db(path, sql)
}

# The build report and VIEWS episode tables as bioconductor-metadata declares
# them, with their open-row indexes. One DESeq2 check episode closed on a
# changed status and the next one open; one deprecated package.
bioc_build_sql <- c(
  "CREATE TABLE bioc_build_reports (
     bioc_version TEXT NOT NULL, repo TEXT NOT NULL, report_at TEXT NOT NULL,
     branch TEXT NOT NULL, snapshot_at TEXT, generated_at TEXT,
     published_at TEXT NOT NULL, status_sha256 TEXT NOT NULL,
     n_packages INTEGER NOT NULL, n_lines INTEGER NOT NULL, n_na INTEGER NOT NULL,
     nodes TEXT NOT NULL, read_at TEXT NOT NULL, outcome TEXT NOT NULL,
     PRIMARY KEY (bioc_version, repo, report_at))",
  "INSERT INTO bioc_build_reports VALUES
     ('3.23', 'bioc', '2026-09-30T13:05:00Z', 'release', '2026-09-29T17:00:00Z',
      '2026-09-30T13:05:00Z', '2026-09-30T13:07:12Z', 'ab12', 2361, 11805, 520,
      'nebbiolo2,palomino8,kjohnson3', '2026-10-01T06:02:00Z', 'applied')",
  "CREATE TABLE bioc_build_status_history (
     package TEXT NOT NULL, bioc_version TEXT NOT NULL, repo TEXT NOT NULL,
     node TEXT NOT NULL, stage TEXT NOT NULL, episode_seq INTEGER NOT NULL,
     status TEXT NOT NULL, detail TEXT, first_version TEXT, last_version TEXT,
     first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
     first_seen_exact INTEGER NOT NULL, ended_on TEXT, end_reason TEXT,
     PRIMARY KEY (package, bioc_version, repo, node, stage, episode_seq),
     CHECK ((ended_on IS NULL) = (end_reason IS NULL)),
     CHECK (last_seen >= first_seen))",
  "CREATE UNIQUE INDEX ux_bioc_build_open
     ON bioc_build_status_history(package, bioc_version, repo, node, stage) WHERE ended_on IS NULL",
  "CREATE INDEX idx_bioc_build_open_status
     ON bioc_build_status_history(status) WHERE ended_on IS NULL",
  "INSERT INTO bioc_build_status_history VALUES
     ('DESeq2', '3.23', 'bioc', 'nebbiolo2', 'checksrc', 1, 'OK', NULL, '1.52.0', '1.52.1',
      '2026-09-29T13:05:00Z', '2026-09-29T13:05:00Z', 0, '2026-09-30T13:05:00Z', 'changed'),
     ('DESeq2', '3.23', 'bioc', 'nebbiolo2', 'checksrc', 2, 'WARNINGS', NULL, '1.52.1', '1.52.1',
      '2026-09-30T13:05:00Z', '2026-09-30T13:05:00Z', 1, NULL, NULL)",
  "CREATE TABLE bioc_views_history (
     package TEXT NOT NULL, field TEXT NOT NULL, episode_seq INTEGER NOT NULL,
     value TEXT NOT NULL, bioc_version TEXT NOT NULL, category TEXT NOT NULL,
     first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
     first_seen_exact INTEGER NOT NULL, ended_on TEXT,
     PRIMARY KEY (package, field, episode_seq),
     CHECK (last_seen >= first_seen))",
  "CREATE UNIQUE INDEX ux_bioc_views_open
     ON bioc_views_history(package, field) WHERE ended_on IS NULL",
  "INSERT INTO bioc_views_history VALUES
     ('airway', 'PackageStatus', 1, 'Deprecated', '3.23', 'experiment',
      '2026-09-30T11:20:00Z', '2026-09-30T11:20:00Z', 0, NULL)")

write_bioc_catalogue <- function(path, with_vignettes, with_builds = FALSE) {
  sql <- c(
    "CREATE TABLE bioc_packages (package TEXT PRIMARY KEY, category TEXT)",
    "INSERT INTO bioc_packages VALUES ('DESeq2', 'software'), ('airway', 'experiment')")
  if (with_builds) sql <- c(sql, bioc_build_sql)
  if (with_vignettes) sql <- c(sql,
    "CREATE TABLE bioc_vignettes (
       package TEXT NOT NULL, release TEXT NOT NULL, category TEXT NOT NULL,
       version TEXT, seq INTEGER NOT NULL, file TEXT NOT NULL, title TEXT,
       output TEXT, url TEXT NOT NULL, PRIMARY KEY (package, seq))",
    "INSERT INTO bioc_vignettes VALUES
       ('DESeq2', '3.23', 'software', NULL, 1, 'vignettes/DESeq2/inst/doc/DESeq2.html',
        NULL, 'html', 'https://bioconductor.org/packages/3.23/bioc/vignettes/DESeq2/inst/doc/DESeq2.html'),
       ('airway', '3.23', 'experiment', NULL, 1, 'vignettes/airway/inst/doc/airway.html',
        NULL, 'html', 'https://bioconductor.org/packages/3.23/data/experiment/vignettes/airway/inst/doc/airway.html')")
  write_db(path, sql)
}

# The copy narrates each table to the merge log; keep that out of the test run.
quiet_merge_source_db <- function(con, src_path, allow) {
  out <- NULL
  utils::capture.output(out <- merge_source_db(con, src_path, allow))
  out
}

# merge_sources walks the whole source list and a test directory holds only the
# sources the test is about, so the rest are reported missing. Those warnings
# are expected here; any other warning still reaches the test. The log rides
# along as the "log" attribute.
quiet_merge_sources <- function(con, sources_dir) {
  out <- NULL
  log <- withCallingHandlers(
    utils::capture.output(out <- merge_sources(con, sources_dir)),
    warning = function(w) {
      if (startsWith(conditionMessage(w), "Source DB not found")) {
        invokeRestart("muffleWarning")
      }
    })
  attr(out, "log") <- log
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
    res <- quiet_merge_source_db(con, src,
                                 tables_to_merge_from("vcs-signals-summary.db", source_tables)))

  expect_equal(res$tables$vcs_signals_summary, 2)
  expect_false("repo_package_links" %in% output_tables(con))
  expect_null(res$tables$repo_package_links)
  expect_length(res$failed, 0)
  expect_false(any(vcs_added_tables %in% output_tables(con)))
  # Detached again, so the next source in the loop can attach as src.
  expect_false("src" %in% DBI::dbGetQuery(con, "PRAGMA database_list")$name)
})

test_that("a table that fails to copy costs only that table and lets go of src", {
  dir <- withr::local_tempdir()
  src <- write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))
  # An earlier source already made a table by this name with other columns, so
  # the insert fails after vcs_signals_summary has been copied.
  DBI::dbExecute(con, "CREATE TABLE repo_package_links (repo_id TEXT)")

  expect_no_error(
    res <- quiet_merge_source_db(con, src,
                                 tables_to_merge_from("vcs-signals-summary.db", source_tables)))

  expect_named(res$failed, "repo_package_links")
  expect_match(res$failed[["repo_package_links"]], "no column named package")
  expect_false("src" %in% DBI::dbGetQuery(con, "PRAGMA database_list")$name)
  # The tables that did copy stay.
  expect_equal(res$tables$vcs_signals_summary, 2)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM vcs_signals_summary")$n, 2)
  expect_null(res$tables$repo_package_links)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM repo_package_links")$n, 0)
  # Its index is not put on the other source's table.
  expect_false("idx_rpl_package" %in%
                 DBI::dbGetQuery(con, "PRAGMA main.index_list(repo_package_links)")$name)
})

test_that("a table whose rows fail partway leaves nothing behind, not even the table", {
  dir <- withr::local_tempdir()
  src <- write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE,
                           with_added = TRUE)
  # A row the owner-type check refuses, written with the check switched off, so
  # the copy creates vcs_repo_owner and then fails on its second row.
  write_db(src, c("PRAGMA ignore_check_constraints = 1",
                  "INSERT INTO vcs_repo_owner VALUES
                     ('github.com/x/y', 'R_x', 'x', 'Bot', 'O_x', 'x/y', '2026-10-04')"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  res <- quiet_merge_source_db(con, src,
                               tables_to_merge_from("vcs-signals-summary.db", source_tables))

  expect_named(res$failed, "vcs_repo_owner")
  expect_match(res$failed[["vcs_repo_owner"]], "CHECK constraint failed")
  expect_false("vcs_repo_owner" %in% output_tables(con))
  expect_true(all(c("vcs_signals_summary", "repo_package_links", setdiff(vcs_added_tables, "vcs_repo_owner"))
                  %in% output_tables(con)))
  # Its three indexes are accounted for as belonging to a table that failed.
  vro <- res$indexes[res$indexes$table == "vcs_repo_owner", , drop = FALSE]
  expect_setequal(vro$index, c("idx_vro_login", "idx_vro_owner_node", "idx_vro_node"))
  expect_true(all(vro$outcome == "skipped"))
  expect_true(all(grepl("failed", vro$note)))
})

test_that("a source refused a table is still an error, and the sources after it merge", {
  dir <- withr::local_tempdir()
  # queue.db merges every table it has and comes before vcs-signals, so it
  # brings repo_package_links first and the vcs table of that name is refused.
  write_db(file.path(dir, "queue.db"), "CREATE TABLE repo_package_links (repo_id TEXT)")
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE,
                    with_added = TRUE)
  write_task_views(file.path(dir, "cran-task-views.db"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  expect_warning(stats <- quiet_merge_sources(con, dir),
                 "Error processing vcs-signals-summary.db.*repo_package_links.*queue.db")

  status <- vapply(stats, function(s) s$status, character(1))
  expect_equal(status[c("queue.db", "vcs-signals-summary.db", "cran-task-views.db")],
               c("queue.db" = "merged", "vcs-signals-summary.db" = "error",
                 "cran-task-views.db" = "merged"))
  vcs <- stats[["vcs-signals-summary.db"]]
  expect_named(vcs$failed_tables, "repo_package_links")
  expect_equal(vcs$failed_tables[["repo_package_links"]],
               "not copied, queue.db already brought a table named repo_package_links")
  # queue.db's table is left as it came, with none of the vcs rows in it.
  expect_equal(DBI::dbGetQuery(con, "PRAGMA main.table_info(repo_package_links)")$name, "repo_id")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM repo_package_links")$n, 0)
  # Every other allowlisted vcs table still lands.
  expect_setequal(names(vcs$tables), c("vcs_signals_summary", vcs_added_tables))
  expect_true(all(c("vcs_signals_summary", vcs_added_tables) %in% output_tables(con)))
  # merge.R refuses the release when these are missing.
  expect_equal(
    missing_expected_tables(TRUE, c("cran_task_views", "cran_task_view_events",
                                    "cran_task_view_membership"),
                            output_tables(con)),
    character(0))
})

test_that("the sources that did not merge whole are named with what failed", {
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "queue.db"), "CREATE TABLE repo_package_links (repo_id TEXT)")
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  # Not a database at all, so nothing of it merges.
  writeLines("not a database", file.path(dir, "cran-archive.db"))
  write_task_views(file.path(dir, "cran-task-views.db"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))
  stats <- suppressWarnings(quiet_merge_sources(con, dir))

  failures <- merge_failures(stats)

  expect_named(failures, c("cran-archive.db", "vcs-signals-summary.db"), ignore.order = TRUE)
  expect_equal(failures[["vcs-signals-summary.db"]],
               "table repo_package_links: not copied, queue.db already brought a table named repo_package_links")
  expect_match(failures[["cran-archive.db"]], "nothing merged")
  expect_false(any(grepl("[\t\n]", failures)))
})

test_that("the merge failures survive the trip to the gate through a file", {
  dir <- withr::local_tempdir()
  path <- file.path(dir, ".merge-failed")
  failures <- c("vcs-signals-summary.db" = "table repo_package_links: no column named package",
                "cran-archive.db" = "nothing merged: file is not a database")

  write_merge_failures(path, failures)
  expect_equal(read_merge_failures(path), failures)

  # Nothing failed: no file, and a stale one from an earlier run is removed.
  write_merge_failures(path, character())
  expect_false(file.exists(path))
  expect_equal(read_merge_failures(path), character())
})

# The index DDL both code-metrics producers declare. They reuse the same index
# names on their own tables, and index names are one namespace per database.
write_indexed_code_metrics <- function(path, prefix, pad = " ") {
  write_db(path, c(
    sprintf("CREATE TABLE %s_code_summary (package TEXT, version TEXT, loc_r INTEGER)", prefix),
    sprintf("INSERT INTO %s_code_summary VALUES ('p', '1.0', 10), ('p', '1.1', 12)", prefix),
    sprintf("CREATE UNIQUE INDEX idx_summary_pkg_ver%sON %s_code_summary(package, version)",
            pad, prefix),
    sprintf("CREATE TABLE %s_api_history (package TEXT, version TEXT, n_exports INTEGER)", prefix),
    sprintf("INSERT INTO %s_api_history VALUES ('p', '1.0', 3), ('p', '1.1', 4)", prefix),
    sprintf("CREATE INDEX idx_api_pkg_ver ON %s_api_history(package, version)", prefix),
    sprintf("CREATE TABLE %s_code_churn (package TEXT, version TEXT, file TEXT,
               added INTEGER, deleted INTEGER)", prefix),
    sprintf("INSERT INTO %s_code_churn VALUES ('p', '1.1', 'R/a.R', 5, 1)", prefix),
    sprintf("CREATE INDEX idx_churn_pkg_ver ON %s_code_churn(package, version)", prefix),
    sprintf("CREATE INDEX idx_churn_pkg ON %s_code_churn(package)", prefix)))
}

indexes_on <- function(con, tbl) {
  DBI::dbGetQuery(con, "SELECT name, sql FROM main.sqlite_master
                        WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL
                        ORDER BY name", params = list(tbl))
}

index_columns <- function(con, idx) {
  DBI::dbGetQuery(con, sprintf('PRAGMA main.index_info("%s")', idx))$name
}

merge_code_metrics_pair <- function(dir) {
  write_indexed_code_metrics(file.path(dir, "cran-code-metrics.db"), "cran")
  # The Bioconductor summary index is declared with a run of spaces before ON.
  write_indexed_code_metrics(file.path(dir, "bioc-code-metrics.db"), "bioc", pad = "     ")
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  list(con = con, stats = quiet_merge_sources(con, dir))
}

test_that("an index name the CRAN tables already hold lands on the Bioconductor table under its table's name", {
  dir <- withr::local_tempdir()
  m <- merge_code_metrics_pair(dir)
  con <- m$con
  withr::defer(DBI::dbDisconnect(con))

  expect_equal(indexes_on(con, "cran_api_history")$name, "idx_api_pkg_ver")
  expect_equal(indexes_on(con, "cran_code_churn")$name, c("idx_churn_pkg", "idx_churn_pkg_ver"))
  expect_equal(indexes_on(con, "cran_code_summary")$name, "idx_summary_pkg_ver")

  expect_equal(indexes_on(con, "bioc_api_history")$name, "bioc_api_history__idx_api_pkg_ver")
  expect_equal(indexes_on(con, "bioc_code_churn")$name,
               c("bioc_code_churn__idx_churn_pkg", "bioc_code_churn__idx_churn_pkg_ver"))
  expect_equal(indexes_on(con, "bioc_code_summary")$name, "bioc_code_summary__idx_summary_pkg_ver")
  expect_equal(index_columns(con, "bioc_api_history__idx_api_pkg_ver"), c("package", "version"))
  expect_equal(index_columns(con, "bioc_code_churn__idx_churn_pkg"), "package")

  expect_equal(m$stats[["bioc-code-metrics.db"]]$status, "merged")
})

test_that("the renamed summary index is still unique", {
  dir <- withr::local_tempdir()
  m <- merge_code_metrics_pair(dir)
  con <- m$con
  withr::defer(DBI::dbDisconnect(con))

  il <- DBI::dbGetQuery(con, "PRAGMA main.index_list(bioc_code_summary)")
  expect_equal(il$unique[il$name == "bioc_code_summary__idx_summary_pkg_ver"], 1L)
  expect_error(DBI::dbExecute(con, "INSERT INTO bioc_code_summary VALUES ('p', '1.0', 99)"),
               "UNIQUE")
})

test_that("every renamed index is written to the merge log and kept in the stats", {
  dir <- withr::local_tempdir()
  write_indexed_code_metrics(file.path(dir, "cran-code-metrics.db"), "cran")
  write_indexed_code_metrics(file.path(dir, "bioc-code-metrics.db"), "bioc")
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)
  log <- attr(stats, "log")

  expect_true(any(grepl("idx_api_pkg_ver.*cran_api_history.*bioc_api_history__idx_api_pkg_ver", log)))
  idx <- stats[["bioc-code-metrics.db"]]$indexes
  renamed <- idx[idx$outcome == "renamed", , drop = FALSE]
  expect_setequal(renamed$created_as,
                  c("bioc_api_history__idx_api_pkg_ver", "bioc_code_churn__idx_churn_pkg",
                    "bioc_code_churn__idx_churn_pkg_ver", "bioc_code_summary__idx_summary_pkg_ver"))
  expect_true(all(stats[["cran-code-metrics.db"]]$indexes$outcome == "created"))
})

test_that("an index name is matched without regard to case", {
  dir <- withr::local_tempdir()
  write_indexed_code_metrics(file.path(dir, "cran-code-metrics.db"), "cran")
  write_db(file.path(dir, "bioc-code-metrics.db"), c(
    "CREATE TABLE bioc_api_history (package TEXT, version TEXT)",
    "INSERT INTO bioc_api_history VALUES ('p', '1.0')",
    "CREATE INDEX IDX_API_PKG_VER ON bioc_api_history(package, version)"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  quiet_merge_sources(con, dir)

  expect_equal(indexes_on(con, "bioc_api_history")$name, "bioc_api_history__IDX_API_PKG_VER")
})

test_that("a qualified name that is itself taken fails the source rather than going quiet", {
  dir <- withr::local_tempdir()
  # queue.db merges every table it has, and this one takes the qualified name.
  write_db(file.path(dir, "queue.db"), "CREATE TABLE bioc_api_history__idx_api_pkg_ver (x TEXT)")
  write_indexed_code_metrics(file.path(dir, "cran-code-metrics.db"), "cran")
  write_indexed_code_metrics(file.path(dir, "bioc-code-metrics.db"), "bioc")
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  expect_warning(stats <- quiet_merge_sources(con, dir),
                 "Error processing bioc-code-metrics.db.*idx_api_pkg_ver")

  bioc <- stats[["bioc-code-metrics.db"]]
  expect_equal(bioc$status, "error")
  failed <- bioc$indexes[bioc$indexes$outcome == "failed", , drop = FALSE]
  expect_equal(failed$index, "idx_api_pkg_ver")
  expect_match(merge_failures(stats)[["bioc-code-metrics.db"]], "idx_api_pkg_ver")
  # The data itself landed, and the other renamed indexes still went on.
  expect_equal(bioc$tables$bioc_api_history, 2)
  expect_true("bioc_code_churn__idx_churn_pkg_ver" %in% indexes_on(con, "bioc_code_churn")$name)
})

test_that("an index on a table the allowlist leaves out is skipped and says why", {
  dir <- withr::local_tempdir()
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  vcs <- quiet_merge_sources(con, dir)[["vcs-signals-summary.db"]]

  expect_equal(vcs$status, "merged")
  rp <- vcs$indexes[vcs$indexes$index == "idx_rp_package", , drop = FALSE]
  expect_equal(rp$outcome, "skipped")
  expect_match(rp$note, "not copied")
  expect_length(merge_failures(list("vcs-signals-summary.db" = vcs)), 0)
})

test_that("a renamed index keeps its UNIQUE, collation and WHERE clause", {
  expect_equal(index_sql_as('CREATE INDEX "Idx One"   ON a(x)', "a__Idx One"),
               'CREATE INDEX "a__Idx One" ON a(x)')
  expect_equal(index_sql_as("CREATE UNIQUE INDEX [idx2] ON a(y) WHERE y > 0", "a__idx2"),
               'CREATE UNIQUE INDEX "a__idx2" ON a(y) WHERE y > 0')
  expect_equal(index_sql_as("CREATE INDEX idx_vro_login ON vcs_repo_owner(owner_login_current COLLATE NOCASE)",
                            "vcs_repo_owner__idx_vro_login"),
               'CREATE INDEX "vcs_repo_owner__idx_vro_login" ON vcs_repo_owner(owner_login_current COLLATE NOCASE)')
  expect_equal(index_sql_as("CREATE UNIQUE INDEX idx_summary_pkg_ver     ON bioc_code_summary(package, version)",
                            "bioc_code_summary__idx_summary_pkg_ver"),
               'CREATE UNIQUE INDEX "bioc_code_summary__idx_summary_pkg_ver" ON bioc_code_summary(package, version)')
  expect_true(is.na(index_sql_as("CREATE TABLE t (x)", "t__x")))
})

test_that("a table-qualified index name fits the 64 characters MySQL allows", {
  expect_equal(qualified_index_name("bioc_api_history", "idx_api_pkg_ver"),
               "bioc_api_history__idx_api_pkg_ver")
  long <- qualified_index_name(strrep("t", 40), strrep("i", 40))
  expect_equal(nchar(long), 64L)
  expect_true(startsWith(long, paste0(strrep("t", 40), "__")))
})

test_that("a source that did not merge whole says so in the release notes", {
  stats <- list(
    status = "error", file_size = 2048,
    tables = list(vcs_signals_summary = 2, vcs_repo_owner = 2),
    failed_tables = c(repo_package_links = "table repo_package_links has no column named package"),
    indexes = data.frame(index = character(), table = character(), created_as = character(),
                         outcome = character(), note = character()))

  row <- source_note_row("vcs-signals-summary.db", stats)

  expect_match(row, "| vcs-signals-summary.db | error, failed: repo_package_links |", fixed = TRUE)
  expect_match(row, "vcs_signals_summary, vcs_repo_owner (2)", fixed = TRUE)
  expect_match(row, "| 4 |", fixed = TRUE)
  expect_equal(source_note_row("x.db", list(status = "error", reason = "boom")),
               "| x.db | error | - | - | - |")
})

test_that("the latest DESCRIPTION fields, release notes and Bioconductor vignettes land", {
  dir <- withr::local_tempdir()
  write_cran_code_metrics(file.path(dir, "cran-code-metrics.db"))
  write_bioc_code_metrics(file.path(dir, "bioc-code-metrics.db"), with_text = TRUE)
  write_bioc_catalogue(file.path(dir, "bioconductor-metadata.db"), with_vignettes = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)

  expect_equal(stats[["cran-code-metrics.db"]]$tables$cran_description_fields, 1)
  expect_equal(stats[["cran-code-metrics.db"]]$tables$cran_release_notes, 1)
  expect_equal(stats[["bioc-code-metrics.db"]]$tables$bioc_description_fields, 2)
  expect_equal(stats[["bioc-code-metrics.db"]]$tables$bioc_release_notes, 1)
  expect_equal(stats[["bioconductor-metadata.db"]]$tables$bioc_vignettes, 2)

  pk <- function(tbl) {
    info <- DBI::dbGetQuery(con, sprintf("PRAGMA main.table_info(%s)", tbl))
    key <- info[info$pk > 0, , drop = FALSE]
    key$name[order(key$pk)]
  }
  expect_equal(pk("cran_description_fields"), c("package", "field"))
  expect_equal(pk("bioc_release_notes"), "package")
  expect_equal(pk("bioc_vignettes"), c("package", "seq"))
  got <- DBI::dbGetQuery(con, "SELECT url FROM bioc_vignettes WHERE package = 'airway'")$url
  expect_equal(got, "https://bioconductor.org/packages/3.23/data/experiment/vignettes/airway/inst/doc/airway.html")
})

test_that("the per-version DESCRIPTION and release notes history stays out of observatory.db", {
  dir <- withr::local_tempdir()
  write_bioc_code_metrics(file.path(dir, "bioc-code-metrics.db"), with_text = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["bioc-code-metrics.db"]]$tables

  expect_setequal(names(stats), c("bioc_code_summary", "bioc_description_fields", "bioc_release_notes"))
  expect_false(any(c("bioc_description_history", "bioc_release_notes_history",
                     "bioc_release_text_versions") %in% output_tables(con)))
})

test_that("code-metrics and catalogue DBs published before these tables still merge", {
  dir <- withr::local_tempdir()
  write_bioc_code_metrics(file.path(dir, "bioc-code-metrics.db"), with_text = FALSE)
  write_bioc_catalogue(file.path(dir, "bioconductor-metadata.db"), with_vignettes = FALSE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)

  expect_equal(stats[["bioc-code-metrics.db"]]$status, "merged")
  expect_equal(stats[["bioconductor-metadata.db"]]$status, "merged")
  expect_equal(stats[["bioc-code-metrics.db"]]$tables$bioc_code_summary, 1)
  expect_equal(stats[["bioconductor-metadata.db"]]$tables$bioc_packages, 2)
  expect_false(any(c("bioc_description_fields", "bioc_release_notes", "bioc_vignettes")
                   %in% output_tables(con)))
})

test_that("a summary column the pipeline adds or retires reaches observatory.db as the source now declares it", {
  # The pipelines ALTER ADD new summary columns and DROP COLUMN retired ones; the
  # merge copies the CREATE TABLE text SQLite rewrote, so the merger needs no change.
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "cran-code-metrics.db"), c(
    "CREATE TABLE cran_code_summary (package TEXT NOT NULL, version TEXT NOT NULL,
       has_website INTEGER, copyright_holder_declared INTEGER,
       PRIMARY KEY (package, version))",
    "INSERT INTO cran_code_summary VALUES ('prova', '1.0.0', 1, 0)",
    "ALTER TABLE cran_code_summary ADD COLUMN input_kind TEXT",
    "UPDATE cran_code_summary SET input_kind = 'release'",
    "ALTER TABLE cran_code_summary DROP COLUMN has_website",
    "ALTER TABLE cran_code_summary DROP COLUMN copyright_holder_declared"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["cran-code-metrics.db"]]

  expect_equal(stats$status, "merged")
  expect_equal(DBI::dbGetQuery(con, "PRAGMA main.table_info(cran_code_summary)")$name,
               c("package", "version", "input_kind"))
  expect_equal(DBI::dbGetQuery(con, "SELECT input_kind FROM cran_code_summary")$input_kind,
               "release")
})

test_that("the added vcs-signals tables land with their keys, and the read state stays out", {
  dir <- withr::local_tempdir()
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE, with_added = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["vcs-signals-summary.db"]]$tables

  expect_equal(unlist(stats[vcs_added_tables]),
               c(vcs_dev_tooling_rules = 1, vcs_ai_search_coverage = 1,
                 vcs_ai_review_signals = 1, vcs_ai_outside_prs = 1,
                 vcs_ai_ruleset_history = 1, vcs_repo_owner = 2,
                 vcs_pr_quarterly = 2, vcs_pr_coverage = 1, vcs_repo_name_history = 2))
  ddl <- DBI::dbGetQuery(con, sprintf(
    "SELECT name, sql FROM main.sqlite_master WHERE type = 'table' AND name IN (%s)",
    paste0("'", vcs_added_tables, "'", collapse = ", ")))
  expect_setequal(ddl$name, vcs_added_tables)
  expect_true(all(grepl("WITHOUT ROWID", ddl$sql, fixed = TRUE)))
  expect_error(DBI::dbExecute(con, "INSERT INTO vcs_ai_outside_prs VALUES
    ('github.com/o/rtika2', 12, 'copilot', 'pr-author', '2026-08-02T10:00:00Z', 1,
     'NONE', '2026-10-05')"), "UNIQUE")
  expect_false(any(c("vcs_ai_repo_reads", "vcs_ai_account_counts", "vcs_ai_search_log")
                   %in% output_tables(con)))
})

test_that("the repository owner table keeps its owner type check and its three indexes", {
  dir <- withr::local_tempdir()
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE, with_added = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  quiet_merge_sources(con, dir)

  expect_error(DBI::dbExecute(con, "INSERT INTO vcs_repo_owner VALUES
    ('github.com/x/y', 'R_x', 'x', 'Bot', 'O_x', 'x/y', '2026-10-04')"), "CHECK constraint failed")
  idx <- DBI::dbGetQuery(con, "SELECT name, sql FROM main.sqlite_master
                               WHERE type = 'index' AND tbl_name = 'vcs_repo_owner'
                                 AND sql IS NOT NULL")
  expect_setequal(idx$name, c("idx_vro_login", "idx_vro_owner_node", "idx_vro_node"))
  expect_match(idx$sql[idx$name == "idx_vro_login"], "COLLATE NOCASE", fixed = TRUE)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(DISTINCT node_id) AS n FROM vcs_repo_owner")$n, 1)
})

test_that("columns vcs-signals adds with ALTER TABLE arrive with their tables", {
  # ensure_series_schema adds new columns with ALTER TABLE ADD COLUMN; SQLite rewrites
  # the stored CREATE TABLE, which is what the merge copies.
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "vcs-signals-summary.db"), c(
    "CREATE TABLE vcs_dev_tooling (repo_id TEXT NOT NULL, last_scanned TEXT,
       has_pr_template INTEGER, PRIMARY KEY (repo_id)) WITHOUT ROWID",
    "ALTER TABLE vcs_dev_tooling ADD COLUMN ruleset_version TEXT",
    "ALTER TABLE vcs_dev_tooling ADD COLUMN pr_template_source TEXT",
    "INSERT INTO vcs_dev_tooling VALUES
       ('github.com/r-lib/log4r', '2026-10-04', 1, 'v3 (2026-10-01)', 'account_default')",
    "CREATE TABLE vcs_ai_signals (repo_id TEXT NOT NULL, tool TEXT NOT NULL,
       authored_commits INTEGER, assisted_commits INTEGER, PRIMARY KEY (repo_id, tool))",
    "ALTER TABLE vcs_ai_signals ADD COLUMN authored_measured_on TEXT",
    "ALTER TABLE vcs_ai_signals ADD COLUMN assisted_measured_on TEXT",
    "INSERT INTO vcs_ai_signals VALUES
       ('github.com/o/rtika2', 'claude', 4, 2, '2026-10-04', '2026-10-05')"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  quiet_merge_sources(con, dir)

  got <- DBI::dbGetQuery(con, "SELECT ruleset_version, pr_template_source FROM vcs_dev_tooling")
  expect_equal(got$ruleset_version, "v3 (2026-10-01)")
  expect_equal(got$pr_template_source, "account_default")
  expect_match(DBI::dbGetQuery(con, "SELECT sql FROM main.sqlite_master
                                     WHERE name = 'vcs_dev_tooling'")$sql,
               "WITHOUT ROWID", fixed = TRUE)
  got <- DBI::dbGetQuery(con, "SELECT authored_measured_on, assisted_measured_on FROM vcs_ai_signals")
  expect_equal(unlist(got), c(authored_measured_on = "2026-10-04", assisted_measured_on = "2026-10-05"))
})

test_that("the autoobs run record lands and the counters and day ledger stay behind", {
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "autoobs-downloads-summary.db"), c(
    "CREATE TABLE autoobs_downloads_summary (package TEXT PRIMARY KEY, total_30d INTEGER)",
    "INSERT INTO autoobs_downloads_summary VALUES ('Rcpp', 812)",
    "CREATE TABLE autoobs_runs (run_id INTEGER PRIMARY KEY, run_at TEXT,
       snapshot_date TEXT NOT NULL, source TEXT NOT NULL, outcome TEXT NOT NULL,
       reason TEXT, day_aggregated INTEGER, window_end TEXT,
       counters_prior TEXT, counters_published INTEGER)",
    "INSERT INTO autoobs_runs VALUES
       (1790741529, '2026-09-30T04:12:09Z', '2026-09-30', 'run', 'ok', NULL, 1,
        '2026-09-29', 'loaded', 1),
       (1790827841, '2026-10-01T04:10:41Z', '2026-10-01', 'run', 'heartbeat',
        'no stats', 0, '2026-09-29', 'download_failed', 0)",
    "CREATE TABLE autoobs_counters (run_id INTEGER NOT NULL, package TEXT NOT NULL,
       cnt_today INTEGER, cnt_1d INTEGER, cnt_7d INTEGER, cnt_30d INTEGER,
       cnt_total INTEGER, PRIMARY KEY (run_id, package)) WITHOUT ROWID",
    "INSERT INTO autoobs_counters VALUES (1790741529, 'Rcpp', 3, 27, 190, 812, 0)",
    "CREATE TABLE autoobs_days (date TEXT PRIMARY KEY, method TEXT NOT NULL,
       run_id INTEGER, packages INTEGER, downloads INTEGER)",
    "INSERT INTO autoobs_days VALUES ('2026-09-29', 'cnt_1d', 1790741529, 5210, 40112)"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["autoobs-downloads-summary.db"]]

  expect_equal(stats$status, "merged")
  expect_equal(stats$tables$autoobs_runs, 2)
  got <- DBI::dbGetQuery(con, "SELECT outcome, counters_prior FROM autoobs_runs ORDER BY run_id")
  expect_equal(got$outcome, c("ok", "heartbeat"))
  expect_equal(got$counters_prior, c("loaded", "download_failed"))
  expect_false(any(c("autoobs_counters", "autoobs_days") %in% output_tables(con)))
})

test_that("the Bioconductor build and VIEWS episodes land with their open-row indexes", {
  dir <- withr::local_tempdir()
  write_bioc_catalogue(file.path(dir, "bioconductor-metadata.db"), with_vignettes = TRUE,
                       with_builds = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["bioconductor-metadata.db"]]

  expect_equal(stats$status, "merged")
  expect_equal(unlist(stats$tables[c("bioc_build_reports", "bioc_build_status_history",
                                     "bioc_views_history")]),
               c(bioc_build_reports = 1, bioc_build_status_history = 2,
                 bioc_views_history = 1))
  idx <- rbind(indexes_on(con, "bioc_build_status_history"), indexes_on(con, "bioc_views_history"))
  expect_setequal(idx$name, c("ux_bioc_build_open", "idx_bioc_build_open_status",
                              "ux_bioc_views_open"))
  expect_true(all(grepl("WHERE ended_on IS NULL", idx$sql, fixed = TRUE)))
  # One open episode per package, node and stage.
  expect_error(DBI::dbExecute(con, "INSERT INTO bioc_build_status_history VALUES
    ('DESeq2', '3.23', 'bioc', 'nebbiolo2', 'checksrc', 3, 'ERROR', NULL, '1.52.1', '1.52.1',
     '2026-10-01T13:05:00Z', '2026-10-01T13:05:00Z', 1, NULL, NULL)"), "UNIQUE")
  expect_error(DBI::dbExecute(con, "INSERT INTO bioc_build_status_history VALUES
    ('limma', '3.23', 'bioc', 'nebbiolo2', 'checksrc', 1, 'OK', NULL, NULL, NULL,
     '2026-10-01', '2026-09-01', 1, NULL, NULL)"), "CHECK constraint failed")
  expect_equal(DBI::dbGetQuery(con, "SELECT value FROM bioc_views_history
                                     WHERE package = 'airway' AND ended_on IS NULL")$value,
               "Deprecated")
})

test_that("the CRAN tarball table lands keyed by revision, WITHOUT ROWID and with its check", {
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "cran-archive.db"), c(
    "CREATE TABLE cran_archive (package TEXT PRIMARY KEY, archived_on TEXT)",
    "INSERT INTO cran_archive VALUES ('behaviorchange', '2026-08-28')",
    "CREATE TABLE cran_tarballs (
       package TEXT NOT NULL, version TEXT NOT NULL, revision INTEGER NOT NULL,
       size_bytes INTEGER NOT NULL, mtime TEXT NOT NULL, md5sum TEXT,
       listing TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
       PRIMARY KEY (package, version, revision),
       CHECK (last_seen >= first_seen)) WITHOUT ROWID",
    # A version whose file was replaced keeps one row per file.
    "INSERT INTO cran_tarballs VALUES
       ('lmeInfo', '0.3.2', 1, 65124, '2023-03-07T09:10:11Z', NULL, 'gone',
        '2026-10-01', '2026-10-01'),
       ('lmeInfo', '0.3.2', 2, 65532, '2026-09-27T08:01:02Z',
        '0f5c1c1a8e3c4b8a9d7e6f5a4b3c2d1e', 'current', '2026-10-01', '2026-10-01')"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["cran-archive.db"]]

  expect_equal(stats$status, "merged")
  expect_equal(stats$tables$cran_tarballs, 2)
  sql <- DBI::dbGetQuery(con, "SELECT sql FROM main.sqlite_master WHERE name = 'cran_tarballs'")$sql
  expect_match(sql, "WITHOUT ROWID", fixed = TRUE)
  info <- DBI::dbGetQuery(con, "PRAGMA main.table_info(cran_tarballs)")
  key <- info[info$pk > 0, , drop = FALSE]
  expect_equal(key$name[order(key$pk)], c("package", "version", "revision"))
  expect_error(DBI::dbExecute(con, "INSERT INTO cran_tarballs VALUES
    ('cli', '3.6.5', 1, 1000, '2026-10-01T00:00:00Z', NULL, 'current',
     '2026-10-02', '2026-10-01')"), "CHECK constraint failed")
  expect_equal(DBI::dbGetQuery(con, "SELECT MAX(revision) AS r FROM cran_tarballs
                                     WHERE package = 'lmeInfo'")$r, 2)
})

test_that("the pull request tallies and rename episodes keep their keys and checks", {
  dir <- withr::local_tempdir()
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE, with_added = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  quiet_merge_sources(con, dir)

  expect_error(DBI::dbExecute(con, "INSERT INTO vcs_pr_quarterly VALUES
    ('github.com/r-lib/cli', '2026-Q4', 'MEMBER', 'User', 0, 1, 9)"), "UNIQUE")
  expect_error(DBI::dbExecute(con, "INSERT INTO vcs_pr_quarterly VALUES
    ('github.com/r-lib/cli', '2026-Q4', 'MEMBER', 'User', 2, 1, 1)"), "CHECK constraint failed")
  expect_equal(DBI::dbGetQuery(con, "SELECT SUM(prs) AS n FROM vcs_pr_quarterly")$n, 5)
  got <- DBI::dbGetQuery(con, "SELECT name_with_owner FROM vcs_repo_name_history
                               WHERE ended_on IS NULL")
  expect_equal(got$name_with_owner, "r-lib/log4r")
  # The walk cursor is the producer's own state.
  expect_false("vcs_ai_repo_reads" %in% output_tables(con))
})

test_that("queue.db brings the archive-folder episodes, read log and index", {
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "queue.db"), c(
    "CREATE TABLE queue_snapshots (snapshot_time TEXT, package TEXT, version TEXT, folder TEXT)",
    "INSERT INTO queue_snapshots VALUES ('2026-10-01 00:49:55', 'polle', '1.6.5', 'pretest')",
    "CREATE TABLE IF NOT EXISTS queue_archive_episodes (
       package TEXT NOT NULL, version TEXT NOT NULL, mtime TEXT NOT NULL, size_kb REAL,
       first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
       PRIMARY KEY (package, version, mtime)) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS idx_qae_first_seen ON queue_archive_episodes(first_seen)",
    # One version uploaded twice is two episodes.
    "INSERT INTO queue_archive_episodes VALUES
       ('acR', '1.2.0', '2026-09-29 00:42', 20, '2026-09-30 00:05:12', '2026-10-01 00:04:48'),
       ('acR', '1.2.0', '2026-09-29 13:02', 2662.4, '2026-09-30 00:05:12', '2026-09-30 00:05:12')",
    "CREATE TABLE IF NOT EXISTS queue_archive_reads (read_at TEXT PRIMARY KEY, listed INTEGER NOT NULL)",
    "INSERT INTO queue_archive_reads VALUES ('2026-09-30 00:05:12', 292), ('2026-10-01 00:04:48', 297)"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["queue.db"]]

  expect_equal(stats$status, "merged")
  expect_equal(unlist(stats$tables[c("queue_archive_episodes", "queue_archive_reads")]),
               c(queue_archive_episodes = 2, queue_archive_reads = 2))
  expect_match(DBI::dbGetQuery(con, "SELECT sql FROM main.sqlite_master
                                     WHERE name = 'queue_archive_episodes'")$sql,
               "WITHOUT ROWID", fixed = TRUE)
  expect_equal(indexes_on(con, "queue_archive_episodes")$name, "idx_qae_first_seen")
  expect_equal(index_columns(con, "idx_qae_first_seen"), "first_seen")
})

test_that("metadata.db brings the bounce episodes and flavor history with their keys and open-row index", {
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "metadata.db"), c(
    "CREATE TABLE cran_check_results (package TEXT, flavor TEXT, status TEXT,
       version TEXT, flags TEXT)",
    "INSERT INTO cran_check_results VALUES
       ('bunsen', 'r-devel-linux-x86_64-debian-gcc', 'ERROR', '0.1.1', '--no-vignettes')",
    "CREATE TABLE IF NOT EXISTS cran_maintainer_bounces (
       package TEXT NOT NULL, episode_seq INTEGER NOT NULL, version TEXT,
       onset_known INTEGER NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
       resolved_on TEXT, outcome TEXT, archived_on TEXT,
       PRIMARY KEY (package, episode_seq),
       CHECK (resolved_on IS NULL OR resolved_on <> ''),
       CHECK ((resolved_on IS NULL) = (outcome IS NULL)),
       CHECK (last_seen >= first_seen))",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_cran_maintainer_bounces_open
       ON cran_maintainer_bounces(package) WHERE resolved_on IS NULL",
    "INSERT INTO cran_maintainer_bounces VALUES
       ('permRand', 1, '1.0.0', 0, '2026-10-01', '2026-10-03', '2026-10-04', 'vanished', NULL),
       ('bunsen', 1, '0.1.1', 0, '2026-10-01', '2026-10-04', NULL, NULL, NULL)",
    "CREATE TABLE cran_check_flavors (flavor_id INTEGER PRIMARY KEY, flavor TEXT NOT NULL UNIQUE)",
    "INSERT INTO cran_check_flavors VALUES (1, 'r-devel-linux-x86_64-debian-gcc')",
    "CREATE TABLE cran_check_flavor_status_history (
       package TEXT NOT NULL, flavor_id INTEGER NOT NULL, episode_seq INTEGER NOT NULL,
       status TEXT NOT NULL, flags TEXT, first_version TEXT, last_version TEXT,
       first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, ended_on TEXT,
       PRIMARY KEY (package, flavor_id, episode_seq)) WITHOUT ROWID",
    "INSERT INTO cran_check_flavor_status_history VALUES
       ('bunsen', 1, 1, 'ERROR', '--no-vignettes', '0.1.1', '0.1.1',
        '2026-09-20', '2026-10-04', NULL)"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- quiet_merge_sources(con, dir)[["metadata.db"]]

  expect_equal(stats$status, "merged")
  expect_equal(unlist(stats$tables[c("cran_maintainer_bounces", "cran_check_flavors",
                                     "cran_check_flavor_status_history")]),
               c(cran_maintainer_bounces = 2, cran_check_flavors = 1,
                 cran_check_flavor_status_history = 1))
  ux <- indexes_on(con, "cran_maintainer_bounces")
  expect_equal(ux$name, "ux_cran_maintainer_bounces_open")
  expect_match(ux$sql, "WHERE resolved_on IS NULL", fixed = TRUE)
  # A second open episode for bunsen is refused; permRand's is closed, so a new one opens.
  expect_error(DBI::dbExecute(con, "INSERT INTO cran_maintainer_bounces VALUES
    ('bunsen', 2, '0.1.1', 1, '2026-10-05', '2026-10-05', NULL, NULL, NULL)"), "UNIQUE")
  expect_no_error(DBI::dbExecute(con, "INSERT INTO cran_maintainer_bounces VALUES
    ('permRand', 2, '1.0.1', 1, '2026-10-05', '2026-10-05', NULL, NULL, NULL)"))
  expect_error(DBI::dbExecute(con, "INSERT INTO cran_maintainer_bounces VALUES
    ('cli', 1, '3.6.5', 1, '2026-10-05', '2026-10-05', '2026-10-06', NULL, NULL)"),
    "CHECK constraint failed")
  info <- DBI::dbGetQuery(con, "PRAGMA main.table_info(cran_check_flavor_status_history)")
  key <- info[info$pk > 0, , drop = FALSE]
  expect_equal(key$name[order(key$pk)], c("package", "flavor_id", "episode_seq"))
})

test_that("a table name is matched across sources without regard to case", {
  dir <- withr::local_tempdir()
  # The vcs columns under a name that differs only in case. SQLite treats the
  # two as one table, so without the guard the vcs rows would join these.
  write_db(file.path(dir, "queue.db"), c(
    "CREATE TABLE Repo_Package_Links (repo_id TEXT NOT NULL, package TEXT NOT NULL,
       origin TEXT NOT NULL, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL,
       PRIMARY KEY (repo_id, package, origin))",
    "INSERT INTO Repo_Package_Links VALUES
       ('github.com/q/qpkg', 'qpkg', 'cran', '2026-10-01', '2026-10-01')"))
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  expect_warning(stats <- quiet_merge_sources(con, dir),
                 "Error processing vcs-signals-summary.db")

  expect_equal(stats[["queue.db"]]$status, "merged")
  vcs <- stats[["vcs-signals-summary.db"]]
  expect_equal(vcs$status, "error")
  expect_equal(vcs$failed_tables[["repo_package_links"]],
               "not copied, queue.db already brought a table named Repo_Package_Links")
  expect_equal(DBI::dbGetQuery(con, "SELECT package FROM repo_package_links")$package, "qpkg")
  expect_equal(vcs$tables$vcs_signals_summary, 2)
})

test_that("a refused table reddens the run through the gate and still publishes", {
  source(file.path(getwd(), "..", "..", "merge_gate.R"), local = TRUE)
  dir <- withr::local_tempdir()
  write_db(file.path(dir, "queue.db"), "CREATE TABLE repo_package_links (repo_id TEXT)")
  write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE)
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))
  stats <- suppressWarnings(quiet_merge_sources(con, dir))
  # merge.R and check-freshness.R pass the failures through this file.
  path <- file.path(dir, ".merge-failed")
  write_merge_failures(path, merge_failures(stats))

  dbs <- c("queue.db", "vcs-signals-summary.db")
  res <- evaluate_freshness_gate(
    meta = data.frame(pipeline = c("cran-queue", "vcs-signals"),
                      last_checked = "2026-10-01T09:00:00Z", last_changed = NA_character_,
                      released_at = NA_character_, expected_max_age_hours = c(3L, 30L)),
    present_dbs = dbs, all_source_dbs = dbs,
    config = list(list(name = "cran-queue", max_age_h = 3L, db_filename = "queue.db"),
                  list(name = "vcs-signals", max_age_h = 30L,
                       db_filename = "vcs-signals-summary.db")),
    now_iso = "2026-10-01T10:00:00Z", output_bytes = NA_real_,
    merge_failed = read_merge_failures(path))

  verdict <- stats::setNames(res$rows$verdict, res$rows$source)
  expect_equal(verdict[["queue.db"]], "ok")
  expect_equal(verdict[["vcs-signals-summary.db"]], "merge error")
  expect_match(res$rows$detail[res$rows$source == "vcs-signals-summary.db"],
               "queue.db already brought a table named repo_package_links", fixed = TRUE)
  expect_true(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("only an overlap listed for both sources lets them share a table name", {
  dir <- withr::local_tempdir()
  for (src in c("a", "b", "c")) {
    write_db(file.path(dir, paste0(src, ".db")), c(
      "CREATE TABLE shared_names (name TEXT PRIMARY KEY)",
      sprintf("INSERT INTO shared_names VALUES ('from %s')", src)))
  }
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  stats <- NULL
  expect_warning(
    utils::capture.output(stats <- merge_sources(
      con, dir, dbs = c("a.db", "b.db", "c.db"),
      tables = list("a.db" = NULL, "b.db" = NULL, "c.db" = NULL),
      overlaps = list(Shared_Names = c("a.db", "b.db")))),
    "Error processing c.db: table shared_names: not copied, a.db already brought")

  expect_equal(vapply(stats, function(s) s$status, character(1)),
               c(a.db = "merged", b.db = "merged", c.db = "error"))
  expect_setequal(DBI::dbGetQuery(con, "SELECT name FROM shared_names")$name,
                  c("from a", "from b"))
})

test_that("a table that fails to copy inside the source loop is still an error", {
  dir <- withr::local_tempdir()
  src <- write_vcs_summary(file.path(dir, "vcs-signals-summary.db"), with_links = TRUE,
                           with_added = TRUE)
  # A row the owner-type check refuses, so vcs_repo_owner fails partway.
  write_db(src, c("PRAGMA ignore_check_constraints = 1",
                  "INSERT INTO vcs_repo_owner VALUES
                     ('github.com/x/y', 'R_x', 'x', 'Bot', 'O_x', 'x/y', '2026-10-04')"))
  write_task_views(file.path(dir, "cran-task-views.db"))
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(dir, "observatory.db"))
  withr::defer(DBI::dbDisconnect(con))

  expect_warning(stats <- quiet_merge_sources(con, dir),
                 "Error processing vcs-signals-summary.db.*vcs_repo_owner.*CHECK constraint failed")

  vcs <- stats[["vcs-signals-summary.db"]]
  expect_equal(vcs$status, "error")
  expect_named(vcs$failed_tables, "vcs_repo_owner")
  expect_match(merge_failures(stats)[["vcs-signals-summary.db"]],
               "table vcs_repo_owner: .*CHECK constraint failed")
  expect_equal(stats[["cran-task-views.db"]]$status, "merged")
})
