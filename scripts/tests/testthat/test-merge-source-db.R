# Edition 3, so expect_warning() lets through any warning it was not written for.
testthat::local_edition(3)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

# The tables vcs-signals adds, as the producer declares them, plus the three it
# keeps for its own next run, which must not be copied.
vcs_added_tables <- c("vcs_dev_tooling_rules", "vcs_ai_search_coverage",
                      "vcs_ai_review_signals", "vcs_ai_outside_prs",
                      "vcs_ai_ruleset_history", "vcs_repo_owner")
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
  "CREATE TABLE IF NOT EXISTS vcs_ai_repo_reads (repo_id TEXT PRIMARY KEY,
     commits_read_on TEXT) WITHOUT ROWID",
  "INSERT INTO vcs_ai_repo_reads VALUES ('github.com/o/rtika2', '2026-10-04')",
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

write_bioc_catalogue <- function(path, with_vignettes) {
  sql <- c(
    "CREATE TABLE bioc_packages (package TEXT PRIMARY KEY, category TEXT)",
    "INSERT INTO bioc_packages VALUES ('DESeq2', 'software'), ('airway', 'experiment')")
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
  expect_false(any(vcs_added_tables %in% output_tables(con)))
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
                 vcs_ai_ruleset_history = 1, vcs_repo_owner = 2))
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
