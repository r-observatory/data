source(file.path(getwd(), "..", "..", "merge_helpers.R"))

test_that("source_dbs and source_tables are defined by the helpers module", {
  expect_true(is.character(source_dbs) && length(source_dbs) > 0)
  expect_true(is.list(source_tables))
})

test_that("every source DB has a source_tables entry (two-list gotcha guard)", {
  missing <- setdiff(source_dbs, names(source_tables))
  expect_identical(missing, character(0))
})

test_that("no source_tables entry references a DB absent from source_dbs", {
  extra <- setdiff(names(source_tables), source_dbs)
  expect_identical(extra, character(0))
})

test_that("cran-coverage.db is registered in both merger lists", {
  expect_true("cran-coverage.db" %in% source_dbs)
  expect_equal(
    tables_to_merge_from("cran-coverage.db", source_tables),
    c("coverage_summary", "coverage_file", "coverage_function")
  )
})

test_that("code-metrics DBs expose summary, api_history and detail tables", {
  expect_equal(
    tables_to_merge_from("cran-code-metrics.db", source_tables),
    c("cran_code_summary", "cran_api_history", "cran_functions", "cran_call_edges", "cran_archived_meta", "cran_author_package_span")
  )
  expect_equal(
    tables_to_merge_from("bioc-code-metrics.db", source_tables),
    c("bioc_code_summary", "bioc_api_history", "bioc_functions", "bioc_call_edges")
  )
})

test_that("dataset row_sketch tables stay out of observatory.db", {
  expect_false("cran_dataset_sketches" %in% tables_to_merge_from("cran-data-metrics.db", source_tables))
  expect_false("bioc_dataset_sketches" %in% tables_to_merge_from("bioc-data-metrics.db", source_tables))
})

test_that("code-metrics DBs carry code tables (plus cran archived metadata) after the split", {
  # cran-code-metrics also parses each archived DESCRIPTION, so it ships the
  # cran_archived_meta table that powers removed-package detail pages.
  expect_equal(
    tables_to_merge_from("cran-code-metrics.db", source_tables),
    c("cran_code_summary", "cran_api_history", "cran_functions", "cran_call_edges", "cran_archived_meta", "cran_author_package_span")
  )
  expect_equal(
    tables_to_merge_from("bioc-code-metrics.db", source_tables),
    c("bioc_code_summary", "bioc_api_history", "bioc_functions", "bioc_call_edges")
  )
})

test_that("data-metrics DBs are registered and carry only dataset tables", {
  expect_true("cran-data-metrics.db" %in% source_dbs)
  expect_true("bioc-data-metrics.db" %in% source_dbs)
  expect_equal(
    tables_to_merge_from("cran-data-metrics.db", source_tables),
    c("cran_datasets", "cran_dataset_versions", "cran_dataset_contents")
  )
  expect_equal(
    tables_to_merge_from("bioc-data-metrics.db", source_tables),
    c("bioc_datasets", "bioc_dataset_versions", "bioc_dataset_contents")
  )
})

test_that("sketch tables never enter observatory.db from either DB", {
  expect_false("cran_dataset_sketches" %in% tables_to_merge_from("cran-data-metrics.db", source_tables))
  expect_false("bioc_dataset_sketches" %in% tables_to_merge_from("bioc-data-metrics.db", source_tables))
})

test_that("the name-authority tables are copied into observatory.db", {
  expect_true("cran_names_all" %in% tables_to_merge_from("cran-archive.db", source_tables))
  expect_true("bioc_names_all" %in% tables_to_merge_from("bioconductor-metadata.db", source_tables))
})

test_that("cran-archive exposes archive, events, name authority, history, and lineage", {
  expect_true("cran-archive.db" %in% source_dbs)
  expect_equal(
    tables_to_merge_from("cran-archive.db", source_tables),
    c("cran_archive", "cran_archive_events", "cran_names_all", "cran_archive_history",
      "cran_archive_lineage", "cran_archive_action_counts")
  )
})

test_that("the merge workflow downloads cran-coverage", {
  yml <- readLines(file.path(getwd(), "..", "..", "..",
                             ".github", "workflows", "merge.yml"))
  expect_true(any(grepl("cran-coverage", yml)))
})

test_that("vcs-signals is registered in both merger lists", {
  expect_true("vcs-signals-summary.db" %in% source_dbs)
  expect_equal(
    tables_to_merge_from("vcs-signals-summary.db", source_tables),
    c("vcs_signals_summary")
  )
})

test_that("the merge workflow downloads vcs-signals", {
  yml <- readLines(file.path(getwd(), "..", "..", "..",
                             ".github", "workflows", "merge.yml"))
  expect_true(any(grepl("vcs-signals", yml)))
})

test_that("the merge workflow downloads the split code and data metrics DBs", {
  yml <- readLines(file.path(getwd(), "..", "..", "..",
                             ".github", "workflows", "merge.yml"))
  joined <- paste(yml, collapse = "\n")
  expect_true(grepl("cran-data-metrics.db", joined, fixed = TRUE))
  expect_true(grepl("bioc-data-metrics.db", joined, fixed = TRUE))
  # Dated-tag discovery, not the rolling "current" tag, for the metrics repos.
  expect_true(grepl("code-", joined, fixed = TRUE))
  expect_true(grepl("data-", joined, fixed = TRUE))
})

test_that("cran-task-views is registered in both merger lists", {
  expect_true("cran-task-views.db" %in% source_dbs)
  expect_equal(
    tables_to_merge_from("cran-task-views.db", source_tables),
    c("cran_task_views", "cran_task_view_events", "cran_task_view_membership")
  )
})

test_that("the merge workflow downloads cran-task-views", {
  yml <- readLines(file.path(getwd(), "..", "..", "..",
                             ".github", "workflows", "merge.yml"))
  expect_true(any(grepl("cran-task-views", yml)))
})

test_that("missing_expected_tables flags a dropped table only when the source was present", {
  expect_identical(missing_expected_tables(FALSE, c("a", "b"), c("a")), character(0))
  expect_identical(missing_expected_tables(TRUE,  c("a", "b"), c("a")), "b")
  expect_identical(missing_expected_tables(TRUE,  c("a", "b"), c("a", "b")), character(0))
})
