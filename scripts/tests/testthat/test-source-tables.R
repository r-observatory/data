# Source the merge_helpers from scripts directory
# Go up from the current test directory (scripts/tests/testthat) to scripts (2 levels up)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

test_that("returns NULL (meaning 'all tables') for unconfigured sources", {
  config <- list("feed.db" = NULL)
  expect_null(tables_to_merge_from("feed.db", config))
})

test_that("returns explicit allowlist for configured sources", {
  config <- list("downloads-summary.db" = c("downloads_summary"))
  expect_equal(
    tables_to_merge_from("downloads-summary.db", config),
    c("downloads_summary")
  )
})

test_that("returns NULL for sources missing from config (defaults to all)", {
  config <- list()
  expect_null(tables_to_merge_from("queue.db", config))
})

test_that("preserves multi-table allowlists", {
  config <- list("foo.db" = c("t1", "t2", "t3"))
  expect_equal(
    tables_to_merge_from("foo.db", config),
    c("t1", "t2", "t3")
  )
})

test_that("r2u-summary.db ingests only the r2u_downloads_summary table", {
  config <- list("r2u-summary.db" = c("r2u_downloads_summary"))
  expect_equal(
    tables_to_merge_from("r2u-summary.db", config),
    c("r2u_downloads_summary")
  )
})

test_that("autoobs-downloads-summary.db ingests only the autoobs_downloads_summary table", {
  config <- list("autoobs-downloads-summary.db" = c("autoobs_downloads_summary"))
  expect_equal(
    tables_to_merge_from("autoobs-downloads-summary.db", config),
    c("autoobs_downloads_summary")
  )
})

test_that("copr-downloads-summary.db ingests only the copr_downloads_summary table", {
  config <- list("copr-downloads-summary.db" = c("copr_downloads_summary"))
  expect_equal(
    tables_to_merge_from("copr-downloads-summary.db", config),
    c("copr_downloads_summary")
  )
})

test_that("conda-forge-downloads-summary.db ingests only the conda_forge_downloads_summary table", {
  config <- list("conda-forge-downloads-summary.db" = c("conda_forge_downloads_summary"))
  expect_equal(
    tables_to_merge_from("conda-forge-downloads-summary.db", config),
    c("conda_forge_downloads_summary")
  )
})

test_that("bioconda-downloads-summary.db ingests only the bioconda_downloads_summary table", {
  config <- list("bioconda-downloads-summary.db" = c("bioconda_downloads_summary"))
  expect_equal(
    tables_to_merge_from("bioconda-downloads-summary.db", config),
    c("bioconda_downloads_summary")
  )
})
