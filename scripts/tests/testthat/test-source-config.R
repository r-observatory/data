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
