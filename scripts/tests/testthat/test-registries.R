# Source the merge_helpers from scripts directory
# Go up from the current test directory (scripts/tests/testthat) to scripts (2 levels up)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

test_that("copr_rows_from_monitor keeps R-CRAN- packages and strips the prefix", {
  mon <- list(packages = list(
    list(name = "R-CRAN-Rcpp"),
    list(name = "R-CRAN-a11yShiny"),
    list(name = "jags"),          # non-CRAN, dropped
    list(name = "R")))            # non-CRAN, dropped
  out <- copr_rows_from_monitor(mon)
  expect_setequal(out$name, c("Rcpp", "a11yShiny"))
  expect_equal(out$name_lower[out$name == "Rcpp"], "rcpp")
})

test_that("copr_rows_from_monitor handles an empty/missing list", {
  out <- copr_rows_from_monitor(list(packages = list()))
  expect_equal(nrow(out), 0)
  expect_equal(names(out), c("name", "name_lower"))
})

test_that("r_versions_from_list extracts version + date (date-only)", {
  lst <- list(
    list(version = "3.6.0", date = "2019-04-26T07:05:03.123Z"),
    list(version = "4.6.1", date = "2026-06-24T07:14:42.209715Z"))
  out <- r_versions_from_list(lst)
  expect_equal(out$version, c("3.6.0", "4.6.1"))
  expect_equal(out$released, c("2019-04-26", "2026-06-24"))
})
