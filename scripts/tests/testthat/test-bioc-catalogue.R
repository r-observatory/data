# Source the merge_helpers from scripts directory
# Go up from the current test directory (scripts/tests/testthat) to scripts (2 levels up)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

test_that("bioc_rows_from_available turns an available.packages matrix into registry rows", {
  mat <- matrix(
    c("BiocGenerics", "0.52.0",
      "S4Vectors",    "0.44.0"),
    ncol = 2, byrow = TRUE,
    dimnames = list(c("BiocGenerics", "S4Vectors"), c("Package", "Version")))
  out <- bioc_rows_from_available(mat, "software")
  expect_equal(nrow(out), 2)
  expect_equal(out$name, c("BiocGenerics", "S4Vectors"))
  expect_equal(out$name_lower, c("biocgenerics", "s4vectors"))
  expect_equal(out$version, c("0.52.0", "0.44.0"))
  expect_true(all(out$category == "software"))
})

test_that("bioc_rows_from_available handles an empty matrix", {
  mat <- matrix(character(0), ncol = 2, dimnames = list(NULL, c("Package", "Version")))
  out <- bioc_rows_from_available(mat, "software")
  expect_equal(nrow(out), 0)
  expect_equal(names(out), c("name", "name_lower", "version", "category"))
})
