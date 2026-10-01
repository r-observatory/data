source(file.path(getwd(), "..", "..", "history", "check_flavor_fold.R"))

fold_file <- file.path(getwd(), "..", "..", "history", "check_flavor_fold.R")
golden_file <- file.path(getwd(), "fixtures", "check-flavor-golden.json")

file_sha256 <- function(path) {
  if ("sha256sum" %in% getNamespaceExports("tools")) {
    return(unname(getExportedValue("tools", "sha256sum")(path)))
  }
  sub(" .*$", "", system2("shasum", c("-a", "256", shQuote(path)), stdout = TRUE)[1])
}

# One fixture day as a data frame, NULL for a day whose fetch failed.
golden_day <- function(day) {
  if (length(day$rows) == 0L) return(NULL)
  cols <- unlist(day$columns)
  out <- lapply(seq_along(cols), function(j) {
    unlist(lapply(day$rows, function(r) if (is.null(r[[j]])) NA else r[[j]]))
  })
  as.data.frame(stats::setNames(out, cols), stringsAsFactors = FALSE)
}

memory_db <- function(env = parent.frame()) {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  withr::defer(DBI::dbDisconnect(con), envir = env)
  con
}

check_rows <- function(package, flavor, status, version = NULL, flags = NULL) {
  df <- data.frame(Package = package, Flavor = flavor, Status = status, stringsAsFactors = FALSE)
  if (!is.null(version)) df$Version <- version
  if (!is.null(flags)) df$Flags <- flags
  df
}

open_episodes <- function(con) {
  DBI::dbGetQuery(con, "SELECT package, flavor_id, episode_seq, status, flags, first_version,
                               last_version, first_seen, last_seen
                          FROM cran_check_flavor_status_history WHERE ended_on IS NULL
                         ORDER BY package, flavor_id")
}

test_that("the golden days leave exactly the expected episodes", {
  golden <- jsonlite::read_json(golden_file, simplifyVector = FALSE)
  con <- memory_db()
  prior <- NULL
  outcomes <- character(0)
  for (day in golden$days) {
    step <- check_flavor_step(con, golden_day(day), day$on, prior,
                              fetched = isTRUE(day$fetched), fill_flags = isTRUE(day$fill_flags))
    outcomes <- c(outcomes, step$outcome)
    if (identical(step$outcome, "applied")) expect_equal(step$counts$mismatches, 0L, info = day$on)
    prior <- step$prior
  }
  expect_equal(outcomes, unlist(golden$expected$outcomes))

  flavors <- DBI::dbGetQuery(con, "SELECT flavor_id, flavor FROM cran_check_flavors ORDER BY flavor_id")
  expect_equal(flavors$flavor_id, vapply(golden$expected$flavors, function(f) as.integer(f[[1]]), 1L))
  expect_equal(flavors$flavor, vapply(golden$expected$flavors, function(f) f[[2]], ""))

  cols <- unlist(golden$expected$episode_columns)
  got <- DBI::dbGetQuery(con, sprintf(
    "SELECT %s FROM cran_check_flavor_status_history", paste(cols, collapse = ", ")))
  got <- got[order(got$package, got$flavor_id, got$episode_seq, method = "radix"), ]
  want <- golden_day(list(columns = golden$expected$episode_columns,
                          rows = golden$expected$episodes))
  for (col in cols) {
    expect_equal(as.character(got[[col]]), as.character(want[[col]]), info = col)
  }
})

test_that("the fold file and the golden fixture are the copies cran-metadata carries", {
  expect_equal(file_sha256(fold_file), "a88c811f096450d5e349b618c27e4ca9b5b8b145549b3f493844fa3e04c0e1f9")
  expect_equal(file_sha256(golden_file), "8e294c59828279485473677172e3723c799f6d9078d9e86dca68797334c5bbc9")
})

test_that("CRAN_check_results() columns and the stored table normalize alike", {
  raw <- data.frame(Flavor = c("f1", "f1", "f2", "f2"), Package = c("b", "b", "a", "c"),
                    Version = c("1.0", "1.0", " ", "2.0"), Status = c("OK", "OK", "NOTE", ""),
                    Flags = c("", "", "--no-tests", NA), T_install = c("1.5", "1.5", "2", "3"),
                    Maintainer = "m", stringsAsFactors = FALSE)
  norm <- normalize_check_results(raw)
  expect_equal(names(norm), c("package", "flavor", "status", "version", "flags",
                              "tinstall"))
  expect_equal(norm$package, c("a", "b"))
  expect_equal(norm$version, c(NA, "1.0"))
  expect_equal(norm$flags, c("--no-tests", NA))
  expect_equal(norm$tinstall, c(2, 1.5))
})

test_that("the guard names stale, unhealthy and applied tables", {
  prior <- list(rows = 100L, fingerprint = "abc")
  expect_equal(check_flavor_guard(100L, "new", prior, fetched = FALSE), "stale")
  expect_equal(check_flavor_guard(100L, "abc", prior), "stale")
  expect_equal(check_flavor_guard(0L, "new", NULL), "unhealthy")
  expect_equal(check_flavor_guard(49L, "new", prior), "unhealthy")
  expect_equal(check_flavor_guard(50L, "new", prior), "applied")
  expect_equal(check_flavor_guard(3L, "new", NULL), "applied")
})

test_that("empty and missing flags are the same value", {
  con <- memory_db()
  day1 <- check_rows(c("a", "b"), "f1", "OK", version = "1.0", flags = c("", "--no-tests"))
  s1 <- check_flavor_step(con, day1, "2026-10-01")
  day2 <- check_rows(c("a", "b"), "f1", "OK", version = "1.0", flags = c(NA, "--no-tests"))
  s2 <- check_flavor_step(con, day2, "2026-10-02", s1$prior)
  expect_equal(s2$outcome, "stale")
  day3 <- check_rows(c("a", "b"), "f1", "OK", version = "1.1", flags = c(" ", "--no-tests"))
  s3 <- check_flavor_step(con, day3, "2026-10-03", s2$prior)
  expect_equal(s3$outcome, "applied")
  expect_equal(s3$counts$opened, 0L)
  expect_equal(s3$counts$closed, 0L)
  expect_equal(open_episodes(con)$flags, c(NA, "--no-tests"))
})

test_that("a new version with the same result moves the episode, a new flag opens one", {
  con <- memory_db()
  s1 <- check_flavor_step(con, check_rows("a", "f1", "OK", version = "1.0", flags = ""),
                          "2026-10-01")
  s2 <- check_flavor_step(con, check_rows("a", "f1", "OK", version = "1.1", flags = ""),
                          "2026-10-02", s1$prior)
  expect_equal(s2$counts$extended, 1L)
  ep <- open_episodes(con)
  expect_equal(c(ep$episode_seq, ep$first_version, ep$last_version), c("1", "1.0", "1.1"))
  s3 <- check_flavor_step(con, check_rows("a", "f1", "OK", version = "1.1",
                                          flags = "--no-vignettes"), "2026-10-03", s2$prior)
  expect_equal(c(s3$counts$closed, s3$counts$opened), c(1L, 1L))
  ep <- open_episodes(con)
  expect_equal(c(ep$episode_seq, ep$flags, ep$first_version), c("2", "--no-vignettes", "1.1"))
})

test_that("open episodes are compared with a results table row for row", {
  con <- memory_db()
  day <- check_rows(c("a", "b"), c("f1", "f2"), c("OK", "ERROR"), flags = c("", "--no-tests"))
  check_flavor_step(con, day, "2026-10-01")
  expect_equal(check_flavor_open_mismatches(con, day), 0L)
  other <- day
  other$Status[2] <- "OK"
  expect_equal(check_flavor_open_mismatches(con, other), 2L)
  other <- day
  other$Flags[1] <- "--no-vignettes"
  expect_equal(check_flavor_open_mismatches(con, other), 2L)
  expect_equal(check_flavor_open_mismatches(con, rbind(day, check_rows("c", "f9", "OK", flags = ""))), 1L)
})
