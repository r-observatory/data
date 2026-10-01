source(file.path(getwd(), "..", "..", "history", "fold.R"))

fold_db <- function(env = parent.frame()) {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  withr::defer(DBI::dbDisconnect(con), envir = env)
  DBI::dbExecute(con, "CREATE TEMP TABLE day_rows (package TEXT, origin TEXT, total INTEGER, rank INTEGER)")
  con
}

# Loads `rows` as the snapshot and folds it into totals_history on `on`.
fold_day <- function(con, rows, on, values = c("total", "rank")) {
  DBI::dbExecute(con, "DELETE FROM temp.day_rows")
  if (nrow(rows) > 0L) DBI::dbAppendTable(con, DBI::Id(schema = "temp", table = "day_rows"), rows)
  types <- c(total = "INTEGER", rank = "INTEGER")[values]
  load <- load_history_snap(con, sprintf("SELECT package, %s FROM temp.day_rows",
                                         paste(values, collapse = ", ")),
                            "package", "TEXT", values, types)
  fill <- ensure_episode_table(con, "totals_history", "package", "TEXT", values, types)
  c(load, fold_episodes(con, "totals_history", "package", values, on, fill))
}

rows <- function(package, total, rank = NA_integer_) {
  data.frame(package = package, origin = "cran", total = as.integer(total),
             rank = as.integer(rank), stringsAsFactors = FALSE)
}

episodes <- function(con) {
  DBI::dbGetQuery(con, "SELECT * FROM totals_history ORDER BY package, episode_seq")
}

test_that("an unchanged value extends its episode", {
  con <- fold_db()
  fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-01")
  got <- fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-02")
  expect_equal(c(got$extended, got$closed, got$opened), c(2L, 0L, 0L))
  expect_equal(episodes(con)$last_seen, c("2026-07-02", "2026-07-02"))
})

test_that("a changed value closes the episode on that snapshot and opens the next", {
  con <- fold_db()
  fold_day(con, rows("a", 1), "2026-07-01")
  fold_day(con, rows("a", 5), "2026-07-04")
  e <- episodes(con)
  expect_equal(e$episode_seq, c(1L, 2L))
  expect_equal(e$last_seen, c("2026-07-01", "2026-07-04"))
  expect_equal(e$ended_on, c("2026-07-04", NA))
  expect_equal(e$total, c(1L, 5L))
})

test_that("an absent key closes, and its return opens a new episode", {
  con <- fold_db()
  fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-01")
  fold_day(con, rows("a", 1), "2026-07-02")
  fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-03")
  e <- episodes(con)[episodes(con)$package == "b", ]
  expect_equal(e$episode_seq, c(1L, 2L))
  expect_equal(e$ended_on, c("2026-07-02", NA))
  expect_equal(e$first_seen, c("2026-07-01", "2026-07-03"))
})

test_that("NULL equals NULL and never equals a value", {
  con <- fold_db()
  fold_day(con, rows(c("a", "b"), c(1, 2), c(NA, NA)), "2026-07-01")
  got <- fold_day(con, rows(c("a", "b"), c(1, 2), c(NA, 7)), "2026-07-02")
  expect_equal(c(got$extended, got$closed, got$opened), c(1L, 1L, 1L))
  expect_equal(episodes(con)$rank, c(NA, NA, 7L))
})

test_that("a new column is filled into open episodes, not counted as a change", {
  con <- fold_db()
  fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-01", values = "total")
  expect_false("rank" %in% history_table_columns(con, "totals_history")$name)
  got <- fold_day(con, rows(c("a", "b"), c(1, 3), c(4, 5)), "2026-07-02")
  expect_equal(got$filled, 2L)
  e <- episodes(con)
  expect_equal(e$package, c("a", "b", "b"))
  expect_equal(e$rank, c(4L, 5L, 5L))
  expect_equal(e$ended_on, c(NA, "2026-07-02", NA))
})

test_that("a column the source drops is not compared", {
  con <- fold_db()
  fold_day(con, rows(c("a", "b"), c(1, 2), c(8, 9)), "2026-07-01")
  got <- fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-02", values = "total")
  expect_equal(c(got$extended, got$closed, got$opened), c(2L, 0L, 0L))
  got <- fold_day(con, rows(c("a", "c"), c(1, 6)), "2026-07-03", values = "total")
  e <- episodes(con)
  expect_equal(e$rank, c(8L, 9L, NA))
  expect_equal(e$ended_on, c(NA, "2026-07-03", NA))
})

test_that("rows with a NULL or repeated key are left out and counted", {
  con <- fold_db()
  got <- fold_day(con, rows(c("a", "a", NA, "b"), c(3, 1, 5, 2)), "2026-07-01")
  expect_equal(c(got$read, got$kept), c(4L, 2L))
  expect_equal(episodes(con)$total, c(1L, 2L))
})

test_that("the open episodes equal the snapshot after every fold", {
  con <- fold_db()
  fold_day(con, rows(c("a", "b"), c(1, 2)), "2026-07-01")
  fold_day(con, rows(c("a", "c"), c(4, 2)), "2026-07-02")
  expect_equal(open_mismatches(con, "totals_history", "package", c("total", "rank")), 0L)
  DBI::dbExecute(con, "UPDATE totals_history SET total = 99 WHERE package = 'c'")
  expect_equal(open_mismatches(con, "totals_history", "package", c("total", "rank")), 2L)
})

test_that("a snapshot under half the rows last applied is unhealthy", {
  expect_equal(series_guard(0L, NA), "unhealthy")
  expect_equal(series_guard(4L, 10L), "unhealthy")
  expect_equal(series_guard(5L, 10L), "applied")
  expect_equal(series_guard(1L, NA), "applied")
})

test_that("the episode table refuses overlapping dates and a second open row", {
  con <- fold_db()
  fold_day(con, rows("a", 1), "2026-07-01")
  expect_error(DBI::dbExecute(con, "INSERT INTO totals_history
    (package, episode_seq, total, rank, first_seen, last_seen) VALUES
    ('a', 2, 5, NULL, '2026-07-02', '2026-07-02')"), "UNIQUE")
  expect_error(DBI::dbExecute(con, "UPDATE totals_history SET ended_on = last_seen"), "CHECK")
})

test_that("the ledger tables accept only known outcomes", {
  con <- fold_db()
  ensure_history_ledger(con)
  ensure_history_ledger(con)
  expect_error(DBI::dbExecute(con, "INSERT INTO history_snapshots
    (family, tag, snapshot_on, outcome, processed_at) VALUES ('data', 'v', '2026-07-01', 'odd', 'x')"),
    "CHECK")
  expect_error(DBI::dbExecute(con, "INSERT INTO history_series_observations
    (family, tag, series, outcome) VALUES ('data', 'v', 's', 'odd')"), "CHECK")
})
