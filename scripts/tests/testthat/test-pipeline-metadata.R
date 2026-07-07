# Source the producer module from the scripts directory (two levels up).
source(file.path(getwd(), "..", "..", "pipeline_metadata.R"))

test_that("max_data_through picks the latest shard date, NA when absent", {
  shards <- list(
    "r2u-2025.db" = list(date_max = "2025-12-31"),
    "r2u-2026.db" = list(date_max = "2026-06-01"))
  expect_equal(max_data_through(shards), "2026-06-01")
  expect_true(is.na(max_data_through(NULL)))
  expect_true(is.na(max_data_through(list())))
})

test_that("manifest_data_through falls back to summary$data_through without a shard map", {
  expect_equal(manifest_data_through(list(shards = list("a" = list(date_max = "2026-06-30")))), "2026-06-30")
  expect_equal(manifest_data_through(list(summary = list(data_through = "2026-07-07"))), "2026-07-07")
  expect_true(is.na(manifest_data_through(NULL)))
})

test_that("changed_summary describes manifest runs, NA without a manifest", {
  expect_equal(changed_summary(list(changed_shards = list("a", "b"))), "2 shards changed last run")
  expect_equal(changed_summary(list(changed_shards = list("a"))), "1 shard changed last run")
  expect_equal(changed_summary(list(changed_shards = list())), "no change last run")
  expect_true(is.na(changed_summary(NULL)))
})

test_that("build uses manifest timestamps and computes upstream lag for manifest pipelines", {
  fetched <- list(
    "r2u-downloads" = list(
      cfg = list(name = "r2u-downloads", repo = "r-observatory/r2u-downloads",
                 schedule = "daily 06:00 UTC", max_age_h = 840L, rolling = TRUE,
                 manifest = TRUE, upstream = "eddelbuettel/r2u-logs"),
      release = list(tag = "current", published_at = "2026-06-01T00:00:00Z"),
      manifest = list(last_checked = "2026-06-28T06:00:00Z",
                      last_changed = "2026-06-01T06:00:00Z",
                      upstream_head_sha = "67a7a71",
                      changed_shards = list("r2u-2026.db"),
                      shards = list("r2u-2026.db" = list(date_max = "2026-06-01"))),
      upstream = list(latest_sha = "67a7a71", latest_at = "2026-06-01T13:22:22Z")))

  df <- build_pipeline_metadata(fetched, now_iso = "2026-06-28T08:00:00Z")
  row <- df[df$pipeline == "r2u-downloads", ]
  expect_equal(row$last_checked, "2026-06-28T06:00:00Z")   # from manifest, not release
  expect_equal(row$last_changed, "2026-06-01T06:00:00Z")
  expect_equal(row$data_through, "2026-06-01")
  expect_equal(row$behind_upstream, 0L)                    # same sha -> not behind
  expect_equal(row$changed_summary, "1 shard changed last run")
  expect_equal(row$upstream_latest_at, "2026-06-01T13:22:22Z")
})

test_that("build flags behind_upstream when our sha lags the source", {
  fetched <- list("r2u-downloads" = list(
    cfg = list(name = "r2u-downloads", repo = "r", schedule = "s", max_age_h = 1L,
               rolling = TRUE, manifest = TRUE, upstream = "u"),
    release = list(tag = "current", published_at = "x"),
    manifest = list(upstream_head_sha = "OLDsha", changed_shards = list(), shards = list()),
    upstream = list(latest_sha = "NEWsha", latest_at = "2026-06-28T00:00:00Z")))
  df <- build_pipeline_metadata(fetched, "2026-06-28T08:00:00Z")
  expect_equal(df$behind_upstream[1], 1L)
})

test_that("build falls back to release time for plain pipelines, now for self", {
  fetched <- list(
    "cran-feed" = list(
      cfg = list(name = "cran-feed", repo = "r-observatory/cran-feed",
                 schedule = "every 6 hours", max_age_h = 8L, rolling = FALSE, manifest = FALSE),
      release = list(tag = "v20260628-191006", published_at = "2026-06-28T19:10:10Z"),
      manifest = NULL, upstream = NULL),
    "data" = list(
      cfg = list(name = "data", repo = "r-observatory/data", schedule = "daily 08:00 UTC",
                 max_age_h = 30L, rolling = FALSE, manifest = FALSE, self = TRUE),
      release = NULL, manifest = NULL, upstream = NULL))

  df <- build_pipeline_metadata(fetched, now_iso = "2026-06-28T08:00:00Z")

  feed <- df[df$pipeline == "cran-feed", ]
  expect_equal(feed$last_checked, "2026-06-28T19:10:10Z")  # falls back to release time
  expect_true(is.na(feed$behind_upstream))
  expect_true(is.na(feed$data_through))

  self <- df[df$pipeline == "data", ]
  expect_equal(self$released_at, "2026-06-28T08:00:00Z")   # self uses now
  expect_equal(self$last_changed, "2026-06-28T08:00:00Z")
})

test_that("write_pipeline_metadata creates the table and round-trips", {
  fetched <- list("data" = list(
    cfg = list(name = "data", repo = "r", schedule = "s", max_age_h = 30L,
               rolling = FALSE, manifest = FALSE, self = TRUE),
    release = NULL, manifest = NULL, upstream = NULL))
  df <- build_pipeline_metadata(fetched, "2026-06-28T08:00:00Z")
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  write_pipeline_metadata(con, df)
  got <- DBI::dbGetQuery(con, "SELECT pipeline, schedule FROM pipeline_metadata")
  expect_equal(got$pipeline, "data")
  expect_equal(got$schedule, "s")
})
