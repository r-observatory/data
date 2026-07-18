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

test_that("manifest_complete reads standardized and metrics-style flags, NA otherwise", {
  expect_equal(manifest_complete(list(complete = TRUE)), 1L)
  expect_equal(manifest_complete(list(complete = FALSE)), 0L)
  expect_equal(manifest_complete(list(bootstrap = list(bootstrap_complete = TRUE))), 1L)
  expect_equal(manifest_complete(list(bootstrap = list(bootstrap_complete = FALSE))), 0L)
  # A manifest that exposes no completeness signal, and no manifest at all,
  # are both honest-NA (unknown), not 0.
  expect_true(is.na(manifest_complete(list(summary = list(data_through = "2026-07-01")))))
  expect_true(is.na(manifest_complete(NULL)))
})

test_that("build surfaces db_bytes/db_sha256 and metrics-style completeness", {
  fetched <- list(
    # Metrics-style: code-manifest.json with bootstrap.bootstrap_complete=TRUE,
    # and integrity computed from the merged-in source DB.
    "cran-code-metrics" = list(
      cfg = list(name = "cran-code-metrics", repo = "r-observatory/cran-code-metrics",
                 schedule = "daily 04:00 UTC", max_age_h = 30L, rolling = FALSE,
                 manifest = TRUE, manifest_file = "code-manifest.json",
                 db_filename = "cran-code-metrics.db"),
      release = list(tag = "metrics-2026-07-14", published_at = "2026-07-14T05:00:00Z"),
      manifest = list(bootstrap = list(bootstrap_complete = TRUE)),
      upstream = NULL,
      integrity = list(bytes = 1162678272, sha256 = "b4c719d0deadbeef")),
    # No-manifest source: complete is honest-NA, but integrity still comes through.
    "cran-feed" = list(
      cfg = list(name = "cran-feed", repo = "r-observatory/cran-feed",
                 schedule = "every 6 hours", max_age_h = 8L, rolling = FALSE,
                 manifest = FALSE, db_filename = "feed.db"),
      release = list(tag = "v1", published_at = "2026-07-14T00:00:00Z"),
      manifest = NULL, upstream = NULL,
      integrity = list(bytes = 4096, sha256 = "abc123")))

  df <- build_pipeline_metadata(fetched, now_iso = "2026-07-14T08:00:00Z")

  m <- df[df$pipeline == "cran-code-metrics", ]
  expect_equal(m$complete, 1L)                 # bootstrap_complete=TRUE -> 1
  expect_equal(m$db_bytes, 1162678272)
  expect_equal(m$db_sha256, "b4c719d0deadbeef")

  f <- df[df$pipeline == "cran-feed", ]
  expect_true(is.na(f$complete))               # no manifest -> honest NA
  expect_equal(f$db_bytes, 4096)
  expect_equal(f$db_sha256, "abc123")
})

test_that("build yields honest NA integrity when a source contributed no file", {
  fetched <- list("cran-queue" = list(
    cfg = list(name = "cran-queue", repo = "r", schedule = "s", max_age_h = 3L,
               rolling = FALSE, manifest = FALSE, db_filename = "queue.db"),
    release = list(tag = "v1", published_at = "2026-07-14T00:00:00Z"),
    manifest = NULL, upstream = NULL,
    integrity = list(bytes = NA_real_, sha256 = NA_character_)))
  df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z")
  expect_true(is.na(df$db_bytes[1]))           # never a coerced 0
  expect_true(is.na(df$db_sha256[1]))
  expect_true(is.na(df$complete[1]))
})

test_that("write_pipeline_metadata persists integrity + completeness columns", {
  fetched <- list("cran-code-metrics" = list(
    cfg = list(name = "cran-code-metrics", repo = "r", schedule = "s", max_age_h = 30L,
               rolling = FALSE, manifest = TRUE, manifest_file = "code-manifest.json",
               db_filename = "cran-code-metrics.db"),
    release = list(tag = "metrics-2026-07-14", published_at = "x"),
    manifest = list(bootstrap = list(bootstrap_complete = TRUE)),
    upstream = NULL,
    integrity = list(bytes = 999, sha256 = "cafe")))
  df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z")
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  write_pipeline_metadata(con, df)
  got <- DBI::dbGetQuery(con, "SELECT db_bytes, db_sha256, complete FROM pipeline_metadata")
  expect_equal(got$db_bytes, 999)
  expect_equal(got$db_sha256, "cafe")
  expect_equal(got$complete, 1L)
})

test_that("build sets verified=1 when the manifest's declared sha matches the computed one", {
  fetched <- list("cran-downloads" = list(
    cfg = list(name = "cran-downloads", repo = "r-observatory/cran-downloads",
               schedule = "daily 07:00 UTC", max_age_h = 30L, rolling = TRUE,
               manifest = TRUE, db_filename = "downloads-summary.db"),
    release = list(tag = "current", published_at = "2026-07-14T00:00:00Z"),
    manifest = list(db_sha256 = "AbC123", complete = TRUE),
    upstream = NULL,
    # Computed hash differs only in case -> still a match (hex is case-insensitive).
    integrity = list(bytes = 4096, sha256 = "abc123")))
  df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z")
  expect_equal(df$verified[1], 1L)
})

test_that("build sets verified=0 AND emits a loud warning when declared != computed", {
  fetched <- list("cran-downloads" = list(
    cfg = list(name = "cran-downloads", repo = "r-observatory/cran-downloads",
               schedule = "daily 07:00 UTC", max_age_h = 30L, rolling = TRUE,
               manifest = TRUE, db_filename = "downloads-summary.db"),
    release = list(tag = "current", published_at = "2026-07-14T00:00:00Z"),
    manifest = list(db_sha256 = "declared-aaaa", complete = TRUE),
    upstream = NULL,
    integrity = list(bytes = 4096, sha256 = "computed-bbbb")))
  # The warning names the pipeline and both hashes so it is visible in the CI log.
  expect_message(
    df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z"),
    "cran-downloads.*declared-aaaa.*computed-bbbb")
  expect_equal(df$verified[1], 0L)
})

test_that("build leaves verified=NA when the manifest declares no sha (not-yet-republished / no manifest)", {
  fetched <- list(
    # Manifest present but without db_sha256 (a source mid-rollout).
    "cran-coverage" = list(
      cfg = list(name = "cran-coverage", repo = "r-observatory/cran-coverage",
                 schedule = "every 6 hours", max_age_h = 8L, rolling = TRUE,
                 manifest = TRUE, db_filename = "cran-coverage.db"),
      release = list(tag = "current", published_at = "2026-07-14T00:00:00Z"),
      manifest = list(processed = 941, remaining = 23000),
      upstream = NULL,
      integrity = list(bytes = 4096, sha256 = "abc123")),
    # No manifest at all: also honest-NA, not a mismatch.
    "cran-feed" = list(
      cfg = list(name = "cran-feed", repo = "r-observatory/cran-feed",
                 schedule = "every 6 hours", max_age_h = 8L, rolling = FALSE,
                 manifest = FALSE, db_filename = "feed.db"),
      release = list(tag = "v1", published_at = "2026-07-14T00:00:00Z"),
      manifest = NULL, upstream = NULL,
      integrity = list(bytes = 4096, sha256 = "abc123")))
  # No mismatch means no warning is emitted for either row.
  expect_no_message(df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z"))
  expect_true(is.na(df$verified[df$pipeline == "cran-coverage"]))
  expect_true(is.na(df$verified[df$pipeline == "cran-feed"]))
})

test_that("is_scalar_str accepts only a length-1, non-NA, non-empty character string", {
  expect_true(is_scalar_str("abc123"))
  expect_false(is_scalar_str(NULL))
  expect_false(is_scalar_str(list()))            # JSON [] via simplifyVector = FALSE
  expect_false(is_scalar_str(list("a", "b")))    # multi-element JSON array
  expect_false(is_scalar_str(list("abc")))       # single-element array is still not a scalar string
  expect_false(is_scalar_str(character(0)))
  expect_false(is_scalar_str(c("a", "b")))       # multi-element character vector
  expect_false(is_scalar_str(NA_character_))
  expect_false(is_scalar_str(""))
  expect_false(is_scalar_str(123))               # non-character scalar
})

test_that("compute_verified: the crash cases (list()/multi-element/empty string) degrade to NA, never error", {
  expect_true(is.na(compute_verified(list(), "abc123")))          # db_sha256: []
  expect_true(is.na(compute_verified(list("a", "b"), "abc123")))  # db_sha256: ["a","b"]
  expect_true(is.na(compute_verified("", "abc123")))              # db_sha256: ""
})

test_that("compute_verified: present-and-scalar matches/differs, and honest-NA when either side is unusable", {
  expect_equal(compute_verified("AbC123", "abc123"), 1L)   # case-insensitive match
  expect_equal(compute_verified("aaaa", "bbbb"), 0L)       # present-and-scalar, differs
  expect_true(is.na(compute_verified(NULL, "abc123")))     # no declared sha at all
  expect_true(is.na(compute_verified("abc123", NA_character_)))  # declared present, computed sha missing
  expect_true(is.na(compute_verified("abc123", NULL)))     # declared present, no computed sha
})

test_that("build degrades verified to NA (never errors) when a manifest emits a malformed db_sha256", {
  base_cfg <- list(name = "cran-downloads", repo = "r-observatory/cran-downloads",
                    schedule = "daily 07:00 UTC", max_age_h = 30L, rolling = TRUE,
                    manifest = TRUE, db_filename = "downloads-summary.db")
  make <- function(db_sha256) list("cran-downloads" = list(
    cfg = base_cfg,
    release = list(tag = "current", published_at = "2026-07-14T00:00:00Z"),
    manifest = list(db_sha256 = db_sha256, complete = TRUE),
    upstream = NULL,
    integrity = list(bytes = 4096, sha256 = "abc123")))

  # db_sha256: [] parsed with simplifyVector = FALSE -> list().
  expect_no_message(df <- build_pipeline_metadata(make(list()), "2026-07-14T08:00:00Z"))
  expect_true(is.na(df$verified[1]))

  # db_sha256: ["a", "b"] (multi-element array).
  expect_no_message(df <- build_pipeline_metadata(make(list("a", "b")), "2026-07-14T08:00:00Z"))
  expect_true(is.na(df$verified[1]))

  # db_sha256: "" (empty string).
  expect_no_message(df <- build_pipeline_metadata(make(""), "2026-07-14T08:00:00Z"))
  expect_true(is.na(df$verified[1]))
})

test_that("write_pipeline_metadata persists the verified column", {
  fetched <- list("cran-downloads" = list(
    cfg = list(name = "cran-downloads", repo = "r", schedule = "s", max_age_h = 30L,
               rolling = TRUE, manifest = TRUE, db_filename = "downloads-summary.db"),
    release = list(tag = "current", published_at = "x"),
    manifest = list(db_sha256 = "abc", complete = TRUE),
    upstream = NULL,
    integrity = list(bytes = 4096, sha256 = "abc")))
  df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z")
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  write_pipeline_metadata(con, df)
  got <- DBI::dbGetQuery(con, "SELECT verified FROM pipeline_metadata")
  expect_equal(got$verified, 1L)
})

test_that("pipeline_config includes the newly-folded-in sources with the merge's db_filenames", {
  cfgs <- pipeline_config()
  by_name <- setNames(cfgs, vapply(cfgs, function(c) c$name, character(1)))
  expected <- c(
    "bioconductor-downloads" = "bioconductor-summary.db",
    "bioconductor-metadata"  = "bioconductor-metadata.db",
    "cran-archive"           = "cran-archive.db",
    "cran-coverage"          = "cran-coverage.db",
    "cran-task-views"        = "cran-task-views.db")
  for (nm in names(expected)) {
    expect_true(nm %in% names(by_name), info = nm)
    entry <- by_name[[nm]]
    expect_equal(entry$db_filename, expected[[nm]], info = nm)
    expect_true(isTRUE(entry$rolling), info = nm)   # all use the rolling `current` tag
    expect_true(isTRUE(entry$manifest), info = nm)  # all publish a manifest.json
  }
})

test_that("build produces a row per config entry, including the new sources (honest-NA when unresolved)", {
  cfgs <- pipeline_config()
  # Minimal fetched skeleton for every configured pipeline (no manifest/integrity).
  fetched <- setNames(lapply(cfgs, function(cfg) list(
    cfg = cfg, release = NULL, manifest = NULL, upstream = NULL,
    integrity = list(bytes = NA_real_, sha256 = NA_character_))),
    vapply(cfgs, function(c) c$name, character(1)))
  df <- build_pipeline_metadata(fetched, "2026-07-14T08:00:00Z")
  expect_equal(nrow(df), length(cfgs))
  for (nm in c("bioconductor-downloads", "bioconductor-metadata",
               "cran-archive", "cran-coverage", "cran-task-views")) {
    row <- df[df$pipeline == nm, ]
    expect_equal(nrow(row), 1L, info = nm)
    expect_true(is.na(row$verified), info = nm)   # no declared sha yet -> honest NA
    expect_true(is.na(row$complete), info = nm)   # no completeness flag yet -> honest NA
  }
})

test_that("sha256_file/db_integrity: real file yields correct size+hash, missing file yields NA/NA", {
  # sha256_file() on an arbitrary path, checked against an independent (shell
  # utility) sha256 computation rather than re-deriving via the same code path.
  tmp <- tempfile()
  writeLines("pipeline metadata integrity test", tmp)
  on.exit(unlink(tmp), add = TRUE)

  sha_util <- Sys.which("sha256sum")
  independent_sha256 <- if (nzchar(sha_util)) {
    sub("\\s.*$", "", system2(sha_util, shQuote(tmp), stdout = TRUE)[1])
  } else {
    sha_util <- Sys.which("shasum")
    sub("\\s.*$", "", system2(sha_util, c("-a", "256", shQuote(tmp)), stdout = TRUE)[1])
  }
  skip_if(!nzchar(sha_util), "no sha256 utility available for an independent check")

  expect_equal(sha256_file(tmp), independent_sha256)

  # db_integrity(cfg) resolves sources/<db_filename> relative to the working
  # directory, so exercise it from a scratch dir with its own sources/.
  old_wd <- getwd()
  scratch <- tempfile("meta-io-")
  dir.create(file.path(scratch, "sources"), recursive = TRUE)
  setwd(scratch)
  on.exit({ setwd(old_wd); unlink(scratch, recursive = TRUE) }, add = TRUE)

  file.copy(tmp, file.path("sources", "present.db"))
  io <- default_meta_io()

  present <- io$db_integrity(list(db_filename = "present.db"))
  expect_equal(present$bytes, file.size(file.path("sources", "present.db")))
  expect_equal(present$sha256, independent_sha256)

  missing <- io$db_integrity(list(db_filename = "missing.db"))
  expect_true(is.na(missing$bytes))
  expect_true(is.na(missing$sha256))
})

test_that("build falls back to manifest generated_at when last_checked is absent", {
  # A rolling 'current' pipeline that clobbers its release (frozen published_at)
  # and does not commit each run: its manifest carries generated_at but no
  # last_checked/last_changed. Freshness must track generated_at, not the stale
  # release/commit time. (cran-coverage was showing 2-day-stale for this reason.)
  fetched <- list(
    "cran-coverage" = list(
      cfg = list(name = "cran-coverage", repo = "r-observatory/cran-coverage",
                 schedule = "every 6 hours", max_age_h = 8L, manifest = TRUE),
      release     = list(tag = "current", published_at = "2026-07-04T23:24:36Z"),
      repo_commit = "2026-07-16T01:13:48Z",
      manifest    = list(generated_at = "2026-07-18T06:28:34Z"),
      upstream    = NULL
    )
  )
  df  <- build_pipeline_metadata(fetched, now_iso = "2026-07-18T08:00:00Z")
  row <- df[df$pipeline == "cran-coverage", ]
  expect_equal(row$last_checked, "2026-07-18T06:28:34Z")
  expect_equal(row$last_changed, "2026-07-18T06:28:34Z")
})
