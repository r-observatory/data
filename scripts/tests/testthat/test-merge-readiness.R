# The merge waits for its sources instead of guessing an hour.

source(file.path("..", "..", "merge_gate.R"), chdir = FALSE)
source(file.path("..", "..", "merge_readiness.R"), chdir = FALSE)

# One row per pipeline, shaped like build_pipeline_metadata's output.
mk <- function(...) {
  rows <- list(...)
  do.call(rbind, lapply(rows, function(r) data.frame(
    pipeline = r$name,
    expected_max_age_hours = r$max_age %||% 30L,
    last_checked = r$at %||% NA_character_,
    last_changed = NA_character_,
    released_at  = r$released %||% NA_character_,
    stringsAsFactors = FALSE)))
}

NOW <- "2026-08-05T12:00:00Z"   # midday, before the 14:00 deadline

test_that("the merge runs once every daily source has published today", {
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"),
             list(name = "vcs-signals",    at = "2026-08-05T10:54:00Z"),
             list(name = "data",           at = NOW))
  res <- merge_readiness(meta, last_merge_at = "2026-08-04T10:44:00Z", now_iso = NOW)
  expect_true(res$should_merge)
  expect_true(res$ready)
  expect_equal(res$not_ready, character(0))
  # The merger's own row must not be treated as a source it waits for.
  expect_false("data" %in% res$daily_sources)
})

test_that("the merge waits when a daily source has not published today", {
  # This is the case that was silently losing a day of data: the merge fired
  # while vcs-signals was still running and took yesterday's copy.
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"),
             list(name = "vcs-signals",    at = "2026-08-04T10:54:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-04T10:44:00Z", now_iso = NOW)
  expect_false(res$should_merge)
  expect_false(res$ready)
  expect_equal(res$not_ready, "vcs-signals")
  expect_match(res$reason, "waiting on vcs-signals")
})

test_that("waiting is bounded, and what was late is named rather than passed over", {
  # A pipeline that breaks must not freeze the site. Past the deadline the
  # merge publishes what it has, and says what it did not have.
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"),
             list(name = "vcs-signals",    at = "2026-08-04T10:54:00Z"))
  late <- merge_readiness(meta, last_merge_at = "2026-08-04T10:44:00Z",
                          now_iso = "2026-08-05T14:00:00Z")
  expect_true(late$should_merge)
  expect_false(late$ready)
  expect_equal(late$not_ready, "vcs-signals")
  expect_match(late$reason, "past 14:00 UTC")
})

test_that("a source that is not expected daily never holds the merge back", {
  # c2d4u is monthly and r2u declares 35 days. Requiring them to publish today
  # would mean the merge never runs.
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"),
             list(name = "c2d4u-downloads", max_age = 45L * 24L,
                  at = "2026-07-03T05:45:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-04T10:44:00Z", now_iso = NOW)
  expect_true(res$should_merge)
  expect_false("c2d4u-downloads" %in% res$not_ready)
})

test_that("a source whose timestamp cannot be read counts as not ready", {
  # Unknown is not the same as published. Reading an unparseable timestamp as
  # ready is how a source silently drops out of the freshness decision.
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"),
             list(name = "vcs-signals",    at = "not a timestamp"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-04T10:44:00Z", now_iso = NOW)
  expect_false(res$should_merge)
  expect_equal(res$not_ready, "vcs-signals")
})

test_that("nothing new since the last merge means nothing to publish", {
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"),
             list(name = "vcs-signals",    at = "2026-08-05T10:54:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-05T11:30:00Z", now_iso = NOW)
  expect_false(res$should_merge)
  expect_match(res$reason, "no source has published since the last merge")
})

test_that("the hourly check does not merge once per fast source", {
  # cran-queue is hourly and cran-coverage every six hours. Without a floor the
  # hourly readiness check would publish, and redeploy the site, on each one.
  meta <- mk(list(name = "cran-queue",    max_age = 3L,  at = "2026-08-05T11:55:00Z"),
             list(name = "cran-coverage", max_age = 8L,  at = "2026-08-05T11:40:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-05T09:00:00Z", now_iso = NOW,
                         cooldown_h = 6)
  expect_false(res$should_merge)
  expect_match(res$reason, "under the 6h floor")

  # And it does merge again once the floor has passed.
  later <- merge_readiness(meta, last_merge_at = "2026-08-05T04:00:00Z", now_iso = NOW,
                           cooldown_h = 6)
  expect_true(later$should_merge)
})

test_that("the shipped cadence stays one merge a day, only better timed", {
  # The point of this change is WHEN the merge runs, not how often. Four merges
  # a day would mean four site deploys a day, which is a separate decision.
  meta <- mk(list(name = "cran-queue", max_age = 3L, at = "2026-08-05T11:55:00Z"))
  # A merge earlier the same morning must not be followed by another at midday.
  same_day <- merge_readiness(meta, last_merge_at = "2026-08-05T02:17:00Z", now_iso = NOW)
  expect_false(same_day$should_merge)
  expect_match(same_day$reason, "under the 20h floor")

  # A merge at yesterday's usual time does not block today's.
  next_day <- merge_readiness(meta, last_merge_at = "2026-08-04T13:00:00Z", now_iso = NOW)
  expect_true(next_day$should_merge)
})

test_that("the first ever merge is not blocked by having no previous merge", {
  meta <- mk(list(name = "cran-downloads", at = "2026-08-05T11:09:00Z"))
  res <- merge_readiness(meta, last_merge_at = NA_character_, now_iso = NOW)
  expect_true(res$should_merge)
})

test_that("a publish time is read in its stated zone, not as if it were UTC", {
  # A source stamping "+02:00" or "-04:00" was being parsed as UTC, shifting it
  # by hours and, across a day boundary, changing whether it counted as today.
  expect_equal(gate_parse_time("2026-08-05T04:00:00+02:00"),
               gate_parse_time("2026-08-05T02:00:00Z"))
  expect_equal(gate_parse_time("2026-08-04T22:00:00-04:00"),
               gate_parse_time("2026-08-05T02:00:00Z"))
  expect_equal(gate_parse_time("2025-07-09T12:22:43.000+02:00"),
               gate_parse_time("2025-07-09T10:22:43Z"))
  # Still NA for shapes we genuinely cannot read.
  expect_true(is.na(gate_parse_time("2026-08-05T0200+0000")))
  expect_true(is.na(gate_parse_time("not a time")))

  # And the fix must reach the readiness decision, not only the parser: a
  # source stamping 22:00-04:00 published at 02:00 UTC TODAY.
  meta <- mk(list(name = "vcs-signals", at = "2026-08-04T22:00:00-04:00"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-04T10:00:00Z", now_iso = NOW)
  expect_true(res$should_merge)
  expect_equal(res$not_ready, character(0))
})

test_that("the workflow actually consults readiness before merging", {
  # The decision is only worth computing if the merge is gated on it.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_true(grepl("needs: readiness", wf, fixed = TRUE))
  expect_true(grepl("if: needs.readiness.outputs.should_merge == 'true'", wf, fixed = TRUE),
              info = "the merge job runs only on a MERGE verdict")

  # A fixed daily hour is the thing being replaced. Hourly plus the readiness
  # gate is what makes the merge land after the pipelines rather than among
  # them, so a cron pinned back to one hour would silently restore the race.
  expect_true(grepl('cron: "0 \\* \\* \\* \\*"', wf),
              info = "the schedule is hourly, with readiness deciding")

  # The readiness job must stay cheap, or 24 runs a day stops being free: it
  # exists to answer one question before anything is downloaded.
  readiness_block <- sub("(?s)^.*?  readiness:", "", wf, perl = TRUE)
  readiness_block <- sub("(?s)\n  merge:.*$", "", readiness_block, perl = TRUE)
  expect_false(grepl("observatory.db", readiness_block, fixed = TRUE),
               info = "readiness does not touch the merged database")
  expect_true(grepl("check-readiness.R", readiness_block, fixed = TRUE))
})
