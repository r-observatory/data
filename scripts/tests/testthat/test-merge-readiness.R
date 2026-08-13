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

NOW <- "2026-08-05T19:00:00Z"   # evening: past the 18:00 hold, before the 22:00 deadline

# The merge job's body, its `if:` condition, and its first step. Several rules
# below are about which of the three a guarantee lives in, so reading the whole
# workflow as one blob would let a guard drift into a place it cannot help.
merge_job <- function(wf) sub("(?s)^.*\n  merge:\n", "", wf, perl = TRUE)

merge_job_if <- function(wf) {
  cond <- sub("(?s)^.*?\n    if: >-\n", "", merge_job(wf), perl = TRUE)
  sub("(?s)\n    runs-on:.*$", "", cond, perl = TRUE)
}

merge_job_first_step <- function(wf) {
  lines <- strsplit(sub("(?s)^.*?\n    steps:\n", "", merge_job(wf), perl = TRUE), "\n")[[1]]
  heads <- grep("^      - (name|uses):", lines)
  paste(lines[seq.int(heads[1], heads[2] - 1L)], collapse = "\n")
}

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
                          now_iso = "2026-08-05T22:00:00Z")
  expect_true(late$should_merge)
  expect_false(late$ready)
  expect_equal(late$not_ready, "vcs-signals")
  expect_match(late$reason, "past 22:00 UTC")
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
  expect_match(res$reason, "no daily source has published since the last merge")
})

test_that("an intraday source ticking is not a reason to merge and redeploy", {
  # cran-queue is hourly and cran-coverage every six hours. Triggering on those
  # would rebuild and redeploy the site several times a day for data that is
  # already only minutes old.
  meta <- mk(list(name = "cran-queue",    max_age = 3L, at = "2026-08-05T11:55:00Z"),
             list(name = "cran-coverage", max_age = 8L, at = "2026-08-05T11:40:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-05T09:00:00Z", now_iso = NOW)
  expect_false(res$should_merge)
  expect_match(res$reason, "no daily source has published since the last merge")
})

test_that("a once-daily source publishing new data merges without waiting a day", {
  # The case that stranded uGMAR 3.6.1: the merge ran at 02:17 from metrics
  # computed before the version existed, the day's metrics landed at 06:48, and a
  # 20h floor then blocked publishing them until 22:17. New data we already hold
  # must not sit unpublished for the rest of the day.
  meta <- mk(list(name = "cran-code-metrics", max_age = 30L, at = "2026-08-05T06:48:00Z"),
             list(name = "cran-queue",        max_age = 3L,  at = "2026-08-05T11:55:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-05T02:17:00Z", now_iso = NOW)
  expect_true(res$should_merge)
  expect_true("cran-code-metrics" %in% res$unmerged)
  expect_false("cran-queue" %in% res$unmerged)
})

test_that("a once-daily source does not trigger a second merge the same day", {
  # Cadence still lands near one merge a day, but because once-daily sources
  # publish once, not because a timer forbids the second one.
  meta <- mk(list(name = "cran-code-metrics", max_age = 30L, at = "2026-08-05T06:48:00Z"),
             list(name = "cran-queue",        max_age = 3L,  at = "2026-08-05T11:55:00Z"))
  # Already merged after metrics landed: nothing daily is newer than that merge.
  res <- merge_readiness(meta, last_merge_at = "2026-08-05T07:30:00Z", now_iso = NOW)
  expect_false(res$should_merge)
  expect_equal(res$unmerged, character(0))
})

test_that("the merge holds until the day's later metrics runs have happened", {
  # The site deploys on every merge and that deploy is billed, so there is one
  # merge a day. Taking it as soon as the set first looks ready would capture the
  # 04:00 code-metrics run and miss the 10:00 and 16:00 ones, which is what left
  # writexl 2.0.0 (released 13:50) invisible until the next day.
  meta <- mk(list(name = "cran-code-metrics", max_age = 30L, at = "2026-08-05T04:30:00Z"))
  morning <- merge_readiness(meta, last_merge_at = "2026-08-04T18:30:00Z",
                             now_iso = "2026-08-05T09:00:00Z")
  expect_false(morning$should_merge)
  expect_true(morning$ready)          # ready, and deliberately still waiting
  expect_match(morning$reason, "holding until 18:00 UTC")

  evening <- merge_readiness(meta, last_merge_at = "2026-08-04T18:30:00Z",
                             now_iso = "2026-08-05T18:30:00Z")
  expect_true(evening$should_merge)
})

test_that("the deadline stays reachable after the earliest hour", {
  # A deadline at or before the earliest hour could never fire, so a stuck
  # pipeline would hold the site indefinitely instead of being published around.
  expect_gt(readiness_deadline_hour(), readiness_earliest_hour())

  # And past the deadline it publishes even with a source still missing.
  meta <- mk(list(name = "cran-code-metrics", max_age = 30L, at = "2026-08-05T16:10:00Z"),
             list(name = "vcs-signals",       max_age = 30L, at = "2026-08-04T10:00:00Z"))
  late <- merge_readiness(meta, last_merge_at = "2026-08-04T18:30:00Z",
                          now_iso = "2026-08-05T22:30:00Z")
  expect_true(late$should_merge)
  expect_equal(late$not_ready, "vcs-signals")
})

test_that("the floor is a loop guard, not a publishing delay", {
  # It must be short enough that it never becomes the reason real data waits.
  expect_lte(readiness_cooldown_h(), 2)
  # New daily data exists, but we merged five minutes ago: the guard holds briefly.
  meta <- mk(list(name = "cran-code-metrics", max_age = 30L, at = "2026-08-05T18:56:00Z"))
  res <- merge_readiness(meta, last_merge_at = "2026-08-05T18:55:00Z", now_iso = NOW)
  expect_false(res$should_merge)
  expect_match(res$reason, "floor")

  # And it clears in an hour, rather than parking the data until tomorrow.
  soon <- merge_readiness(meta, last_merge_at = "2026-08-05T17:30:00Z", now_iso = NOW)
  expect_true(soon$should_merge)
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
  expect_true(grepl("needs.readiness.outputs.should_merge == 'true'",
                    merge_job_if(wf), fixed = TRUE),
              info = "a MERGE verdict starts the merge job")

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

test_that("a readiness check that cannot answer does not freeze the site", {
  # Gating the merge on this check hands it the power to stop the site updating
  # at all, and quietly: every run green, no merge, stale data. Deciding not to
  # merge and being unable to decide must not look the same to the merge job.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_true(grepl("continue-on-error: true", wf, fixed = TRUE),
              info = "a crashing check does not fail the job outright")
  expect_true(grepl("steps.check.outputs.should_merge || steps.fallback.outputs.should_merge",
                    wf, fixed = TRUE),
              info = "the job output falls through to the fallback")
  # The fallback must not need the source metadata the check just failed to get.
  fb <- sub("(?s)^.*Fall back when readiness could not be determined", "", wf, perl = TRUE)
  fb <- sub("(?s)\n  merge:.*$", "", fb, perl = TRUE)
  expect_false(grepl("check-readiness.R", fb, fixed = TRUE),
               info = "the fallback does not re-run the thing that failed")
  expect_true(grepl("should_merge=true", fb, fixed = TRUE),
              info = "the fallback can still decide to merge")
})

test_that("a readiness JOB that dies does not skip the day either", {
  # The previous test covers the readiness CHECK failing. The job itself dying
  # is the same event one level up, and the fallback that answers it lived
  # inside that job, so it died too: on 2026-08-06 readiness never got a runner,
  # recorded zero steps, was cancelled by its own timeout, and the merge job
  # read an empty output and skipped. No database was published that day and
  # every job in the run reported green or skipped, so nothing surfaced it.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")
  cond <- merge_job_if(wf)

  expect_true(grepl("needs.readiness.result == 'cancelled'", cond, fixed = TRUE),
              info = "a cancelled readiness job still starts the merge job")
  expect_true(grepl("needs.readiness.result == 'failure'", cond, fixed = TRUE),
              info = "a failed readiness job still starts the merge job")

  # `always()` would also run the merge job when somebody cancels the run, and
  # publish a release out from under them. `!cancelled()` runs after a readiness
  # job that its own timeout cancelled but stops on a cancelled run.
  expect_true(grepl("!cancelled()", cond, fixed = TRUE),
              info = "a run cancelled by hand does not publish anyway")
  expect_false(grepl("always()", cond, fixed = TRUE))
})

test_that("the merge job re-decides on age when it arrives without an answer", {
  # Reaching the merge job no longer means readiness approved the merge, so the
  # job must not simply merge. It applies the same rule the dead step would
  # have: time since the last release, which needs no source metadata and no
  # readiness job.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")
  guard <- merge_job_first_step(wf)

  expect_true(grepl("MERGE_FALLBACK_AGE_H", guard, fixed = TRUE),
              info = "the first step of the merge job is the age guard")
  expect_true(grepl("gh release view", guard, fixed = TRUE),
              info = "the guard reads the age of the last published release")
  expect_false(grepl("check-readiness.R", guard, fixed = TRUE),
              info = "the guard does not re-run the thing that just died")
  # Cheap, and before the checkout: a run that must not merge should not pay for
  # an R toolchain and twenty database downloads to find that out.
  expect_false(grepl("actions/checkout", guard, fixed = TRUE))

  # Under the floor the job stops without merging and without failing, which is
  # a verdict the rest of the steps have to be gated on.
  expect_true(grepl("proceed=false", guard, fixed = TRUE))
  expect_true(grepl("steps.gate.outputs.proceed == 'true'", merge_job(wf), fixed = TRUE))
})

test_that("the fallback age floor is written down once", {
  # Two copies of a number that has to agree is one copy too many: the step
  # inside the readiness job and the guard in the merge job are the same rule
  # applied at two levels, and a run that merges by one and not the other would
  # be very hard to read.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_true(grepl('MERGE_FALLBACK_AGE_H: "20"', wf, fixed = TRUE),
              info = "the floor is defined once, at workflow level")
  expect_false(grepl('-ge 20 ', wf, fixed = TRUE),
               info = "no step hardcodes a second copy of the number")
})

test_that("a readiness outage is reported rather than passed over in green", {
  # The incident cost two days because nothing in the run said anything. Losing
  # readiness is not a state anyone chose, so it reddens the run and reaches the
  # existing failure thread instead of getting its own mechanism.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")

  expect_true(grepl("Fail the run when readiness could not be determined", wf, fixed = TRUE))
  expect_true(grepl("READINESS_NOTE", merge_job(wf), fixed = TRUE),
              info = "what happened is carried into the failure report")
  # Anchored so the "gh issue create rejects an unknown label" comment above the
  # label call is not counted as a second notification path.
  expect_equal(length(grep("^ *gh issue create", strsplit(wf, "\n")[[1]])), 1L,
               info = "one notification mechanism, not two")
})

test_that("a WAIT verdict is not overturned by the age floor", {
  # The merge job can now be reached without an approval, so the guard has to
  # tell "nobody decided" from "somebody decided to wait". A readiness job that
  # answered and then failed for its own reasons still answered, and reading
  # that as no answer would let time-since-last-release overrule the readiness
  # decision this workflow exists to respect.
  path <- file.path("..", "..", "..", ".github", "workflows", "merge.yml")
  if (!file.exists(path)) skip("workflow not reachable from the test directory")
  wf <- paste(readLines(path, warn = FALSE), collapse = "\n")
  guard <- merge_job_first_step(wf)

  expect_true(grepl('"$READINESS_ANSWER" = "false"', guard, fixed = TRUE),
              info = "an explicit WAIT short-circuits before the age fallback")
  # The age lookup must sit after both explicit verdicts, or it would run first
  # and decide for them.
  expect_lt(max(gregexpr('READINESS_ANSWER" = "', guard, fixed = TRUE)[[1]]),
            regexpr("gh release view", guard, fixed = TRUE))
})
