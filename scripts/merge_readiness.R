# merge_readiness.R — decide whether the sources are ready to be merged.
#
# The merge used to run on a fixed cron and take whatever was published at that
# moment. Both its cron and the pipelines' crons drift by hours on a busy
# runner, so "08:00" actually fired between 10:02 and 11:33, while vcs-signals
# finished between 09:51 and 11:45. The two landed within minutes of each other
# and the merge won four days in five, publishing a database that was missing
# that day's vcs-signals, cran-downloads, bioconductor-metadata and
# cran-coverage. The freshness gate could not see it: it judges a source stale
# only at 2x its declared max_age_h floored at 12h, so a source one day behind
# is comfortably inside the window, and a source that is merely stale publishes
# anyway by design.
#
# Ordering by clock cannot fix this, because there is no hour that is reliably
# after every pipeline. So the merge stops guessing and asks instead: has each
# source that is supposed to publish daily actually published today?
#
# The reference timestamp per source is the same chain the freshness gate uses
# (last_checked, then last_changed, then released_at). Two definitions of "when
# did this publish" would eventually disagree, and the one nobody is reading
# would be the wrong one.

`%||%` <- function(a, b) if (is.null(a)) b else a

# --------------------------------------------------------------------------
# Tunables
# --------------------------------------------------------------------------

#' A source is expected to publish at least daily when it declares a staleness
#' threshold at or under this many hours. Only these can hold the merge back.
#' c2d4u (monthly) and r2u (35 days) declare far more and so never block.
readiness_daily_max_age_h <- function() 30

#' Hour of the UTC day before which the merge does not run, even once every
#' source is ready.
#'
#' The site deploys on every merge and that deploy is billed, so there is one
#' merge a day and it has to be the one that captures the most. Sources publish
#' across the morning and code metrics runs at 04, 10 and 16 UTC; merging as soon
#' as the set first looks ready would take the 04:00 metrics and miss two later
#' runs, leaving a package released at midday invisible until tomorrow.
readiness_earliest_hour <- function() 18L

#' Hour of the UTC day after which the merge stops waiting and publishes what it
#' has. A pipeline that breaks must not be able to freeze the whole site, so
#' waiting is always bounded. Late sources are named rather than passed over.
#' Must sit after readiness_earliest_hour, or it would never be reachable.
readiness_deadline_hour <- function() 22L

#' Below this declared threshold a source publishes many times a day rather than
#' once: cran-queue declares 3 (hourly), cran-feed and cran-coverage 8 (every six
#' hours). Everything genuinely daily declares 30.
#'
#' The distinction is what decides when to merge. A source publishing today is
#' what makes the set READY. A once-daily source publishing something we have not
#' merged is what makes a merge WORTH DOING. An hourly tick from cran-queue is
#' neither new information about the day nor a reason to rebuild and redeploy.
readiness_intraday_max_age_h <- function() 12

#' Safety floor only, to bound a pathological loop. It is deliberately short:
#' once-daily sources publish once, so they cannot trigger repeated merges on
#' their own, and a long floor blocks real data instead.
#'
#' A 20h floor did exactly that. A merge that ran before the day's metrics were
#' computed could not be followed by one that included them, so a new CRAN
#' version sat computed-but-unpublished for the rest of the day.
readiness_cooldown_h <- function() 1

# --------------------------------------------------------------------------
# Pure decision
# --------------------------------------------------------------------------

#' The UTC day boundary at or before `now_iso`, in seconds since epoch.
readiness_day_start <- function(now_iso) {
  n <- gate_parse_time(now_iso)
  if (is.na(n)) return(NA_real_)
  as.numeric(as.POSIXct(format(as.POSIXct(n, origin = "1970-01-01", tz = "UTC"),
                               "%Y-%m-%d 00:00:00"), tz = "UTC"))
}

#' Whether the merge should run now, and why.
#'
#' @param meta the data.frame from build_pipeline_metadata: one row per pipeline
#'   with pipeline, expected_max_age_hours, last_checked, last_changed,
#'   released_at.
#' @param last_merge_at ISO time of the most recent merge, or NA when none.
#' @param now_iso current time.
#' @return list(should_merge, reason, ready, not_ready, newest_source_at, ...)
merge_readiness <- function(meta, last_merge_at, now_iso,
                            daily_max_age_h    = readiness_daily_max_age_h(),
                            intraday_max_age_h = readiness_intraday_max_age_h(),
                            earliest_hour      = readiness_earliest_hour(),
                            deadline_hour      = readiness_deadline_hour(),
                            cooldown_h         = readiness_cooldown_h(),
                            self_name          = "data") {
  now_s   <- gate_parse_time(now_iso)
  day0    <- readiness_day_start(now_iso)
  hour    <- if (is.na(now_s)) NA_integer_ else
    as.integer(format(as.POSIXct(now_s, origin = "1970-01-01", tz = "UTC"), "%H"))

  rows <- if (is.null(meta) || nrow(meta) == 0) meta[0, , drop = FALSE] else
    meta[meta$pipeline != self_name, , drop = FALSE]

  ref_of <- function(i) gate_first_present(list(rows$last_checked[i],
                                                rows$last_changed[i],
                                                rows$released_at[i]))
  refs <- if (nrow(rows) == 0) character(0) else
    vapply(seq_len(nrow(rows)), ref_of, character(1))
  ref_s <- vapply(refs, gate_parse_time, numeric(1), USE.NAMES = FALSE)

  # Only the daily-or-faster sources can hold the merge back.
  age_h <- suppressWarnings(as.numeric(rows$expected_max_age_hours))
  is_daily <- !is.na(age_h) & age_h <= daily_max_age_h

  # A source whose timestamp we cannot read is NOT ready. Unknown is not the
  # same as published, and treating it as ready is how a silent gap gets in.
  published_today <- !is.na(ref_s) & !is.na(day0) & ref_s >= day0
  not_ready <- as.character(rows$pipeline[is_daily & !published_today])
  ready <- length(not_ready) == 0 && any(is_daily)

  newest_s <- if (all(is.na(ref_s))) NA_real_ else max(ref_s, na.rm = TRUE)
  newest_at <- if (is.na(newest_s)) NA_character_ else
    format(as.POSIXct(newest_s, origin = "1970-01-01", tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")

  last_s <- gate_parse_time(last_merge_at)

  # What makes a merge worth doing is a ONCE-DAILY source carrying something we
  # have not published. Triggering on any source would mean merging, and
  # redeploying the site, every time cran-queue ticks. Triggering on none of them
  # was worse: a merge that landed before the day's metrics were computed could
  # not be followed by one that included them.
  is_once_daily <- is_daily & !is.na(age_h) & age_h >= intraday_max_age_h
  fresh_daily_s <- ref_s[is_once_daily]
  fresh_daily_s <- fresh_daily_s[!is.na(fresh_daily_s)]
  newest_daily_s <- if (length(fresh_daily_s) == 0) NA_real_ else max(fresh_daily_s)
  trigger <- is.na(last_s) || (!is.na(newest_daily_s) && newest_daily_s > last_s)
  unmerged <- as.character(rows$pipeline[is_once_daily & !is.na(ref_s) &
                                         !is.na(last_s) & ref_s > last_s])

  since_h <- if (is.na(last_s) || is.na(now_s)) NA_real_ else (now_s - last_s) / 3600
  cooldown_ok <- is.na(since_h) || since_h >= cooldown_h
  past_deadline <- !is.na(hour) && hour >= deadline_hour
  # An unreadable `now` leaves this permissive, though nothing rests on that:
  # the same failure makes day0 NA, so no source counts as published today and
  # `ready` is already FALSE. Kept so this line states a rule about the hour
  # rather than an accident of how a bad clock happens to fail elsewhere.
  past_earliest <- is.na(hour) || hour >= earliest_hour

  should <- trigger && cooldown_ok && past_earliest && (ready || past_deadline)

  reason <- if (!trigger) {
    "no daily source has published since the last merge"
  } else if (!cooldown_ok) {
    sprintf("last merge was %.1fh ago, under the %gh floor", since_h, cooldown_h)
  } else if (!past_earliest) {
    sprintf("holding until %02d:00 UTC so the day's later runs are included",
            earliest_hour)
  } else if (ready) {
    "every daily source has published today"
  } else if (past_deadline) {
    sprintf("past %02d:00 UTC and still waiting on %s",
            deadline_hour, paste(not_ready, collapse = ", "))
  } else {
    sprintf("waiting on %s", paste(not_ready, collapse = ", "))
  }

  list(should_merge = should, reason = reason, ready = ready,
       not_ready = not_ready, newest_source_at = newest_at,
       hours_since_merge = since_h, past_deadline = past_deadline,
       unmerged = unmerged,
       daily_sources = as.character(rows$pipeline[is_daily]),
       once_daily_sources = as.character(rows$pipeline[is_once_daily]))
}

#' Human-readable table for the run summary, so a skipped merge explains itself.
format_readiness <- function(res, now_iso) {
  c(sprintf("Merge readiness at %s", now_iso),
    sprintf("  decision: %s (%s)",
            if (isTRUE(res$should_merge)) "MERGE" else "WAIT", res$reason),
    sprintf("  daily sources: %d, waiting on: %s",
            length(res$daily_sources),
            if (length(res$not_ready)) paste(res$not_ready, collapse = ", ") else "none"),
    sprintf("  published since our last merge: %s",
            if (length(res$unmerged)) paste(res$unmerged, collapse = ", ") else "none"),
    sprintf("  newest source publish: %s", res$newest_source_at %||% "unknown"),
    sprintf("  hours since last merge: %s",
            if (is.na(res$hours_since_merge)) "no previous merge"
            else sprintf("%.1f", res$hours_since_merge)))
}
