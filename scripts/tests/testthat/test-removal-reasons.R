# Removal events take CRAN's own reason from cran_archive_history. merge.R only
# warns when the enrichment fails, so these tests are its only guard.
testthat::local_edition(3)
source(file.path(getwd(), "..", "..", "merge_helpers.R"))

placeholder <- "no longer on CRAN"

# package_versions as cran-feed declares it, and cran_archive_history as
# cran-archive does. `episodes = NULL` leaves the history table out.
removal_db <- function(events, episodes = NULL) {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  DBI::dbExecute(con, "CREATE TABLE package_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT, package TEXT NOT NULL, version TEXT,
    event_type TEXT NOT NULL, previous_version TEXT, removal_reason TEXT,
    detected_at TEXT NOT NULL, published TEXT)")
  events$version <- events$version %||% "1.0.0"
  DBI::dbAppendTable(con, "package_versions", events)
  if (!is.null(episodes)) {
    DBI::dbExecute(con, "CREATE TABLE cran_archive_history (
      package TEXT NOT NULL, episode_seq INTEGER NOT NULL, archived_on TEXT NOT NULL,
      relisted_on TEXT, removal_reason TEXT, last_version TEXT, relist_source TEXT,
      archived_on_source TEXT, source TEXT NOT NULL DEFAULT 'cran-archive-rds',
      updated_at TEXT NOT NULL, PRIMARY KEY (package, episode_seq),
      CHECK (relisted_on IS NULL OR relisted_on >= archived_on))")
    episodes$updated_at <- "2026-09-30T05:10:00Z"
    DBI::dbAppendTable(con, "cran_archive_history", episodes)
  }
  con
}

removed <- function(package, detected_at, reason = placeholder) {
  data.frame(package = package, event_type = "removed", removal_reason = reason,
             detected_at = detected_at, stringsAsFactors = FALSE)
}

episode <- function(package, episode_seq, archived_on, reason, relisted_on = NA_character_) {
  data.frame(package = package, episode_seq = episode_seq, archived_on = archived_on,
             relisted_on = relisted_on, removal_reason = reason, stringsAsFactors = FALSE)
}

reasons <- function(con) {
  DBI::dbGetQuery(con, "SELECT package, detected_at, removal_reason FROM package_versions
                        ORDER BY id")
}

test_that("a same-day episode replaces the placeholder", {
  con <- removal_db(removed("tsxtreme", "2026-09-27T20:37:57Z"),
                    episode("tsxtreme", 1L, "2026-09-27", "issues were not corrected in time"))
  on.exit(DBI::dbDisconnect(con))

  expect_equal(enrich_removal_reasons(con), 1L)
  expect_equal(reasons(con)$removal_reason, "issues were not corrected in time")
})

test_that("an episode up to seven days away matches and one eight days away does not", {
  con <- removal_db(
    rbind(removed("behaviorchange", "2026-08-24T06:01:10Z"),   # archived 4 days later
          removed("sevenday", "2026-08-21T12:00:00Z"),         # exactly 7 days
          removed("eightday", "2026-08-20T23:59:59Z")),        # 8 days
    rbind(episode("behaviorchange", 1L, "2026-08-28", "Maintainer did not respond"),
          episode("sevenday", 1L, "2026-08-28", "issues were not corrected in time"),
          episode("eightday", 1L, "2026-08-28", "check errors")))
  on.exit(DBI::dbDisconnect(con))

  expect_equal(enrich_removal_reasons(con), 2L)
  expect_equal(reasons(con)$removal_reason,
               c("Maintainer did not respond", "issues were not corrected in time", placeholder))
})

test_that("the nearer of two episodes wins, whichever side of the event it falls", {
  con <- removal_db(removed("nearpkg", "2026-06-10T08:00:00Z"),
                    rbind(episode("nearpkg", 1L, "2026-06-05", "older reason",
                                  relisted_on = "2026-06-08"),
                          episode("nearpkg", 2L, "2026-06-12", "nearer reason")))
  on.exit(DBI::dbDisconnect(con))

  enrich_removal_reasons(con)
  expect_equal(reasons(con)$removal_reason, "nearer reason")
})

test_that("an episode with an empty or blank reason is passed over", {
  # SQLite's one-argument TRIM strips spaces only, so a tab or a newline would
  # otherwise count as a reason and blank out the placeholder.
  con <- removal_db(
    rbind(removed("blankpkg", "2026-07-01T00:00:00Z"),
          removed("tabpkg", "2026-07-01T00:00:00Z"),
          removed("fallback", "2026-07-01T00:00:00Z")),
    rbind(episode("blankpkg", 1L, "2026-07-01", ""),
          episode("tabpkg", 1L, "2026-07-01", " \t\r\n "),
          episode("fallback", 1L, "2026-07-01", NA_character_),
          episode("fallback", 2L, "2026-07-05", "the farther episode has the reason")))
  on.exit(DBI::dbDisconnect(con))

  expect_equal(enrich_removal_reasons(con), 1L)
  expect_equal(reasons(con)$removal_reason,
               c(placeholder, placeholder, "the farther episode has the reason"))
})

test_that("an archive date that does not parse is passed over rather than matched", {
  con <- removal_db(
    rbind(removed("baddate", "2026-07-01T00:00:00Z"),
          removed("badevent", "not a date")),
    rbind(episode("baddate", 1L, "unknown", "a reason that cannot be placed"),
          episode("badevent", 1L, "2026-07-01", "a reason for an unplaceable event")))
  on.exit(DBI::dbDisconnect(con))

  expect_no_error(n <- enrich_removal_reasons(con))
  expect_equal(n, 0L)
  expect_equal(reasons(con)$removal_reason, c(placeholder, placeholder))
})

test_that("an event with no episode near it keeps cran-feed's placeholder", {
  con <- removal_db(rbind(removed("farpkg", "2026-01-15T00:00:00Z"),
                          removed("nohistory", "2026-01-15T00:00:00Z")),
                    episode("farpkg", 1L, "2025-11-01", "long before"))
  on.exit(DBI::dbDisconnect(con))

  expect_equal(enrich_removal_reasons(con), 0L)
  expect_equal(reasons(con)$removal_reason, c(placeholder, placeholder))
})

test_that("each removal of a twice-removed package gets its own episode's reason", {
  con <- removal_db(rbind(removed("twice", "2025-01-10T09:00:00Z"),
                          removed("twice", "2026-03-02T09:00:00Z")),
                    rbind(episode("twice", 1L, "2025-01-10", "first archiving",
                                  relisted_on = "2025-06-01"),
                          episode("twice", 2L, "2026-03-02", "second archiving")))
  on.exit(DBI::dbDisconnect(con))

  expect_equal(enrich_removal_reasons(con), 2L)
  expect_equal(reasons(con)$removal_reason, c("first archiving", "second archiving"))
})

test_that("an absent table leaves everything as it was", {
  con <- removal_db(removed("tsxtreme", "2026-09-27T20:37:57Z"))
  on.exit(DBI::dbDisconnect(con))
  expect_equal(enrich_removal_reasons(con), 0L)
  expect_equal(reasons(con)$removal_reason, placeholder)

  empty <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(empty), add = TRUE)
  DBI::dbExecute(empty, "CREATE TABLE cran_archive_history (package TEXT, episode_seq INTEGER,
                           archived_on TEXT, removal_reason TEXT)")
  expect_equal(enrich_removal_reasons(empty), 0L)
})

test_that("no reason goes from a value to NULL, and only removal events change", {
  con <- removal_db(
    rbind(removed("kept", "2026-05-01T00:00:00Z", reason = "an older recorded reason"),
          removed("nullreason", "2026-05-01T00:00:00Z"),
          data.frame(package = "kept", event_type = "updated", removal_reason = NA_character_,
                     detected_at = "2026-05-01T00:00:00Z", stringsAsFactors = FALSE)),
    rbind(episode("kept", 1L, "2026-05-02", NA_character_),
          episode("nullreason", 1L, "2026-05-01", "   ")))
  on.exit(DBI::dbDisconnect(con))
  before <- reasons(con)

  expect_equal(enrich_removal_reasons(con), 0L)
  after <- reasons(con)
  expect_equal(after, before)
  expect_false(any(!is.na(before$removal_reason) & is.na(after$removal_reason)))
})

test_that("merge.R gives removal reasons inside the enrichment transaction", {
  src <- readLines(file.path(getwd(), "..", "..", "merge.R"))
  begin <- grep('dbExecute(con, "BEGIN TRANSACTION")', src, fixed = TRUE)
  call <- grep("enrich_removal_reasons(con)", src, fixed = TRUE)
  commit <- grep('dbExecute(con, "COMMIT")', src, fixed = TRUE)
  expect_length(call, 1L)
  expect_true(any(begin < call) && any(commit > call))
  # removal_reasons is empty upstream and no longer read.
  expect_false(any(grepl("FROM removal_reasons", src, fixed = TRUE)))
})
