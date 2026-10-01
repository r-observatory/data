for (f in c("fold.R", "check_flavor_fold.R", "series.R", "extract.R")) {
  source(file.path(getwd(), "..", "..", "history", f))
}

families <- history_families()

cran_only <- function() families["cran-metadata"]

test_that("the listing reads every release and never a draft", {
  args <- history_list_args("r-observatory/cran-metadata")
  expect_true("--exclude-drafts" %in% args)
  limit <- as.integer(args[which(args == "--limit") + 1L])
  expect_gte(limit, 1000L)
})

test_that("the plan skips drafts, other tags, today and the days before the series", {
  listing <- releases(
    c("v2026-06-27", "v2026-06-28", "v2026-03-14", "history", "ci-logs", "v2026-10-01", "v2026-06-29"),
    c("2026-06-27T08:00:00Z", "2026-06-28T22:09:08Z", "2026-03-14T08:00:00Z", "2026-10-01T00:00:00Z",
      "2026-10-01T00:00:00Z", "2026-10-01T08:00:00Z", "2026-06-29T16:32:19Z"),
    draft = c(FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE))
  none <- data.frame(tag = character(0), snapshot_on = character(0), outcome = character(0))
  plan <- plan_family_snapshots(listing, families$data, none, "2026-10-01")
  expect_equal(plan$tag, c("v2026-06-27", "v2026-06-28", "v2026-06-29"))
  expect_equal(plan$action, c("skipped", "process", "process"))
  expect_equal(plan$note[1], "predates the history series")
})

test_that("today's cran-metadata release is folded, so a same-day catch-up can reach it", {
  listing <- releases(c("v20261001-061000", "v20261002-000500"),
                      c("2026-10-01T06:10:08Z", "2026-10-02T00:05:00Z"))
  none <- data.frame(tag = character(0), snapshot_on = character(0), outcome = character(0))
  plan <- plan_family_snapshots(listing, families$`cran-metadata`, none, "2026-10-01")
  expect_equal(plan$tag, "v20261001-061000")
  expect_equal(plan$action, "process")
})

test_that("the last release of a day is folded and the earlier ones superseded", {
  listing <- releases(c("v20260716-061000", "v20260716-181500", "v20260717-060500"),
                      c("2026-07-16T06:10:08Z", "2026-07-16T18:15:10Z", "2026-07-17T06:05:02Z"))
  none <- data.frame(tag = character(0), snapshot_on = character(0), outcome = character(0))
  plan <- plan_family_snapshots(listing, families$`cran-metadata`, none, "2026-10-01")
  expect_equal(plan$action, c("superseded", "process", "process"))
  expect_equal(plan$superseded_by[1], "v20260716-181500")
})

test_that("a release for a day already folded is superseded, never folded out of order", {
  listing <- releases(c("v20260928-122459", "v20260928-235900", "v20260929-115049"),
                      c("2026-09-28T12:25:09Z", "2026-09-29T00:01:00Z", "2026-09-29T11:50:57Z"))
  recorded <- data.frame(tag = "v20260928-122459", snapshot_on = "2026-09-28", outcome = "processed")
  plan <- plan_family_snapshots(listing, families$`cran-metadata`, recorded, "2026-10-01")
  expect_equal(plan$tag, c("v20260928-235900", "v20260929-115049"))
  expect_equal(plan$action, c("superseded", "process"))
  expect_equal(plan$note[1], "published after 2026-09-28 was folded")
})
