# Tests for the publish gate. Everything here is offline and deterministic:
# "now" is always an explicit argument and no test touches sources/, the
# network, or a real observatory.db. This suite runs inside merge.yml itself,
# before the merge, so a flaky test here would cost the site a day of data.
source(file.path(getwd(), "..", "..", "merge_gate.R"))

# A one-entry pipeline_config-shaped fixture, written inline so the live config
# can be retuned without breaking these tests.
cfg_entry <- function(name, db_filename, max_age_h = 30L) {
  list(name = name, repo = paste0("r-observatory/", name),
       schedule = "daily", max_age_h = max_age_h,
       rolling = TRUE, manifest = TRUE, db_filename = db_filename)
}

meta_row <- function(pipeline, last_checked = NA_character_,
                     last_changed = NA_character_,
                     released_at = NA_character_,
                     expected_max_age_hours = 30L) {
  data.frame(pipeline = pipeline, last_checked = last_checked,
             last_changed = last_changed, released_at = released_at,
             expected_max_age_hours = expected_max_age_hours,
             stringsAsFactors = FALSE)
}

# The real measured values, 2026-07-22. All three pipelines run every day and
# find nothing new, because Anaconda and r2u publish month-complete data.
unchanged_but_checked <- rbind(
  meta_row("conda-forge-downloads", "2026-07-22T07:35:01Z", "2026-07-10T08:01:42Z",
           "2026-07-22T07:36:00Z", 30L),
  meta_row("bioconda-downloads", "2026-07-22T07:43:40Z", "2026-07-10T08:01:42Z",
           "2026-07-22T07:44:00Z", 30L),
  meta_row("r2u-downloads", "2026-07-22T08:33:57Z", "2026-07-10T08:01:39Z",
           "2026-07-10T08:02:00Z", 840L))

unchanged_cfg <- list(
  cfg_entry("conda-forge-downloads", "conda-forge-downloads-summary.db", 30L),
  cfg_entry("bioconda-downloads", "bioconda-downloads-summary.db", 30L),
  cfg_entry("r2u-downloads", "r2u-summary.db", 840L))

unchanged_dbs <- c("conda-forge-downloads-summary.db",
                   "bioconda-downloads-summary.db",
                   "r2u-summary.db")

now <- "2026-07-22T10:38:00Z"

# ---------------------------------------------------------------------------
# The property that decides the whole design
# ---------------------------------------------------------------------------

test_that("conda-forge, bioconda and r2u pass with data unchanged for twelve days", {
  res <- evaluate_freshness_gate(
    meta = unchanged_but_checked,
    present_dbs = unchanged_dbs,
    all_source_dbs = unchanged_dbs,
    config = unchanged_cfg,
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, c("ok", "ok", "ok"))
  expect_equal(res$problems, character(0))
  expect_equal(res$fatal_problems, character(0))
  expect_false(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("keying on last_changed instead would have failed all three", {
  # Not a change request: this pins down why the gate reads last_checked. Each
  # of these pipelines last changed twelve days ago and is perfectly healthy.
  ages <- vapply(seq_len(nrow(unchanged_but_checked)), function(i) {
    gate_age_hours(unchanged_but_checked$last_changed[i], now)
  }, numeric(1))
  limits <- unchanged_but_checked$expected_max_age_hours * gate_stale_multiplier()

  expect_true(all(ages > 280))
  # conda-forge and bioconda would be judged stale on last_changed; r2u's own
  # 35-day window absorbs it, which is exactly the inconsistency to avoid.
  expect_equal(ages > limits, c(TRUE, TRUE, FALSE))
})

# ---------------------------------------------------------------------------
# Non-fatal staleness
# ---------------------------------------------------------------------------

test_that("a source not checked within its window fails the run but still publishes", {
  meta <- meta_row("bioconductor-metadata", "2026-07-18T06:00:00Z",
                   "2026-07-18T06:00:00Z", "2026-07-18T06:05:00Z", 30L)
  res <- evaluate_freshness_gate(
    meta = meta,
    present_dbs = "bioconductor-metadata.db",
    all_source_dbs = "bioconductor-metadata.db",
    config = list(cfg_entry("bioconductor-metadata", "bioconductor-metadata.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "stale")
  expect_equal(length(res$problems), 1L)
  expect_equal(res$fatal_problems, character(0))
  expect_true(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("slow is reported between one and two windows but does not fail", {
  # 45 hours old against a 30 hour window: past 1x, inside 2x.
  meta <- meta_row("cran-metadata", "2026-07-20T13:38:00Z",
                   "2026-07-20T13:38:00Z", "2026-07-20T13:38:00Z", 30L)
  res <- evaluate_freshness_gate(
    meta = meta,
    present_dbs = "metadata.db",
    all_source_dbs = "metadata.db",
    config = list(cfg_entry("cran-metadata", "metadata.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$freshness, "slow")
  expect_false(res$rows$fails)
  expect_false(res$run_failed)
})

test_that("a missing non-fatal source fails the run but still publishes", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-coverage", "2026-07-22T09:00:00Z", "2026-07-22T09:00:00Z",
                    "2026-07-22T09:00:00Z", 8L),
    present_dbs = character(0),
    all_source_dbs = "cran-coverage.db",
    config = list(cfg_entry("cran-coverage", "cran-coverage.db", 8L)),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "missing")
  expect_true(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("a source removed for failing its integrity check is named as corrupt", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-archive", "2026-07-22T07:35:38Z", "2026-07-22T07:35:38Z",
                    "2026-07-22T07:36:00Z", 30L),
    present_dbs = character(0),
    all_source_dbs = "cran-archive.db",
    config = list(cfg_entry("cran-archive", "cran-archive.db")),
    now_iso = now,
    fatal_specs = list(),
    integrity_failed = "cran-archive.db",
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "corrupt")
  expect_true(any(grepl("integrity_check", res$problems)))
})

# ---------------------------------------------------------------------------
# Fatal sources
# ---------------------------------------------------------------------------

test_that("an absent feed.db refuses the release", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-feed", "2026-07-22T13:27:57Z", "2026-07-22T13:27:57Z",
                    "2026-07-22T13:27:57Z", 8L),
    present_dbs = character(0),
    all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)),
    now_iso = now,
    row_counts = list(packages = 0),
    output_bytes = NA_real_)

  expect_true(res$rows$fatal)
  expect_equal(res$rows$verdict, "missing")
  expect_equal(length(res$fatal_problems), 1L)
  expect_false(res$publish_allowed)
})

test_that("a feed.db that merged too few packages refuses the release", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-feed", "2026-07-22T13:27:57Z", "2026-07-22T13:27:57Z",
                    "2026-07-22T13:27:57Z", 8L),
    present_dbs = "feed.db",
    all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)),
    now_iso = now,
    row_counts = list(packages = 12),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "short rows")
  expect_false(res$publish_allowed)
  expect_true(any(grepl("packages", res$fatal_problems)))
})

test_that("a full feed.db passes the row floor", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-feed", "2026-07-22T13:27:57Z", "2026-07-22T13:27:57Z",
                    "2026-07-22T13:27:57Z", 8L),
    present_dbs = "feed.db",
    all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)),
    now_iso = now,
    row_counts = list(packages = 24331),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "ok")
  expect_true(res$publish_allowed)
  expect_false(res$run_failed)
})

test_that("metadata.db is judged per table, so a legitimately empty table is fine", {
  # removal_reasons really does ship zero rows, so only authors and
  # cran_check_results carry floors.
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-metadata", "2026-07-22T08:11:22Z", "2026-07-22T08:11:22Z",
                    "2026-07-22T08:11:22Z", 30L),
    present_dbs = "metadata.db",
    all_source_dbs = "metadata.db",
    config = list(cfg_entry("cran-metadata", "metadata.db")),
    now_iso = now,
    row_counts = list(authors = 62393, cran_check_results = 315848,
                      removal_reasons = 0),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "ok")
  expect_true(res$publish_allowed)
})

test_that("an emptied authors table refuses the release", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-metadata", "2026-07-22T08:11:22Z", "2026-07-22T08:11:22Z",
                    "2026-07-22T08:11:22Z", 30L),
    present_dbs = "metadata.db",
    all_source_dbs = "metadata.db",
    config = list(cfg_entry("cran-metadata", "metadata.db")),
    now_iso = now,
    row_counts = list(authors = 0, cran_check_results = 315848),
    output_bytes = NA_real_)

  expect_false(res$publish_allowed)
  expect_true(any(grepl("authors", res$fatal_problems)))
})

test_that("only feed.db and metadata.db are fatal", {
  expect_setequal(names(gate_fatal_sources()), c("feed.db", "metadata.db"))
})

test_that("a stale but present and complete feed.db still publishes", {
  # cran-feed missing several six-hourly runs leaves feed.db on disk exactly as
  # it shipped green yesterday, so refusing to publish it would cost the whole
  # site a day of data over a source that is entirely usable.
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-feed", "2026-07-20T10:00:00Z", "2026-07-20T10:00:00Z",
                    "2026-07-20T10:05:00Z", 8L),
    present_dbs = "feed.db",
    all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)),
    now_iso = now,
    row_counts = list(packages = 24331),
    output_bytes = NA_real_)

  expect_true(res$rows$fatal)
  expect_equal(res$rows$verdict, "stale")
  expect_equal(res$fatal_problems, character(0))
  expect_equal(length(res$problems), 1L)
  expect_true(res$publish_allowed)
  expect_true(res$run_failed)
})

test_that("a stale but complete metadata.db still publishes", {
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-metadata", "2026-07-18T08:00:00Z", "2026-07-18T08:00:00Z",
                    "2026-07-18T08:05:00Z", 30L),
    present_dbs = "metadata.db",
    all_source_dbs = "metadata.db",
    config = list(cfg_entry("cran-metadata", "metadata.db")),
    now_iso = now,
    row_counts = list(authors = 62393, cran_check_results = 315848),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "stale")
  expect_true(res$publish_allowed)
  expect_true(res$run_failed)
  expect_equal(res$fatal_problems, character(0))
})

test_that("only absence, corruption and short rows can refuse the release", {
  expect_setequal(gate_fatal_verdicts(), c("corrupt", "missing", "short rows"))
  expect_false("stale" %in% gate_fatal_verdicts())
})

test_that("row counts that were never supplied read as zero rather than erroring", {
  res <- evaluate_freshness_gate(
    meta = NULL, present_dbs = "feed.db", all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)), now_iso = now,
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "short rows")
  expect_false(res$publish_allowed)
})

test_that("row counts given as a named vector are read the same as a list", {
  res <- evaluate_freshness_gate(
    meta = NULL, present_dbs = "feed.db", all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)), now_iso = now,
    row_counts = c(packages = 24331), output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "unknown")
  expect_true(res$publish_allowed)
})

# ---------------------------------------------------------------------------
# Fallbacks and unknowns never fail
# ---------------------------------------------------------------------------

test_that("a manifest-less source resolved from its release time passes", {
  # cran-downloads publishes no last_checked and no last_changed, so the
  # release time is the only reference the chain can reach.
  meta <- meta_row("cran-downloads", last_checked = NA_character_,
                   last_changed = NA_character_,
                   released_at = "2026-07-22T07:05:00Z", 30L)
  res <- evaluate_freshness_gate(
    meta = meta,
    present_dbs = "downloads-summary.db",
    all_source_dbs = "downloads-summary.db",
    config = list(cfg_entry("cran-downloads", "downloads-summary.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$last_checked, "2026-07-22T07:05:00Z")
  expect_equal(res$rows$verdict, "ok")
  expect_false(res$run_failed)
})

test_that("an empty last_checked falls through to last_changed then released_at", {
  meta <- meta_row("cran-archive", last_checked = NA_character_,
                   last_changed = "", released_at = "2026-07-22T07:35:38Z", 30L)
  res <- evaluate_freshness_gate(
    meta = meta,
    present_dbs = "cran-archive.db",
    all_source_dbs = "cran-archive.db",
    config = list(cfg_entry("cran-archive", "cran-archive.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$last_checked, "2026-07-22T07:35:38Z")
  expect_equal(res$rows$verdict, "ok")
})

test_that("no usable timestamp at all reports unknown and never fails", {
  meta <- meta_row("cran-archive", NA_character_, NA_character_, NA_character_, 30L)
  res <- evaluate_freshness_gate(
    meta = meta,
    present_dbs = "cran-archive.db",
    all_source_dbs = "cran-archive.db",
    config = list(cfg_entry("cran-archive", "cran-archive.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$freshness, "unknown")
  expect_false(res$rows$fails)
  expect_false(res$run_failed)
})

test_that("a missing pipeline_metadata table reports unknown, not stale", {
  res <- evaluate_freshness_gate(
    meta = NULL,
    present_dbs = "cran-archive.db",
    all_source_dbs = "cran-archive.db",
    config = list(cfg_entry("cran-archive", "cran-archive.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$freshness, "unknown")
  expect_false(res$run_failed)
})

test_that("a pipeline declaring no window falls back to the shared default", {
  cfg <- list(list(name = "mystery", repo = "r-observatory/mystery",
                   schedule = "daily", rolling = TRUE, manifest = TRUE,
                   db_filename = "mystery.db"))
  meta <- meta_row("mystery", "2026-07-22T09:00:00Z", "2026-07-22T09:00:00Z",
                   "2026-07-22T09:00:00Z", NA_integer_)
  res <- evaluate_freshness_gate(
    meta = meta, present_dbs = "mystery.db", all_source_dbs = "mystery.db",
    config = cfg, now_iso = now, fatal_specs = list(), output_bytes = NA_real_)

  expect_equal(res$rows$max_age_hours, gate_default_max_age_h())
  expect_equal(res$rows$verdict, "ok")
})

test_that("an unparseable timestamp reports unknown rather than stale", {
  expect_true(is.na(gate_parse_time("not a date")))
  expect_true(is.na(gate_age_hours("not a date", now)))
  expect_equal(
    gate_freshness("not a date", NA_character_, NA_character_, 30L, now)$status,
    "unknown")
})

# ---------------------------------------------------------------------------
# Sub-daily windows the merge cannot resolve
# ---------------------------------------------------------------------------

test_that("cran-queue's worst measured age at gate time still passes", {
  # cran-queue declares 3 hours on an hourly cron. GitHub starts the 08:00 UTC
  # merge between 1.6 and 4.1 hours late, so the worst age observed at gate
  # time over 39 scheduled merges was 5.9 hours: a healthy pipeline that would
  # trip an unfloored 2 x 3 hour threshold by six minutes.
  meta <- meta_row("cran-queue", "2026-07-22T04:44:00Z", "2026-07-22T04:44:00Z",
                   "2026-07-22T04:45:00Z", 3L)
  res <- evaluate_freshness_gate(
    meta = meta, present_dbs = "queue.db", all_source_dbs = "queue.db",
    config = list(cfg_entry("cran-queue", "queue.db", 3L)),
    now_iso = now, fatal_specs = list(), output_bytes = NA_real_)

  expect_true(res$rows$age_hours > 5.8 && res$rows$age_hours < 6.0)
  expect_equal(res$rows$verdict, "ok")
  expect_false(res$run_failed)
})

test_that("a dead hourly pipeline is still caught once it clears the floor", {
  meta <- meta_row("cran-queue", "2026-07-20T10:00:00Z", "2026-07-20T10:00:00Z",
                   "2026-07-20T10:00:00Z", 3L)
  res <- evaluate_freshness_gate(
    meta = meta, present_dbs = "queue.db", all_source_dbs = "queue.db",
    config = list(cfg_entry("cran-queue", "queue.db", 3L)),
    now_iso = now, fatal_specs = list(), output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "stale")
  expect_true(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("the declared window is reported even when a wider one is applied", {
  fresh <- gate_freshness("2026-07-22T04:44:00Z", NA_character_, NA_character_,
                          3L, now)
  # The /freshness page and the gate must agree on the declared number.
  expect_equal(fresh$max_age_h, 3)
  expect_equal(fresh$effective_max_age_h, gate_min_stale_window_h())
  expect_equal(fresh$status, "current")
})

test_that("a window wider than the floor is left alone", {
  fresh <- gate_freshness("2026-07-20T10:00:00Z", NA_character_, NA_character_,
                          30L, now)
  expect_equal(fresh$max_age_h, 30)
  expect_equal(fresh$effective_max_age_h, 30)
})

test_that("the table shows both the declared window and the failing age", {
  meta <- meta_row("cran-queue", "2026-07-22T04:44:00Z", "2026-07-22T04:44:00Z",
                   "2026-07-22T04:45:00Z", 3L)
  res <- evaluate_freshness_gate(
    meta = meta, present_dbs = "queue.db", all_source_dbs = "queue.db",
    config = list(cfg_entry("cran-queue", "queue.db", 3L)),
    now_iso = now, fatal_specs = list(), output_bytes = NA_real_)
  tbl <- format_gate_table(res$rows)

  expect_true(any(grepl("fail_h", tbl, fixed = TRUE)))
  expect_equal(res$rows$max_age_hours, 3)
  expect_equal(res$rows$fail_after_hours,
               gate_stale_multiplier() * gate_min_stale_window_h())
})

# ---------------------------------------------------------------------------
# Coverage of the two lists that do not line up
# ---------------------------------------------------------------------------

test_that("a source with no pipeline_config entry is checked for presence only", {
  # cran-data-metrics.db and bioc-data-metrics.db ride in their sibling's
  # release and have no config of their own. They must not borrow a freshness
  # verdict, and they must not silently escape the gate either.
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-code-metrics", "2026-07-22T06:27:47Z",
                    "2026-07-22T06:27:47Z", "2026-07-22T06:28:00Z", 30L),
    present_dbs = c("cran-code-metrics.db", "cran-data-metrics.db"),
    all_source_dbs = c("cran-code-metrics.db", "cran-data-metrics.db"),
    config = list(cfg_entry("cran-code-metrics", "cran-code-metrics.db")),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$source, c("cran-code-metrics.db", "cran-data-metrics.db"))
  expect_equal(res$rows$freshness, c("current", "not tracked"))
  expect_equal(res$rows$verdict, c("ok", "ok"))
  expect_false(res$run_failed)
})

test_that("an unconfigured source going missing still fails the run", {
  res <- evaluate_freshness_gate(
    meta = NULL,
    present_dbs = character(0),
    all_source_dbs = "bioc-data-metrics.db",
    config = list(),
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(res$rows$verdict, "missing")
  expect_true(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("the merger's own config entry is excluded from the gate", {
  config <- list(
    cfg_entry("cran-archive", "cran-archive.db"),
    list(name = "data", repo = "r-observatory/data", schedule = "daily",
         max_age_h = 30L, rolling = FALSE, manifest = FALSE, self = TRUE))
  res <- evaluate_freshness_gate(
    meta = meta_row("cran-archive", "2026-07-22T07:35:38Z", "2026-07-22T07:35:38Z",
                    "2026-07-22T07:36:00Z", 30L),
    present_dbs = "cran-archive.db",
    all_source_dbs = "cran-archive.db",
    config = config,
    now_iso = now,
    fatal_specs = list(),
    output_bytes = NA_real_)

  expect_equal(nrow(res$rows), 1L)
  expect_false("data" %in% res$rows$pipeline)
})

test_that("the live config and source list line up as the gate expects", {
  source(file.path(getwd(), "..", "..", "merge_helpers.R"))
  source(file.path(getwd(), "..", "..", "pipeline_metadata.R"))
  cfg <- pipeline_config()
  dbs <- unlist(lapply(cfg, function(c) c$db_filename))
  # Every configured db_filename is a source the merge actually reads.
  expect_equal(setdiff(dbs, source_dbs), character(0))
  # The two sibling data-metrics DBs lack a config entry. Directional, not a
  # closed world: adding a source ahead of its config entry must not redden the
  # daily merge, since the gate already scores an unconfigured source as "not
  # tracked" and presence-checks it.
  expect_true(all(c("cran-data-metrics.db", "bioc-data-metrics.db") %in%
                    setdiff(source_dbs, dbs)))
  # Both fatal sources are configured.
  expect_true(all(names(gate_fatal_sources()) %in% dbs))
})

# ---------------------------------------------------------------------------
# Output size floor
# ---------------------------------------------------------------------------

test_that("a collapsed output size fails the run without blocking the release", {
  res <- evaluate_freshness_gate(
    meta = NULL, present_dbs = character(0), all_source_dbs = character(0),
    config = list(), now_iso = now, fatal_specs = list(),
    output_bytes = 190000000)

  expect_false(res$size_ok)
  expect_true(res$run_failed)
  expect_true(res$publish_allowed)
})

test_that("losing one metrics source stays above the size floor", {
  res <- evaluate_freshness_gate(
    meta = NULL, present_dbs = character(0), all_source_dbs = character(0),
    config = list(), now_iso = now, fatal_specs = list(),
    output_bytes = 1900000000)

  expect_true(res$size_ok)
  expect_false(res$run_failed)
})

test_that("an unknown output size is not treated as a collapsed one", {
  res <- evaluate_freshness_gate(
    meta = NULL, present_dbs = character(0), all_source_dbs = character(0),
    config = list(), now_iso = now, fatal_specs = list(),
    output_bytes = NA_real_)

  expect_true(res$size_ok)
  expect_false(res$run_failed)
})

# ---------------------------------------------------------------------------
# Escape hatch
# ---------------------------------------------------------------------------

test_that("the override publishes despite a fatal verdict and still fails the run", {
  fatal_args <- list(
    meta = meta_row("cran-feed", "2026-07-22T13:27:57Z", "2026-07-22T13:27:57Z",
                    "2026-07-22T13:27:57Z", 8L),
    present_dbs = character(0),
    all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)),
    now_iso = now,
    row_counts = list(packages = 0),
    output_bytes = NA_real_)

  blocked <- do.call(evaluate_freshness_gate, c(fatal_args, list(override = FALSE)))
  expect_false(blocked$publish_allowed)
  expect_false(blocked$override_used)

  forced <- do.call(evaluate_freshness_gate, c(fatal_args, list(override = TRUE)))
  expect_true(forced$publish_allowed)
  expect_true(forced$override_used)
  expect_true(forced$run_failed)
  expect_equal(length(forced$fatal_problems), 1L)
})

test_that("only the literal word true asks for the override", {
  # A scheduled run resolves the workflow input to the empty string, so an
  # over-permissive parse here would silently disable the fatal gate forever.
  expect_true(gate_override_requested("true"))
  expect_true(gate_override_requested("TRUE"))
  expect_true(gate_override_requested(" True "))
  expect_true(gate_override_requested(TRUE))
  for (v in list("", "false", "FALSE", "0", "1", "yes", "y", "on", " ",
                 NA_character_, NULL, character(0), c("true", "true"))) {
    expect_false(gate_override_requested(v))
  }
})

test_that("the override is not reported as used when nothing was fatal", {
  res <- evaluate_freshness_gate(
    meta = unchanged_but_checked, present_dbs = unchanged_dbs,
    all_source_dbs = unchanged_dbs, config = unchanged_cfg, now_iso = now,
    fatal_specs = list(), output_bytes = NA_real_, override = TRUE)

  expect_false(res$override_used)
  expect_true(res$publish_allowed)
})

# ---------------------------------------------------------------------------
# The verdict table
# ---------------------------------------------------------------------------

test_that("the verdict table names every source, its age and its limit", {
  res <- evaluate_freshness_gate(
    meta = unchanged_but_checked, present_dbs = unchanged_dbs,
    all_source_dbs = unchanged_dbs, config = unchanged_cfg, now_iso = now,
    fatal_specs = list(), output_bytes = NA_real_)
  tbl <- format_gate_table(res$rows)

  expect_true(any(grepl("^source", tbl)))
  expect_true(all(vapply(unchanged_dbs,
                         function(d) any(grepl(d, tbl, fixed = TRUE)),
                         logical(1))))
  expect_true(any(grepl("2026-07-22T07:35:01Z", tbl, fixed = TRUE)))
  expect_true(any(grepl("840", tbl, fixed = TRUE)))
  expect_true(any(grepl("3 of 3 sources passed", format_gate_summary(res), fixed = TRUE)))
})

test_that("a fatal row is marked as fatal in the table", {
  res <- evaluate_freshness_gate(
    meta = NULL, present_dbs = character(0), all_source_dbs = "feed.db",
    config = list(cfg_entry("cran-feed", "feed.db", 8L)), now_iso = now,
    row_counts = list(packages = 0), output_bytes = NA_real_)

  expect_true(any(grepl("missing (fatal)", format_gate_table(res$rows), fixed = TRUE)))
})

test_that("the status file lists every problem, fatal first, or nothing at all", {
  clean <- evaluate_freshness_gate(
    meta = unchanged_but_checked, present_dbs = unchanged_dbs,
    all_source_dbs = unchanged_dbs, config = unchanged_cfg, now_iso = now,
    fatal_specs = list(), output_bytes = NA_real_)
  expect_equal(gate_status_lines(clean), character())

  stale_only <- evaluate_freshness_gate(
    meta = meta_row("bioconductor-metadata", "2026-07-18T06:00:00Z",
                    "2026-07-18T06:00:00Z", "2026-07-18T06:05:00Z", 30L),
    present_dbs = "bioconductor-metadata.db",
    all_source_dbs = "bioconductor-metadata.db",
    config = list(cfg_entry("bioconductor-metadata", "bioconductor-metadata.db")),
    now_iso = now, fatal_specs = list(), output_bytes = NA_real_)
  expect_equal(length(gate_status_lines(stale_only)), 1L)
  expect_true(any(grepl("bioconductor-metadata.db",
                        gate_status_lines(stale_only), fixed = TRUE)))

  both <- evaluate_freshness_gate(
    meta = NULL, present_dbs = character(0),
    all_source_dbs = c("feed.db", "cran-archive.db"),
    config = list(cfg_entry("cran-feed", "feed.db", 8L),
                  cfg_entry("cran-archive", "cran-archive.db")),
    now_iso = now, row_counts = list(packages = 0), output_bytes = NA_real_)
  expect_equal(gate_status_lines(both), c(both$fatal_problems, both$problems))
  expect_true(any(grepl("feed.db", gate_status_lines(both)[1], fixed = TRUE)))
})

# ---------------------------------------------------------------------------
# Workflow wiring, asserted the way this repo already asserts it
# ---------------------------------------------------------------------------

merge_yml <- function() {
  readLines(file.path(getwd(), "..", "..", "..",
                      ".github", "workflows", "merge.yml"))
}

test_that("the viewer deploy trigger no longer swallows its own failure", {
  yml <- merge_yml()
  trigger <- grep("Trigger viewer deploy", yml)
  expect_true(length(trigger) >= 1L)
  start <- min(trigger)
  window <- yml[start:min(length(yml), start + 12)]
  expect_false(any(grepl("continue-on-error:", window, fixed = TRUE)))
  expect_true(any(grepl("VIEWER_DEPLOY_TOKEN", window)))
})

test_that("a missing deploy token is reported by name", {
  yml <- merge_yml()
  named <- grep("VIEWER_DEPLOY_TOKEN", yml, value = TRUE)
  expect_true(any(grepl("::error::", named)))
})

test_that("the merge workflow runs the freshness gate before creating a release", {
  yml <- merge_yml()
  gate <- grep("check-freshness.R", yml)
  release <- grep("name: Create release", yml)
  expect_true(length(gate) >= 1L)
  expect_true(length(release) >= 1L)
  expect_true(min(gate) < min(release))
})

test_that("a non-fatal gate failure reddens the run after the release", {
  # The publish-then-redden half of the gate. Without this step every stale,
  # missing or corrupt non-fatal source goes green exactly as it did before.
  yml <- merge_yml()
  step <- grep("name: Fail the run if any source failed the gate", yml)
  expect_true(length(step) >= 1L)
  start <- min(step)
  expect_true(start > min(grep("name: Create release", yml)))
  window <- yml[start:min(length(yml), start + 14)]
  # Not `always()`: a cancelled job must not be relabelled as a failed one.
  expect_true(any(grepl("!cancelled()", window, fixed = TRUE)))
  expect_false(any(grepl("if: always()", window, fixed = TRUE)))
  expect_true(any(grepl("merge-gate-failed", window, fixed = TRUE)))
  expect_true(any(grepl("exit 1", window, fixed = TRUE)))
})

test_that("the notification says whether the site got data today", {
  # A month of identical "the run failed" comments is worth nothing. The first
  # line of the body must distinguish an outage from a degraded-but-published
  # run, and the deploy trigger will redden every run until the secret exists.
  yml <- merge_yml()
  expect_true(any(grepl("-f merge-gate-refused", yml, fixed = TRUE)))
  expect_true(any(grepl("> release-published", yml, fixed = TRUE)))
  expect_true(any(grepl("-f release-published", yml, fixed = TRUE)))
  expect_true(any(grepl("NO RELEASE WAS PUBLISHED", yml, fixed = TRUE)))
  expect_true(any(grepl("release WAS published", yml, fixed = TRUE)))
})

test_that("the gate script writes both markers the notification reads", {
  gate <- readLines(file.path(getwd(), "..", "..", "check-freshness.R"))
  expect_true(any(grepl("merge-gate-failed", gate, fixed = TRUE)))
  expect_true(any(grepl("merge-gate-refused", gate, fixed = TRUE)))
  expect_true(any(grepl("gate_status_lines(res), status_file", gate, fixed = TRUE)))
  expect_true(any(grepl("writeLines(\"refused\", refused_file)", gate, fixed = TRUE)))
  expect_true(any(grepl("gate_override_requested(", gate, fixed = TRUE)))
})

test_that("the fatal sources are downloaded with a retry rather than one attempt", {
  # A single 5xx on feed.db or metadata.db would otherwise read as "missing",
  # which is fatal, and cost the site a day of data.
  yml <- merge_yml()
  # Two 3-attempt loops: verify_size already had one, the download now has one.
  expect_true(sum(grepl("for attempt in 1 2 3", yml, fixed = TRUE)) >= 2L)
  expect_true(any(grepl("dl_backoffs", yml, fixed = TRUE)))
  expect_true(any(grepl("dl_ok=yes", yml, fixed = TRUE)))
})

test_that("the merge workflow keeps a documented override input", {
  yml <- merge_yml()
  expect_true(any(grepl("workflow_dispatch", yml)))
  expect_true(any(grepl("ignore_freshness_gate", yml)))
  expect_true(any(grepl("GATE_OVERRIDE", yml)))
})

test_that("the merge workflow can open the failure issue it promises", {
  yml <- merge_yml()
  expect_true(any(grepl("issues: write", yml)))
  expect_true(any(grepl("if: failure\\(\\)", yml)))
  expect_true(any(grepl("gh issue create", yml)))
  # Reuse one thread rather than opening an issue per day.
  expect_true(any(grepl("gh issue comment", yml)))
  title <- grep("^ *TITLE=", yml, value = TRUE)
  expect_true(length(title) >= 1L)
  # No date, run id or any other shell expansion in the title, so a week of
  # failures stays one thread rather than seven.
  expect_false(any(grepl("[$]", title)))
  # And the de-duplication really matches on that title rather than picking
  # whichever labelled issue happens to come back first.
  expect_true(any(grepl("select(.title ==", yml, fixed = TRUE)))
})

test_that("pipeline_metadata carries every column the gate reads", {
  # check-freshness.R selects these five by name with no tryCatch, so dropping
  # or renaming one would error on the fatal path.
  source(file.path(getwd(), "..", "..", "pipeline_metadata.R"))
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con))
  write_pipeline_metadata(con, data.frame())
  expect_true(all(c("pipeline", "last_checked", "last_changed", "released_at",
                    "expected_max_age_hours") %in%
                    DBI::dbListFields(con, "pipeline_metadata")))
})

test_that("the integrity check records what it removed", {
  yml <- merge_yml()
  expect_true(any(grepl("sources/.integrity-failed", yml, fixed = TRUE)))
})
