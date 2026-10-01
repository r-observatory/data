for (f in c("fold.R", "check_flavor_fold.R", "series.R", "extract.R", "assets.R")) {
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

test_that("a download is checked against the release and retried", {
  dir <- withr::local_tempdir()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:00Z"),
                list("v20260901-060000" = path), fail = list("v20260901-060000" = 1L))
  got <- fetch_snapshot(io, families$`cran-metadata`, "v20260901-060000", file.path(dir, "w"), 20)
  expect_equal(unname(tools::md5sum(got$path)), unname(tools::md5sum(path)))
  expect_equal(got$sha256, history_file_sha256(path))
  expect_equal(io$state$sleeps, 15)
})

test_that("a download that never matches its digest stops the run", {
  dir <- withr::local_tempdir()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:00Z"),
                list("v20260901-060000" = path))
  io$asset_info <- function(repo, tag) data.frame(name = "metadata.db", size = file.size(path),
    digest = "sha256:00", state = "uploaded", stringsAsFactors = FALSE)
  expect_error(fetch_snapshot(io, families$`cran-metadata`, "v20260901-060000", dir, 20),
               "sha256 differs.*--give-up=cran-metadata:v20260901-060000")
  expect_equal(io$state$sleeps, c(15, 60))
})

test_that("a .zst asset is expanded and removed", {
  skip_if(!nzchar(Sys.which("zstd")), "zstd is not available")
  dir <- withr::local_tempdir()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  system2("zstd", c("-q", "-f", shQuote(path), "-o", shQuote(paste0(path, ".zst"))))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:00Z"),
                list("v20260901-060000" = paste0(path, ".zst")), asset = "metadata.db.zst")
  got <- fetch_snapshot(io, families$`cran-metadata`, "v20260901-060000", file.path(dir, "w"), 20)
  expect_equal(basename(got$path), "metadata.db")
  expect_false(file.exists(paste0(got$path, ".zst")))
  expect_equal(unname(tools::md5sum(got$path)), unname(tools::md5sum(path)))
})

test_that("the run stops before a download that would cross the free-space floor", {
  dir <- withr::local_tempdir()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:00Z"),
                list("v20260901-060000" = path), free = 20.000001)
  expect_error(fetch_snapshot(io, families$`cran-metadata`, "v20260901-060000", dir, 20),
               "free some space")
  expect_length(io$state$downloads, 0L)
})

test_that("free space is read with df in a work directory that did not exist yet", {
  dir <- withr::local_tempdir()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:00Z"),
                list("v20260901-060000" = path))
  io$free_gib <- default_history_io()$free_gib
  work <- file.path(dir, "not", "made", "yet")
  got <- fetch_snapshot(io, families$`cran-metadata`, "v20260901-060000", work, 0)
  expect_true(dir.exists(work))
  expect_equal(unname(tools::md5sum(got$path)), unname(tools::md5sum(path)))
})

test_that("a work directory that cannot be made stops the run before any download", {
  dir <- withr::local_tempdir()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:00Z"),
                list("v20260901-060000" = path))
  expect_error(fetch_snapshot(io, families$`cran-metadata`, "v20260901-060000", path, 20),
               "could not make the work directory")
  expect_length(io$state$downloads, 0L)
  expect_length(io$state$sleeps, 0L)
})

test_that("a snapshot path a file: URI would misread is refused", {
  dir <- withr::local_tempdir()
  odd <- file.path(dir, "a?b")
  dir.create(odd)
  path <- metadata_snapshot(odd, "m.db", c("OK", "OK", "OK", "OK"))
  con <- history_test_db()
  expect_error(attach_snapshot(con, path), "file: URI")
})

test_that("a listing that reaches the limit stops the run", {
  con <- history_test_db()
  many <- releases(sprintf("v20260101-%06d", seq_len(1000)), "2026-01-01T00:00:00Z")
  io <- fake_io(many, list())
  expect_error(run_extraction(con, tempdir(), io, cran_only()), "the most one listing returns")
})

run_three <- function(con, dir, fail = list(), give_up = character(0)) {
  tags <- c("v20260901-060000", "v20260902-060000", "v20260903-060000")
  files <- list(metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK")),
                metadata_snapshot(dir, "m2.db", c("OK", "OK", "ERROR", "OK"), "2026-10-17"),
                metadata_snapshot(dir, "m3.db", c("OK", "NOTE", "ERROR", "OK"), "2026-10-17"))
  io <- fake_io(releases(tags, sprintf("2026-09-0%dT06:00:09Z", 1:3)),
                stats::setNames(files, tags), fail = fail)
  run_extraction(con, file.path(dir, "w"), io, cran_only(), give_up = give_up)
  io
}

test_that("each release is folded once, and a second run reads nothing", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  run_three(con, dir)
  snaps <- DBI::dbGetQuery(con, "SELECT tag, outcome, asset, sha256 FROM history_snapshots ORDER BY tag")
  expect_equal(snaps$outcome, rep("processed", 3))
  expect_false(anyNA(snaps$sha256))
  obs <- DBI::dbGetQuery(con, "SELECT series, outcome FROM history_series_observations
                                WHERE tag = 'v20260903-060000' ORDER BY series")
  expect_equal(obs$series, c("check_deadline", "check_flavor_status", "check_issue", "check_timing"))
  expect_equal(obs$outcome, rep("applied", 4))
  dl <- DBI::dbGetQuery(con, "SELECT episode_seq, deadline, ended_on FROM cran_check_deadline_history")
  expect_equal(dl$deadline, c("2026-10-10", "2026-10-17"))
  expect_equal(dl$ended_on, c("2026-09-02", NA))
  before <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cran_check_timing_history")$n
  io <- run_three(con, dir)
  expect_length(io$state$downloads, 0L)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cran_check_timing_history")$n, before)
  expect_equal(nrow(DBI::dbGetQuery(con, "SELECT * FROM history_snapshots")), 3L)
})

test_that("a run that stops partway resumes at the release it stopped on", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  expect_error(run_three(con, dir, fail = list("v20260902-060000" = 3L)), "after 3 attempts")
  expect_equal(DBI::dbGetQuery(con, "SELECT tag FROM history_snapshots")$tag, "v20260901-060000")
  io <- run_three(con, dir)
  expect_equal(io$state$downloads, c("v20260902-060000", "v20260903-060000"))
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM history_snapshots")$n, 3L)
})

test_that("a fold that fails leaves nothing of that release behind", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  path <- metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK"))
  io <- fake_io(releases("v20260901-060000", "2026-09-01T06:00:09Z"), list("v20260901-060000" = path))
  broken <- history_series()
  broken[[4]]$values <- function(cols) stop("the deadline columns could not be read")
  expect_error(run_extraction(con, file.path(dir, "w"), io, cran_only(), broken),
               "v20260901-060000: the deadline columns could not be read")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM history_snapshots")$n, 0L)
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM history_series_observations")$n, 0L)
  expect_false(history_table_exists(con, "cran_check_timing_history"))
  expect_false("snap" %in% DBI::dbGetQuery(con, "PRAGMA database_list")$name)
})

test_that("a release given up on is recorded as failed and the run goes on", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  io <- run_three(con, dir, give_up = "cran-metadata:v20260902-060000")
  expect_equal(io$state$downloads, c("v20260901-060000", "v20260903-060000"))
  got <- DBI::dbGetQuery(con, "SELECT tag, outcome FROM history_snapshots ORDER BY tag")
  expect_equal(got$outcome, c("processed", "failed", "processed"))
})

test_that("the status series stops at the release cran-metadata seeded from, and the timings go on", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  run_three(con, dir)
  path <- metadata_snapshot(dir, "m4.db", c("ERROR", "NOTE", "ERROR", "OK"), "2026-10-17", seeded = TRUE)
  io <- fake_io(releases("v20260904-060000", "2026-09-04T06:00:09Z"), list("v20260904-060000" = path))
  expect_error(run_extraction(con, file.path(dir, "w"), io, cran_only()),
               "v20260904-060000 carries its own .* seeded from v20260903-060000.*--flavor-handover=v20260903-060000")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM history_snapshots")$n, 3L)
  expect_error(record_flavor_handover(con, "v20260902-060000"), "newest folded")
  record_flavor_handover(con, "v20260903-060000")
  expect_error(record_flavor_handover(con, "v20260904-060000"), "already recorded")
  statuses <- DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cran_check_flavor_status_history")$n
  run_extraction(con, file.path(dir, "w"), io, cran_only())
  obs <- DBI::dbGetQuery(con, "SELECT series, outcome FROM history_series_observations
                                WHERE tag = 'v20260904-060000' ORDER BY series")
  expect_equal(obs$outcome[obs$series == "check_flavor_status"], "handed_over")
  expect_equal(obs$outcome[obs$series == "check_timing"], "applied")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM cran_check_flavor_status_history")$n,
               statuses)
  expect_error(withdraw_flavor_handover(con, "v20260903-060000", "2026-10-01T00:00:00Z"),
               "the handover stands")
})

test_that("a handover no seed followed stops the next release until it is withdrawn", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  run_three(con, dir)
  record_flavor_handover(con, "v20260903-060000")
  path <- metadata_snapshot(dir, "m4.db", c("ERROR", "NOTE", "ERROR", "OK"), "2026-10-17")
  io <- fake_io(releases("v20260904-060000", "2026-09-04T06:00:09Z"), list("v20260904-060000" = path))
  expect_error(run_extraction(con, file.path(dir, "w"), io, cran_only()),
               "v20260904-060000 has no per-flavor .*--withdraw-flavor-handover=v20260903-060000")
  expect_equal(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM history_snapshots")$n, 3L)
  expect_error(withdraw_flavor_handover(con, "v20260902-060000", "2026-10-01T00:00:00Z"),
               "no flavor status handover is recorded at v20260902-060000")
  withdraw_flavor_handover(con, "v20260903-060000", "2026-10-01T00:00:00Z")
  expect_true(is.na(history_setting(con, "flavor_handover_tag")))
  expect_equal(history_setting(con, "flavor_handover_withdrawn:2026-10-01T00:00:00Z"), "v20260903-060000")
  run_extraction(con, file.path(dir, "w"), io, cran_only())
  expect_equal(DBI::dbGetQuery(con, "SELECT outcome FROM history_series_observations
                                     WHERE tag = 'v20260904-060000' AND series = 'check_flavor_status'")$outcome,
               "applied")
})

test_that("a correction note is added to a folded release", {
  dir <- withr::local_tempdir()
  con <- history_test_db()
  run_three(con, dir)
  record_note(con, "cran-metadata", "v20260902-060000", "correction: first run with version")
  record_note(con, "cran-metadata", "v20260902-060000", "second note")
  expect_equal(DBI::dbGetQuery(con, "SELECT note FROM history_snapshots
                                     WHERE tag = 'v20260902-060000'")$note,
               "correction: first run with version; second note")
  expect_error(record_note(con, "cran-metadata", "v20990101-000000", "x"), "no folded")
})

test_that("the command line starts fresh, records the handover first and adds notes", {
  dir <- withr::local_tempdir()
  work <- file.path(dir, "work")
  files <- list("v20260901-060000" = metadata_snapshot(dir, "m1.db", c("OK", "OK", "ERROR", "OK")),
                "v20260902-060000" = metadata_snapshot(dir, "m2.db", c("OK", "NOTE", "ERROR", "OK")))
  io <- fake_io(releases(names(files), c("2026-09-01T06:00:09Z", "2026-09-02T06:00:09Z")), files)
  io$release_http_status <- function(repo, tag) 404L
  expect_error(history_main(c(paste0("--workdir=", work), "--only=cran"), io), "--only takes")
  expect_error(history_main(c(paste0("--workdir=", work), "--flavor-handover=v20260902-060000",
                              "--withdraw-flavor-handover=v20260902-060000"), io), "not both")
  capture.output(history_main(c(paste0("--workdir=", work), "--only=cran-metadata",
                                "--note=cran-metadata:v20260902-060000=correction: noted"), io))
  files[["v20260903-060000"]] <- metadata_snapshot(dir, "m3.db", c("ERROR", "NOTE", "ERROR", "OK"),
                                                   seeded = TRUE)
  io <- fake_io(releases(names(files), sprintf("2026-09-0%dT06:00:09Z", 1:3)), files)
  expect_error(capture.output(history_main(c(paste0("--workdir=", work), "--only=cran-metadata"), io)),
               "--flavor-handover=v20260902-060000")
  capture.output(history_main(c(paste0("--workdir=", work), "--only=cran-metadata",
                                "--flavor-handover=v20260902-060000"), io))
  expect_error(capture.output(history_main(c(paste0("--workdir=", work), "--only=cran-metadata",
                                             "--withdraw-flavor-handover=v20260902-060000"), io)),
               "the handover stands")
  con <- DBI::dbConnect(RSQLite::SQLite(), file.path(work, "history.db"))
  on.exit(DBI::dbDisconnect(con))
  expect_equal(DBI::dbGetQuery(con, "SELECT note FROM history_snapshots
                                     WHERE tag = 'v20260902-060000'")$note, "correction: noted")
  expect_equal(history_setting(con, "flavor_handover_tag"), "v20260902-060000")
  expect_equal(DBI::dbGetQuery(con, "SELECT outcome FROM history_series_observations
                                     WHERE tag = 'v20260903-060000' AND series = 'check_flavor_status'")$outcome,
               "handed_over")
})
