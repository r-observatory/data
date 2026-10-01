for (f in c("fold.R", "check_flavor_fold.R", "series.R", "extract.R", "assets.R", "publish.R")) {
  source(file.path(getwd(), "..", "..", "history", f))
}

repo_root <- normalizePath(file.path(getwd(), "..", "..", ".."))
pair_names <- function(stamp) unlist(history_asset_names(stamp), use.names = FALSE)

test_that("the history release is created as a prerelease that is never Latest", {
  args <- history_release_create_args()
  expect_equal(args[1:3], c("release", "create", "history"))
  expect_true(all(c("--prerelease", "--latest=false") %in% args))
  expect_equal(args[which(args == "--repo") + 1L], "r-observatory/data")
})

test_that("nothing here deletes the history release, and only one step creates it", {
  code <- c(list.files(file.path(repo_root, "scripts"), "\\.R$", recursive = TRUE, full.names = TRUE),
            list.files(file.path(repo_root, ".github", "workflows"), "\\.ya?ml$", full.names = TRUE))
  code <- code[!grepl("/tests/", code)]
  deletion <- "release[[:space:]\"',]+delete|delete-asset|--delete|DELETE"
  for (f in code) {
    hits <- grep(deletion, readLines(f, warn = FALSE), value = TRUE)
    expect_false(any(grepl("history", hits, ignore.case = TRUE)), info = f)
    if (grepl("/history/", f)) expect_length(hits, 0L)
  }
  creates <- grep("\"release\", \"create\"|release create", unlist(lapply(code, readLines)),
                  value = TRUE)
  expect_equal(sum(grepl("HISTORY_RELEASE_TAG|history", creates)), 1L)
})

test_that("a first publish creates the release and uploads both pairs", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  io <- release_io(store, 404L)
  publish_history(b$dir, "2026-10-01", io)
  expect_equal(io$state$created, history_release_create_args())
  expect_setequal(io$state$uploaded, pair_names("2026-10-01"))
})

test_that("a later publish reads the prior pair and refuses one that lost rows", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  publish_history(b$dir, "2026-10-01", release_io(store, 404L))
  con <- DBI::dbConnect(RSQLite::SQLite(), b$history)
  DBI::dbExecute(con, "DELETE FROM cran_check_timing_history WHERE package = 'cli'")
  DBI::dbDisconnect(con)
  build_history_assets(b$history, b$out, "2026-10-02", "2026-10-02T09:00:00Z")
  io <- release_io(store, 200L)
  expect_error(publish_history(b$dir, "2026-10-02", io),
               "refusing to publish over history-2026-10-01.db.zst")
  expect_length(io$state$uploaded, 0L)
})

test_that("a later publish that only grows goes up beside the prior pair", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  publish_history(b$dir, "2026-10-01", release_io(store, 404L))
  build_history_assets(b$history, b$out, "2026-10-02", "2026-10-02T09:00:00Z")
  io <- release_io(store, 200L)
  publish_history(b$dir, "2026-10-02", io)
  expect_null(io$state$created)
  expect_setequal(list.files(store), c(pair_names("2026-10-01"), pair_names("2026-10-02")))
})

test_that("a prior pair that cannot be read stops the publish", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  publish_history(b$dir, "2026-10-01", release_io(store, 404L))
  writeBin(as.raw(1:10), file.path(store, "history-2026-10-01.db.zst"))
  build_history_assets(b$history, b$out, "2026-10-02", "2026-10-02T09:00:00Z")
  io <- release_io(store, 200L)
  expect_error(publish_history(b$dir, "2026-10-02", io), "could not be read")
  expect_length(io$state$uploaded, 0L)
})

test_that("an asset already on the release is never replaced", {
  skip_without_zstd()
  b <- built()
  store <- withr::local_tempdir()
  publish_history(b$dir, "2026-10-01", release_io(store, 404L))
  io <- release_io(store, 200L)
  expect_error(publish_history(b$dir, "2026-10-01", io), "never replaced")
  expect_length(io$state$uploaded, 0L)
})

test_that("a release whose state cannot be read is never created", {
  skip_without_zstd()
  b <- built()
  io <- release_io(withr::local_tempdir(), 503L)
  expect_error(publish_history(b$dir, "2026-10-01", io), "HTTP 503")
  expect_null(io$state$created)
})

test_that("a handover withdrawn after its pair went up does not block the next publish", {
  skip_without_zstd()
  b <- built()
  con <- DBI::dbConnect(RSQLite::SQLite(), b$history)
  record_flavor_handover(con, "v20260902-060000")
  DBI::dbDisconnect(con)
  build_history_assets(b$history, b$out, "2026-10-02", "2026-10-02T09:00:00Z")
  store <- withr::local_tempdir()
  publish_history(b$dir, "2026-10-02", release_io(store, 404L))
  con <- DBI::dbConnect(RSQLite::SQLite(), b$history)
  withdraw_flavor_handover(con, "v20260902-060000", "2026-10-03T00:00:00Z")
  DBI::dbDisconnect(con)
  build_history_assets(b$history, b$out, "2026-10-03", "2026-10-03T09:00:00Z")
  publish_history(b$dir, "2026-10-03", release_io(store, 200L))
  handed <- jsonlite::read_json(file.path(store, "history-2026-10-02-manifest.json"))
  expect_equal(handed$flavor_handover_tag, "v20260902-060000")
  expect_null(jsonlite::read_json(file.path(store, "history-2026-10-03-manifest.json"))$flavor_handover_tag)
})
