# Uploads the built pairs to r-observatory/data@history. Run only when the
# owner asks for the upload.
#
#   Rscript scripts/history/publish.R --workdir=DIR --stamp=YYYY-MM-DD

HISTORY_RELEASE_NOTES <- paste(
  "Small history series folded out of the dated cran-metadata and observatory.db releases,",
  "as episode tables. history-YYYY-MM-DD.db.zst is the full set; history-merge-YYYY-MM-DD.db.zst",
  "holds only the tables observatory.db merges. Each has a manifest with its sha256.",
  "Assets are added, never replaced.")

# The one `gh release create` for this tag. A prerelease is never Latest, and
# readers that give no tag resolve Latest.
history_release_create_args <- function(repo = HISTORY_REPO) {
  c("release", "create", HISTORY_RELEASE_TAG, "--repo", repo,
    "--prerelease", "--latest=false",
    "--title", "History series", "--notes", HISTORY_RELEASE_NOTES)
}

publish_history <- function(workdir, stamp, io = default_history_io()) {
  out <- file.path(workdir, "out")
  problems <- validate_history_assets(out, stamp)
  if (length(problems)) {
    stop(paste(c("the built pairs fail their checks:", problems), collapse = "\n"), call. = FALSE)
  }
  files <- file.path(out, unlist(history_asset_names(stamp), use.names = FALSE))
  if (history_release_status(io) == "present") {
    assets <- io$asset_info(HISTORY_REPO, HISTORY_RELEASE_TAG)
    clash <- intersect(basename(files), assets$name)
    if (length(clash)) {
      stop("already on the release, and assets are never replaced: ",
           paste(clash, collapse = ", "), call. = FALSE)
    }
    prior <- newest_history_pair(assets, "full")
    if (!is.null(prior)) {
      prior_db <- tempfile("history-prior-", fileext = ".db")
      on.exit(unlink(prior_db), add = TRUE)
      fetch_history_pair(io, prior, prior_db)
      lost <- history_regressions(prior_db, file.path(out, sprintf("history-%s.db", stamp)))
      if (length(lost)) {
        stop(paste(c(sprintf("refusing to publish over %s:", prior$db), lost), collapse = "\n"),
             call. = FALSE)
      }
    }
  } else if (!isTRUE(io$create_release(history_release_create_args()))) {
    stop("could not create the history release", call. = FALSE)
  }
  if (!isTRUE(io$upload(HISTORY_REPO, HISTORY_RELEASE_TAG, files))) {
    stop("the upload failed; look at the release before running again", call. = FALSE)
  }
  after <- io$asset_info(HISTORY_REPO, HISTORY_RELEASE_TAG)
  got <- after[match(basename(files), after$name), , drop = FALSE]
  if (anyNA(got$name) || any(got$state != "uploaded") || any(got$size != file.size(files))) {
    stop("the release does not list all four assets at their sizes after the upload", call. = FALSE)
  }
  message("published ", paste(basename(files), collapse = ", "))
  invisible(basename(files))
}

if (sys.nframe() == 0L) {
  here <- dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)))
  for (f in c("fold.R", "check_flavor_fold.R", "series.R", "assets.R", "extract.R")) {
    source(file.path(here, f))
  }
  args <- commandArgs(trailingOnly = TRUE)
  arg_value <- function(flag) sub(paste0("^--", flag, "="), "", grep(paste0("^--", flag, "="), args, value = TRUE))
  if (length(arg_value("workdir")) != 1L || length(arg_value("stamp")) != 1L) {
    stop("give --workdir=DIR and --stamp=YYYY-MM-DD", call. = FALSE)
  }
  publish_history(arg_value("workdir"), arg_value("stamp"))
}
