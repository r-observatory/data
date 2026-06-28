# pipeline_metadata.R — collect per-pipeline freshness into observatory.db.
#
# Powers the viewer's data-freshness page. The merger is the natural place to
# gather this: it already runs daily after every pipeline and has GitHub access.
# The pure builder (build_pipeline_metadata) is unit-tested; the gh fetching
# (collect_pipeline_metadata) is injectable via an `io` list.

`%||%` <- function(a, b) if (is.null(a)) b else a

#' The pipelines tracked on the freshness page, with display cadence and the
#' staleness threshold the viewer uses to flag a pipeline.
#'
#' rolling  = publishes to a rolling `current` tag (else latest dated release)
#' manifest = publishes a manifest.json with freshness fields
#' upstream = upstream source repo to compare against (lag), or NULL
#' self     = this is the merger itself (its "last run" is now)
pipeline_config <- function() {
  list(
    list(name = "cran-queue",     repo = "r-observatory/cran-queue",
         schedule = "hourly",          max_age_h = 3L,    rolling = FALSE, manifest = FALSE),
    list(name = "cran-feed",      repo = "r-observatory/cran-feed",
         schedule = "every 6 hours",   max_age_h = 8L,    rolling = FALSE, manifest = FALSE),
    list(name = "cran-metadata",  repo = "r-observatory/cran-metadata",
         schedule = "daily 06:00 UTC", max_age_h = 30L,   rolling = FALSE, manifest = FALSE),
    list(name = "cran-downloads", repo = "r-observatory/cran-downloads",
         schedule = "daily 07:00 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE),
    list(name = "r2u-downloads",  repo = "r-observatory/r2u-downloads",
         schedule = "daily 06:00 UTC", max_age_h = 35L * 24L, rolling = TRUE, manifest = TRUE,
         upstream = "eddelbuettel/r2u-logs"),
    list(name = "data",           repo = "r-observatory/data",
         schedule = "daily 08:00 UTC", max_age_h = 30L,   rolling = FALSE, manifest = FALSE,
         self = TRUE)
  )
}

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

#' Latest data date across a manifest's per-shard coverage map (or NA).
max_data_through <- function(shards) {
  if (is.null(shards) || length(shards) == 0) return(NA_character_)
  dm <- vapply(shards, function(s) s$date_max %||% NA_character_, character(1))
  dm <- dm[!is.na(dm) & nzchar(dm)]
  if (length(dm) == 0) NA_character_ else max(dm)
}

#' Short human description of what changed in a manifest's last run (or NA).
changed_summary <- function(manifest) {
  if (is.null(manifest)) return(NA_character_)
  n <- length(manifest$changed_shards %||% list())
  if (n == 0) return("no change last run")
  sprintf("%d shard%s changed last run", n, if (n == 1L) "" else "s")
}

#' Build the pipeline_metadata data.frame from already-fetched per-pipeline data.
#'
#' @param fetched named list keyed by pipeline name; each entry is
#'   list(cfg, release = list(tag, published_at) | NULL,
#'        manifest = parsed manifest | NULL,
#'        upstream = list(latest_sha, latest_at) | NULL)
#' @param now_iso ISO-8601 timestamp string for this collection run
#' @return data.frame matching the pipeline_metadata schema
build_pipeline_metadata <- function(fetched, now_iso) {
  rows <- lapply(fetched, function(f) {
    cfg <- f$cfg
    rel <- f$release
    man <- f$manifest
    up  <- f$upstream

    # "last ran" proxy: the most recent of the release time and the repo's last
    # commit (every pipeline commits a timestamp each run), overridden by the
    # manifest's own last_checked when a pipeline publishes one.
    run_candidates <- c(rel$published_at, f$repo_commit)
    run_candidates <- run_candidates[!is.na(run_candidates) & nzchar(run_candidates)]
    run_at <- if (length(run_candidates)) max(run_candidates) else NA_character_

    released_at  <- if (isTRUE(cfg$self)) now_iso else (rel$published_at %||% NA_character_)
    last_checked <- if (isTRUE(cfg$self)) now_iso else (man$last_checked %||% run_at)
    last_changed <- if (isTRUE(cfg$self)) now_iso else (man$last_changed %||% run_at)

    behind <- NA_integer_
    if (!is.null(cfg$upstream) && !is.null(up) && !is.null(man$upstream_head_sha)) {
      behind <- as.integer(!identical(man$upstream_head_sha, up$latest_sha))
    }

    data.frame(
      pipeline               = cfg$name,
      repo                   = cfg$repo,
      schedule               = cfg$schedule,
      expected_max_age_hours = cfg$max_age_h,
      release_tag            = rel$tag %||% NA_character_,
      released_at            = released_at,
      last_checked           = last_checked,
      last_changed           = last_changed,
      data_through           = max_data_through(man$shards),
      changed_summary        = changed_summary(man),
      upstream_repo          = cfg$upstream %||% NA_character_,
      upstream_latest_at     = up$latest_at %||% NA_character_,
      behind_upstream        = behind,
      fetched_at             = now_iso,
      stringsAsFactors       = FALSE
    )
  })
  do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# Table writer
# ---------------------------------------------------------------------------

write_pipeline_metadata <- function(con, df) {
  DBI::dbExecute(con, "DROP TABLE IF EXISTS pipeline_metadata")
  DBI::dbExecute(con, "CREATE TABLE pipeline_metadata (
    pipeline               TEXT PRIMARY KEY,
    repo                   TEXT,
    schedule               TEXT,
    expected_max_age_hours INTEGER,
    release_tag            TEXT,
    released_at            TEXT,
    last_checked           TEXT,
    last_changed           TEXT,
    data_through           TEXT,
    changed_summary        TEXT,
    upstream_repo          TEXT,
    upstream_latest_at     TEXT,
    behind_upstream        INTEGER,
    fetched_at             TEXT)")
  if (nrow(df) > 0) DBI::dbWriteTable(con, "pipeline_metadata", df, append = TRUE)
  invisible(NULL)
}

# ---------------------------------------------------------------------------
# Impure collection (gh), injectable for tests
# ---------------------------------------------------------------------------

default_meta_io <- function() {
  gh_json <- function(args) {
    out <- suppressWarnings(system2("gh", args, stdout = TRUE, stderr = TRUE))
    if (!identical(as.integer(attr(out, "status") %||% 0L), 0L)) return(NULL)
    paste(out, collapse = "\n")
  }
  list(
    release = function(repo, tag) {
      args <- c("release", "view")
      if (!is.null(tag)) args <- c(args, tag)
      args <- c(args, "--repo", repo, "--json", "tagName,publishedAt")
      js <- gh_json(args)
      if (is.null(js)) return(NULL)
      r <- jsonlite::fromJSON(js, simplifyVector = TRUE)
      list(tag = r$tagName, published_at = r$publishedAt)
    },
    manifest = function(repo) {
      dir <- tempfile("man"); dir.create(dir)
      on.exit(unlink(dir, recursive = TRUE), add = TRUE)
      st <- suppressWarnings(system2("gh",
        c("release", "download", "current", "--repo", repo,
          "--pattern", "manifest.json", "--dir", dir),
        stdout = TRUE, stderr = TRUE))
      mp <- file.path(dir, "manifest.json")
      if (!file.exists(mp)) return(NULL)
      jsonlite::fromJSON(mp, simplifyVector = FALSE)
    },
    # NOTE: keep jq expressions free of braces/spaces. system2() runs through a
    # shell, so "{a,b}" would brace-expand into two args and gh would reject it.
    upstream = function(repo) {
      clean <- function(s) if (is.null(s)) NA_character_ else trimws(gsub('"', "", s))
      sha <- gh_json(c("api", sprintf("repos/%s/commits/master", repo), "--jq", ".sha"))
      at  <- gh_json(c("api", sprintf("repos/%s/commits/master", repo), "--jq", ".commit.committer.date"))
      if (is.null(sha)) return(NULL)
      list(latest_sha = clean(sha), latest_at = clean(at))
    },
    repo_commit = function(repo) {
      js <- gh_json(c("api", sprintf("repos/%s/commits/main", repo),
                      "--jq", ".commit.committer.date"))
      if (is.null(js)) return(NA_character_)
      trimws(gsub('"', "", js))
    }
  )
}

#' Fetch raw per-pipeline data via `io` (defaults to gh).
collect_pipeline_metadata <- function(config = pipeline_config(), io = default_meta_io()) {
  out <- list()
  for (cfg in config) {
    release <- if (isTRUE(cfg$self)) NULL else io$release(cfg$repo, if (isTRUE(cfg$rolling)) "current" else NULL)
    manifest <- if (isTRUE(cfg$manifest)) io$manifest(cfg$repo) else NULL
    upstream <- if (!is.null(cfg$upstream)) io$upstream(cfg$upstream) else NULL
    repo_commit <- if (isTRUE(cfg$self)) NA_character_ else io$repo_commit(cfg$repo)
    out[[cfg$name]] <- list(cfg = cfg, release = release, manifest = manifest,
                            upstream = upstream, repo_commit = repo_commit)
  }
  out
}
