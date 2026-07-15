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
#' rolling   = publishes to a rolling `current` tag (else latest dated release)
#' manifest  = publishes a freshness manifest as a release asset
#' manifest_file = the manifest asset's filename (default "manifest.json"; the
#'             metrics pipelines publish "code-manifest.json" instead)
#' db_filename = the source DB this pipeline contributes, as it lands in
#'             sources/<db_filename> during the merge (used to stat + sha256 the
#'             merged-in file); omit for the merger itself
#' upstream  = upstream source repo to compare against (lag), or NULL
#' self      = this is the merger itself (its "last run" is now)
pipeline_config <- function() {
  list(
    list(name = "cran-queue",     repo = "r-observatory/cran-queue",
         schedule = "hourly",          max_age_h = 3L,    rolling = FALSE, manifest = FALSE,
         db_filename = "queue.db"),
    list(name = "cran-feed",      repo = "r-observatory/cran-feed",
         schedule = "every 6 hours",   max_age_h = 8L,    rolling = FALSE, manifest = FALSE,
         db_filename = "feed.db"),
    list(name = "cran-metadata",  repo = "r-observatory/cran-metadata",
         schedule = "daily 06:00 UTC", max_age_h = 30L,   rolling = FALSE, manifest = FALSE,
         db_filename = "metadata.db"),
    list(name = "cran-downloads", repo = "r-observatory/cran-downloads",
         schedule = "daily 07:00 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE,
         db_filename = "downloads-summary.db"),
    list(name = "r2u-downloads",  repo = "r-observatory/r2u-downloads",
         schedule = "daily 06:00 UTC", max_age_h = 35L * 24L, rolling = TRUE, manifest = TRUE,
         upstream = "eddelbuettel/r2u-logs", db_filename = "r2u-summary.db"),
    list(name = "autoobs-downloads", repo = "r-observatory/autoobs-downloads",
         schedule = "daily 04:00 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE,
         db_filename = "autoobs-downloads-summary.db"),
    list(name = "copr-downloads", repo = "r-observatory/copr-downloads",
         schedule = "daily 05:30 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE,
         db_filename = "copr-downloads-summary.db"),
    list(name = "conda-forge-downloads", repo = "r-observatory/conda-forge-downloads",
         schedule = "daily 05:00 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE,
         db_filename = "conda-forge-downloads-summary.db"),
    list(name = "bioconda-downloads", repo = "r-observatory/bioconda-downloads",
         schedule = "daily 05:15 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE,
         db_filename = "bioconda-downloads-summary.db"),
    list(name = "c2d4u-downloads", repo = "r-observatory/c2d4u-downloads",
         schedule = "monthly 3rd 05:45 UTC", max_age_h = 45L * 24L, rolling = TRUE, manifest = TRUE,
         db_filename = "c2d4u-downloads-summary.db"),
    list(name = "vcs-signals",    repo = "r-observatory/vcs-signals",
         schedule = "daily 06:30 UTC", max_age_h = 30L,   rolling = TRUE,  manifest = TRUE,
         db_filename = "vcs-signals-summary.db"),
    # Metrics pipelines ship a metrics-style manifest (code-manifest.json) that
    # carries bootstrap.bootstrap_complete, and publish to a dated `metrics-*`
    # release marked latest (not a rolling `current` tag). Each also ships a
    # sibling *-data-metrics.db; only the primary code DB is tracked here.
    list(name = "cran-code-metrics", repo = "r-observatory/cran-code-metrics",
         schedule = "daily 04:00 UTC", max_age_h = 30L,   rolling = FALSE, manifest = TRUE,
         manifest_file = "code-manifest.json", db_filename = "cran-code-metrics.db"),
    list(name = "bioc-code-metrics", repo = "r-observatory/bioc-code-metrics",
         schedule = "daily 04:00 UTC", max_age_h = 30L,   rolling = FALSE, manifest = TRUE,
         manifest_file = "code-manifest.json", db_filename = "bioc-code-metrics.db"),
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

#' Latest data date from a manifest: its per-shard coverage map, falling back to a
#' top-level summary$data_through (pipelines that publish that instead of a shard map).
manifest_data_through <- function(man) {
  dt <- max_data_through(man$shards)
  if (is.na(dt)) dt <- man$summary$data_through %||% NA_character_
  dt
}

#' Short human description of what changed in a manifest's last run (or NA).
changed_summary <- function(manifest) {
  if (is.null(manifest)) return(NA_character_)
  n <- length(manifest$changed_shards %||% list())
  if (n == 0) return("no change last run")
  sprintf("%d shard%s changed last run", n, if (n == 1L) "" else "s")
}

#' Completeness flag (1 = complete, 0 = incomplete) read generically from a
#' manifest, or NA when the manifest exposes no completeness signal. Prefers a
#' standardized top-level `complete`, then a metrics-style
#' `bootstrap$bootstrap_complete`. Most sources publish neither today, so NA
#' (honest unknown) is the common and correct result.
manifest_complete <- function(man) {
  if (is.null(man)) return(NA_integer_)
  flag <- man$complete %||% man$bootstrap$bootstrap_complete
  if (is.null(flag)) return(NA_integer_)
  if (is.logical(flag)) return(as.integer(isTRUE(flag)))
  if (is.numeric(flag)) return(as.integer(flag != 0))
  NA_integer_
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
      data_through           = manifest_data_through(man),
      changed_summary        = changed_summary(man),
      upstream_repo          = cfg$upstream %||% NA_character_,
      upstream_latest_at     = up$latest_at %||% NA_character_,
      behind_upstream        = behind,
      fetched_at             = now_iso,
      # Integrity of the merged-in source DB (from sources/<db_filename>) plus
      # the manifest's completeness flag. All honest-NA when unknown: a torn or
      # absent file yields NA bytes/sha (never a coerced 0), and a source that
      # publishes no completeness flag yields NA complete.
      db_bytes               = as.numeric(f$integrity$bytes %||% NA_real_),
      db_sha256              = f$integrity$sha256 %||% NA_character_,
      complete               = manifest_complete(man),
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
    fetched_at             TEXT,
    db_bytes               INTEGER,
    db_sha256              TEXT,
    complete               INTEGER)")
  if (nrow(df) > 0) DBI::dbWriteTable(con, "pipeline_metadata", df, append = TRUE)
  invisible(NULL)
}

# ---------------------------------------------------------------------------
# Impure collection (gh), injectable for tests
# ---------------------------------------------------------------------------

#' sha256 of a file as a lowercase hex string, using whatever the runner has:
#' the digest package (which streams the file) when installed, else the shell
#' `sha256sum` (ubuntu-latest) or `shasum -a 256` (macOS). NA when none exists.
sha256_file <- function(path) {
  if (requireNamespace("digest", quietly = TRUE)) {
    return(digest::digest(file = path, algo = "sha256"))
  }
  ss <- Sys.which("sha256sum")
  if (nzchar(ss)) {
    out <- suppressWarnings(system2(ss, shQuote(path), stdout = TRUE, stderr = FALSE))
    if (length(out) && nzchar(out[1])) return(sub("\\s.*$", "", out[1]))
  }
  sh <- Sys.which("shasum")
  if (nzchar(sh)) {
    out <- suppressWarnings(system2(sh, c("-a", "256", shQuote(path)),
                                    stdout = TRUE, stderr = FALSE))
    if (length(out) && nzchar(out[1])) return(sub("\\s.*$", "", out[1]))
  }
  NA_character_
}

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
    # Fetch a pipeline's freshness manifest. `manifest_file` lets metrics repos
    # expose a "code-manifest.json" while everyone else uses "manifest.json";
    # `tag` is the rolling "current" for most, or NULL (latest release) for the
    # dated metrics repos.
    manifest = function(repo, manifest_file = "manifest.json", tag = "current") {
      dir <- tempfile("man"); dir.create(dir)
      on.exit(unlink(dir, recursive = TRUE), add = TRUE)
      args <- c("release", "download")
      if (!is.null(tag)) args <- c(args, tag)
      args <- c(args, "--repo", repo, "--pattern", manifest_file, "--dir", dir)
      suppressWarnings(system2("gh", args, stdout = TRUE, stderr = TRUE))
      mp <- file.path(dir, manifest_file)
      if (!file.exists(mp)) return(NULL)
      jsonlite::fromJSON(mp, simplifyVector = FALSE)
    },
    # Stat + sha256 the merged-in source DB in sources/<db_filename>. Honest NA
    # (never 0) when the pipeline contributes no DB or the file is absent.
    db_integrity = function(cfg) {
      fn <- cfg$db_filename
      if (is.null(fn) || !nzchar(fn)) return(list(bytes = NA_real_, sha256 = NA_character_))
      path <- file.path("sources", fn)
      if (!file.exists(path)) return(list(bytes = NA_real_, sha256 = NA_character_))
      list(bytes = as.numeric(file.size(path)), sha256 = sha256_file(path))
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
    tag <- if (isTRUE(cfg$rolling)) "current" else NULL
    release <- if (isTRUE(cfg$self)) NULL else io$release(cfg$repo, tag)
    manifest <- if (isTRUE(cfg$manifest)) {
      io$manifest(cfg$repo, cfg$manifest_file %||% "manifest.json", tag)
    } else NULL
    upstream <- if (!is.null(cfg$upstream)) io$upstream(cfg$upstream) else NULL
    repo_commit <- if (isTRUE(cfg$self)) NA_character_ else io$repo_commit(cfg$repo)
    integrity <- io$db_integrity(cfg)
    out[[cfg$name]] <- list(cfg = cfg, release = release, manifest = manifest,
                            upstream = upstream, repo_commit = repo_commit,
                            integrity = integrity)
  }
  out
}
