# merge_gate.R - decide whether the merged observatory.db may be published.
#
# The merge rebuilds observatory.db from scratch every run, so a source that
# fails to download does not go stale: its tables disappear outright. Until now
# the only guard was "at least one source merged", so nearly every source could
# vanish and the run would still go green.
#
# Two separate questions are answered here, and they have different answers:
#
#   1. May we publish at all? Only a source whose loss visibly breaks the site
#      is allowed to stop the release, because refusing to publish costs every
#      other source a full day of freshness. A false positive here is worse
#      than no gate, so the fatal set is deliberately tiny and its thresholds
#      sit far below live values.
#
#   2. Is anything wrong? Everything else publishes and then fails the run, so
#      the data still ships and a human still finds out.
#
# Freshness is keyed on last_checked, never on last_changed. conda-forge,
# bioconda and r2u run every day but their upstream publishes month-complete
# data, so their last_changed sits still for weeks at a time while they are
# working perfectly. last_checked answers "did this pipeline run", which is the
# question a broken pipeline actually fails.
#
# Every function here is pure and takes its inputs, including "now", as
# arguments. There is no mocking available in this repo, so injection is the
# only seam.

`%||%` <- function(a, b) if (is.null(a)) b else a

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

#' Multiple of a pipeline's own max_age_h at which the gate fails the run.
#'
#' The viewer's freshness page (php-site/templates/pages/freshness.php) buckets
#' a pipeline as current below 1x max_age_h, slow between 1x and 2x, and stale
#' above 2x. The gate fails at the viewer's "stale" boundary so a red run and
#' the public page can never disagree about the same pipeline, and reports
#' "slow" without failing.
#'
#' The tight thresholds are the reason: cran-queue declares 3 hours on an
#' hourly cron and cran-coverage declares 8 hours on a six-hourly one, and
#' GitHub's own scheduled-run backlog routinely eats a large fraction of that.
#' Firing at 1x would redden the daily merge on scheduler jitter rather than on
#' a broken pipeline. Set this to 1 to fail at the viewer's "slow" boundary
#' instead.
gate_stale_multiplier <- function() 2

#' Fallback staleness threshold, in hours, when a pipeline declares none.
#' Matches the viewer's own default.
gate_default_max_age_h <- function() 30

#' Floor on the window the gate is willing to assert against, in hours.
#'
#' The viewer's freshness page is re-rendered continuously, so it can resolve a
#' 3 hour window. The merge samples once a day, at an hour it does not control:
#' GitHub starts the 08:00 UTC cron anywhere from 1.6 to 4.1 hours late, and the
#' gate runs further in still. A window shorter than that jitter cannot be
#' distinguished from a healthy pipeline that skipped one tick, so nothing finer
#' than this floor is asserted. cran-queue (3h) and cran-feed (8h) are the
#' sources this protects. The declared window is still what gets reported, so
#' the gate and the public page never disagree about the number itself.
gate_min_stale_window_h <- function() 12

#' The verdicts that mean a source's data is genuinely not there.
#'
#' Only these escalate to fatal, and only for a source in gate_fatal_sources().
#' A fatal source that is present, complete and merely stale still publishes:
#' the database on disk is the previous release, complete and identical to what
#' shipped green yesterday, so publishing it is strictly better than refusing.
#' Staleness reddens the run and files the issue exactly like every other
#' source.
gate_fatal_verdicts <- function() c("corrupt", "missing", "short rows")

#' Floor on the uncompressed observatory.db, in bytes.
#'
#' The four metrics source DBs are roughly 2.9 GB of a 3.09 GB output, so
#' losing them shrinks the published asset by about 94 percent without any
#' per-table check noticing. This floor sits below "lost one metrics source"
#' (which lands near 1.8 to 1.9 GB) and well above "lost all of them" (near
#' 190 MB), so it catches the whole class without firing on a single source.
#' Non-fatal: it fails the run, it does not block the release.
gate_min_output_bytes <- function() 1500000000

#' The only sources whose absence stops the release.
#'
#' feed.db: `packages` is referenced throughout the viewer with almost no
#' table_exists guards, merge.R cannot build the FTS5 search index without it,
#' the viewer's enrich-db.php queries it unguarded with exceptions enabled, and
#' the viewer's own deploy gate refuses a database with fewer than 15000
#' packages. Publishing without it ships a database the viewer will reject.
#'
#' metadata.db: losing it empties the entire Authors area plus /checks, /health
#' and every package page's check panel while every page still returns 200.
#' That is the silent-degradation case this gate exists to catch.
#'
#' Row floors are per table and sit far below live values (packages 24331,
#' authors 62393, cran_check_results 315848). The packages floor deliberately
#' mirrors the viewer's own 15000, so the merger never publishes something the
#' viewer would refuse. removal_reasons legitimately ships zero rows, so no
#' blanket "every table from this source is non-empty" rule is applied.
gate_fatal_sources <- function() {
  list(
    "feed.db" = list(
      pipeline = "cran-feed",
      floors = c(packages = 15000)
    ),
    "metadata.db" = list(
      pipeline = "cran-metadata",
      floors = c(authors = 1000, cran_check_results = 1000)
    )
  )
}

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

#' Seconds since epoch for an ISO-8601 UTC timestamp, or NA when the value is
#' absent, empty or in a shape we do not recognise. Unparseable never means
#' stale: it means unknown, and unknown never fails the run.
gate_parse_time <- function(x) {
  if (is.null(x) || length(x) != 1L) return(NA_real_)
  if (is.na(x)) return(NA_real_)
  s <- trimws(as.character(x))
  if (!nzchar(s)) return(NA_real_)
  s <- sub("Z$", "", s)
  s <- sub("\\.[0-9]+$", "", s)
  t <- suppressWarnings(as.POSIXct(s, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC"))
  if (length(t) != 1L || is.na(t)) return(NA_real_)
  as.numeric(t)
}

#' Hours between an ISO-8601 reference time and "now", or NA if either is
#' unusable. A reference in the future yields a negative age, which reads as
#' fresh rather than as an error.
gate_age_hours <- function(ref_iso, now_iso) {
  r <- gate_parse_time(ref_iso)
  n <- gate_parse_time(now_iso)
  if (is.na(r) || is.na(n)) return(NA_real_)
  (n - r) / 3600
}

#' First element of `xs` that is a usable non-empty string, else NA.
gate_first_present <- function(xs) {
  for (x in xs) {
    if (is.null(x) || length(x) != 1L) next
    if (is.na(x)) next
    s <- as.character(x)
    if (nzchar(trimws(s))) return(s)
  }
  NA_character_
}

#' Classify one pipeline's freshness the way the viewer's freshness page does.
#'
#' Reference chain, in order: last_checked, then last_changed, then
#' released_at. When all three are empty the answer is "unknown", never
#' "stale". Returns the reference used, its age in hours, the declared window,
#' the window actually applied (the declared one floored at
#' gate_min_stale_window_h) and the bucket.
gate_freshness <- function(last_checked, last_changed, released_at,
                           max_age_h, now_iso,
                           multiplier = gate_stale_multiplier(),
                           min_window_h = gate_min_stale_window_h()) {
  ref <- gate_first_present(list(last_checked, last_changed, released_at))
  age <- gate_age_hours(ref, now_iso)
  mx <- suppressWarnings(as.numeric(max_age_h %||% NA_real_))
  if (length(mx) != 1L || is.na(mx) || mx <= 0) mx <- gate_default_max_age_h()
  # The merge samples once a day at an hour it does not control, so a window
  # finer than that sampling resolution is not something it can assert.
  eff <- max(mx, min_window_h)
  if (is.na(age)) {
    return(list(ref = ref, age = NA_real_, max_age_h = mx,
                effective_max_age_h = eff, status = "unknown"))
  }
  status <- if (age <= eff) "current" else if (age <= multiplier * eff) "slow" else "stale"
  list(ref = ref, age = age, max_age_h = mx, effective_max_age_h = eff,
       status = status)
}

# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

#' Evaluate every source DB the merge expects and decide what happens.
#'
#' @param meta data.frame from the pipeline_metadata table (columns pipeline,
#'   last_checked, last_changed, released_at, expected_max_age_hours), or NULL.
#'   This is the resolution pipeline_metadata.R already performs, manifest
#'   fallbacks included, so a source with no manifest arrives here with a real
#'   timestamp rather than a blank.
#' @param present_dbs basenames of the source DBs actually on disk.
#' @param all_source_dbs every source DB the merge expects, in table order.
#' @param config pipeline_config(); entries without a db_filename (the merger
#'   itself) are excluded, because a self-entry is fresh by definition and
#'   would only pad the pass count.
#' @param now_iso ISO-8601 timestamp for this evaluation.
#' @param row_counts named list or numeric of row counts read from the merged
#'   output, used for the fatal row floors. A table that is not supplied counts
#'   as zero, so an absent table is caught rather than skipped.
#' @param output_bytes size of the merged observatory.db before compression.
#' @param fatal_verdicts the verdicts allowed to refuse the release when they
#'   land on a source in fatal_specs. Staleness is deliberately not one of
#'   them.
#' @param min_stale_window_h floor on the window asserted against, in hours.
#' @param integrity_failed basenames the workflow removed for failing
#'   PRAGMA integrity_check. Without this list a corrupt source and a source
#'   that never published are indistinguishable.
#' @param override TRUE to publish despite a fatal verdict.
#' @return list(rows, fatal_problems, problems, publish_allowed, override_used,
#'   run_failed)
evaluate_freshness_gate <- function(meta,
                                    present_dbs,
                                    all_source_dbs,
                                    config,
                                    now_iso,
                                    fatal_specs = gate_fatal_sources(),
                                    fatal_verdicts = gate_fatal_verdicts(),
                                    row_counts = list(),
                                    output_bytes = NA_real_,
                                    min_output_bytes = gate_min_output_bytes(),
                                    stale_multiplier = gate_stale_multiplier(),
                                    min_stale_window_h = gate_min_stale_window_h(),
                                    integrity_failed = character(),
                                    override = FALSE) {

  meta_for <- function(pipeline) {
    if (is.null(meta) || !is.data.frame(meta) || nrow(meta) == 0) return(NULL)
    if (is.na(pipeline)) return(NULL)
    hit <- which(as.character(meta$pipeline) == pipeline)
    if (length(hit) == 0L) return(NULL)
    meta[hit[1], , drop = FALSE]
  }

  # A table absent from row_counts counts as zero. `[[` on a named numeric
  # vector errors for an unknown name, so never index blind.
  count_of <- function(tbl) {
    if (is.null(row_counts) || !(tbl %in% names(row_counts))) return(0)
    v <- suppressWarnings(as.numeric(row_counts[[tbl]]))
    if (length(v) != 1L || is.na(v)) 0 else v
  }

  col <- function(row, name) {
    if (is.null(row) || !(name %in% names(row))) return(NA_character_)
    v <- row[[name]][1]
    if (is.null(v)) return(NA_character_)
    v
  }

  # db_filename -> config entry. Entries with no db_filename contribute no
  # source DB and are skipped outright.
  cfg_by_db <- list()
  for (cfg in config) {
    fn <- cfg$db_filename %||% ""
    if (!is.character(fn) || length(fn) != 1L || !nzchar(fn)) next
    cfg_by_db[[fn]] <- cfg
  }

  # Cover every expected source DB, then any configured DB the merge does not
  # list. The two *-data-metrics.db sources ride in their sibling's release and
  # have no pipeline_config entry of their own, so they are checked for
  # presence and carry no freshness verdict rather than borrowing one.
  ordered <- unique(c(as.character(all_source_dbs), names(cfg_by_db)))

  rows <- list()
  for (src in ordered) {
    cfg <- cfg_by_db[[src]]
    pipeline <- if (is.null(cfg)) NA_character_ else cfg$name
    max_age <- if (is.null(cfg)) NA_real_ else as.numeric(cfg$max_age_h %||% NA_real_)

    present <- src %in% present_dbs
    corrupt <- src %in% integrity_failed

    m <- meta_for(pipeline)
    if (is.null(cfg)) {
      fresh <- list(ref = NA_character_, age = NA_real_, max_age_h = NA_real_,
                    effective_max_age_h = NA_real_, status = "not tracked")
    } else if (is.null(m)) {
      fresh <- list(ref = NA_character_, age = NA_real_, max_age_h = max_age,
                    effective_max_age_h = NA_real_, status = "unknown")
    } else {
      if ("expected_max_age_hours" %in% names(m)) {
        declared <- suppressWarnings(as.numeric(m$expected_max_age_hours[1]))
        if (length(declared) == 1L && !is.na(declared) && declared > 0) max_age <- declared
      }
      fresh <- gate_freshness(col(m, "last_checked"), col(m, "last_changed"),
                              col(m, "released_at"), max_age, now_iso,
                              multiplier = stale_multiplier,
                              min_window_h = min_stale_window_h)
    }

    spec <- fatal_specs[[src]]
    is_fatal <- !is.null(spec)

    short <- character()
    if (present && is_fatal && length(spec$floors)) {
      for (tbl in names(spec$floors)) {
        floor_n <- as.numeric(spec$floors[[tbl]])
        got <- count_of(tbl)
        if (got < floor_n) {
          short <- c(short, sprintf("%s has %s rows, floor is %s",
                                    tbl, format(got, scientific = FALSE),
                                    format(floor_n, scientific = FALSE)))
        }
      }
    }

    verdict <-
      if (corrupt) "corrupt"
      else if (!present) "missing"
      else if (length(short)) "short rows"
      else if (identical(fresh$status, "stale")) "stale"
      else if (identical(fresh$status, "slow")) "slow"
      else if (identical(fresh$status, "unknown")) "unknown"
      else "ok"

    # "slow" and "unknown" are reported and never fail. Both are the shapes a
    # transient hiccup takes, and this gate prefers a false negative.
    fails <- verdict %in% c("corrupt", "missing", "short rows", "stale")

    rows[[length(rows) + 1L]] <- data.frame(
      source = src,
      pipeline = if (is.na(pipeline)) "-" else pipeline,
      last_checked = fresh$ref %||% NA_character_,
      age_hours = fresh$age,
      max_age_hours = fresh$max_age_h,
      fail_after_hours = stale_multiplier * (fresh$effective_max_age_h %||% NA_real_),
      present = present,
      freshness = fresh$status,
      verdict = verdict,
      fatal = is_fatal,
      fails = fails,
      detail = if (length(short)) paste(short, collapse = "; ") else "",
      stringsAsFactors = FALSE
    )
  }

  rows <- do.call(rbind, rows)
  if (is.null(rows)) {
    rows <- data.frame(source = character(), pipeline = character(),
                       last_checked = character(), age_hours = numeric(),
                       max_age_hours = numeric(), fail_after_hours = numeric(),
                       present = logical(),
                       freshness = character(), verdict = character(),
                       fatal = logical(), fails = logical(),
                       detail = character(), stringsAsFactors = FALSE)
  }

  describe <- function(i) {
    r <- rows[i, ]
    base <- switch(
      r$verdict,
      "corrupt" = sprintf("%s failed PRAGMA integrity_check and was removed before the merge", r$source),
      "missing" = sprintf("%s was not present in sources/ (never downloaded, or its release is gone)", r$source),
      "short rows" = sprintf("%s merged but %s", r$source, r$detail),
      "stale" = sprintf("%s (%s) was last checked %s, %.1f hours ago, past the %.0f hour limit the gate applies (declared window %.0f hours)",
                        r$source, r$pipeline, r$last_checked, r$age_hours,
                        r$fail_after_hours, r$max_age_hours),
      sprintf("%s: %s", r$source, r$verdict))
    base
  }

  failing <- which(rows$fails)
  fatal_problems <- character()
  problems <- character()
  for (i in failing) {
    # Being a fatal source is not enough on its own: only the verdicts that
    # mean the data is actually not there can refuse the release. A stale but
    # present and complete feed.db still ships.
    if (isTRUE(rows$fatal[i]) && rows$verdict[i] %in% fatal_verdicts) {
      fatal_problems <- c(fatal_problems, describe(i))
    } else {
      problems <- c(problems, describe(i))
    }
  }

  size_ok <- TRUE
  if (!is.na(output_bytes) && !is.na(min_output_bytes) && min_output_bytes > 0) {
    if (output_bytes < min_output_bytes) {
      size_ok <- FALSE
      problems <- c(problems, sprintf(
        "observatory.db is %s bytes, below the %s byte floor; a large source probably did not land",
        format(output_bytes, scientific = FALSE),
        format(min_output_bytes, scientific = FALSE)))
    }
  }

  override_used <- isTRUE(override) && length(fatal_problems) > 0
  list(
    rows = rows,
    fatal_problems = fatal_problems,
    problems = problems,
    size_ok = size_ok,
    override_used = override_used,
    publish_allowed = length(fatal_problems) == 0 || isTRUE(override),
    run_failed = length(fatal_problems) > 0 || length(problems) > 0
  )
}

# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

#' Render the verdict table as plain text lines, so a red run explains itself
#' from the log alone without anyone opening a database.
format_gate_table <- function(rows) {
  if (is.null(rows) || nrow(rows) == 0) return("(no sources evaluated)")

  fmt_age <- function(x) ifelse(is.na(x), "-", sprintf("%.1f", x))
  fmt_max <- function(x) ifelse(is.na(x), "-", sprintf("%.0f", x))

  # max_h is the window the pipeline declares, the same number the viewer's
  # freshness page shows. fail_h is the age at which this gate actually fails
  # the source, which is larger whenever the declared window is finer than the
  # merge can resolve.
  cells <- data.frame(
    source = rows$source,
    pipeline = rows$pipeline,
    last_checked = ifelse(is.na(rows$last_checked), "-", rows$last_checked),
    age_h = fmt_age(rows$age_hours),
    max_h = fmt_max(rows$max_age_hours),
    fail_h = fmt_max(rows$fail_after_hours),
    present = ifelse(rows$present, "yes", "NO"),
    verdict = ifelse(rows$fatal, paste0(rows$verdict, " (fatal)"), rows$verdict),
    stringsAsFactors = FALSE
  )
  headers <- c("source", "pipeline", "last_checked", "age_h", "max_h", "fail_h",
               "present", "verdict")
  widths <- vapply(seq_along(headers), function(j) {
    max(nchar(c(headers[j], cells[[j]])))
  }, numeric(1))

  pad <- function(vals, w) formatC(vals, width = -w, flag = " ")
  line <- function(vals) trimws(paste(mapply(pad, vals, widths), collapse = "  "), which = "right")

  out <- c(line(headers),
           line(vapply(widths, function(w) strrep("-", w), character(1))))
  for (i in seq_len(nrow(cells))) {
    out <- c(out, line(as.character(unlist(cells[i, ]))))
  }
  out
}

#' Every problem the gate found, fatal first, as the lines written to the
#' status file the workflow reads. Empty when nothing failed.
gate_status_lines <- function(res) {
  if (!isTRUE(res$run_failed)) return(character())
  c(res$fatal_problems, res$problems)
}

#' Was the escape hatch actually asked for?
#'
#' A scheduled run resolves the workflow input to the empty string, so only the
#' literal word "true" may override, case-insensitively and ignoring
#' surrounding whitespace. Anything else, including "yes", "1" and NA, is not
#' an override.
gate_override_requested <- function(value) {
  if (is.null(value) || length(value) != 1L) return(FALSE)
  if (is.na(value)) return(FALSE)
  identical(tolower(trimws(as.character(value))), "true")
}

#' One-line summary of a gate result.
format_gate_summary <- function(res) {
  n <- nrow(res$rows)
  ok <- sum(!res$rows$fails)
  sprintf("%d of %d sources passed the freshness gate (%d fatal problem%s, %d non-fatal problem%s).",
          ok, n,
          length(res$fatal_problems), if (length(res$fatal_problems) == 1L) "" else "s",
          length(res$problems), if (length(res$problems) == 1L) "" else "s")
}
