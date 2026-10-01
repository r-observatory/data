# The published history pairs: a full history-YYYY-MM-DD.db.zst and a
# merge-only history-merge-YYYY-MM-DD.db.zst, each with its manifest, on the
# fixed prerelease tag `history` of r-observatory/data. Assets are never
# replaced or deleted; a later publish adds a newer dated pair.

HISTORY_REPO <- "r-observatory/data"
HISTORY_RELEASE_TAG <- "history"

history_asset_names <- function(stamp) {
  list(full = list(db = sprintf("history-%s.db.zst", stamp),
                   manifest = sprintf("history-%s-manifest.json", stamp)),
       merge = list(db = sprintf("history-merge-%s.db.zst", stamp),
                    manifest = sprintf("history-merge-%s-manifest.json", stamp)))
}

history_tables <- function(con, schema = "main") {
  DBI::dbGetQuery(con, sprintf(
    "SELECT name FROM %s.sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%%'
      ORDER BY name", schema))$name
}

history_row_counts <- function(con, schema = "main") {
  tables <- history_tables(con, schema)
  counts <- vapply(tables, function(t) {
    as.numeric(DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM %s.%s", schema,
                                            history_quote(t)))$n)
  }, numeric(1))
  as.list(counts)
}

# Structural problems in a history db, empty when there are none.
validate_history_db <- function(con) {
  problems <- character(0)
  ic <- DBI::dbGetQuery(con, "PRAGMA integrity_check")[[1]]
  if (!identical(ic, "ok")) problems <- c(problems, paste("integrity_check:", paste(ic, collapse = "; ")))
  episode <- grep("_history$", history_tables(con), value = TRUE)
  for (t in episode) {
    info <- DBI::dbGetQuery(con, sprintf("PRAGMA table_info(%s)", history_quote(t)))
    key <- info$name[info$pk > 0][order(info$pk[info$pk > 0])]
    key <- setdiff(key, "episode_seq")
    q <- history_quote(t)
    on <- paste(sprintf("b.%1$s = a.%1$s", history_quote(key)), collapse = " AND ")
    k <- paste(history_quote(key), collapse = ", ")
    checks <- c(
      dates = sprintf("SELECT COUNT(*) AS n FROM %s WHERE last_seen < first_seen OR ended_on <= last_seen", q),
      open = sprintf("SELECT COUNT(*) AS n FROM (SELECT %s FROM %s WHERE ended_on IS NULL GROUP BY %s HAVING COUNT(*) > 1)", k, q, k),
      gaps = sprintf("SELECT COUNT(*) AS n FROM (SELECT %s FROM %s GROUP BY %s HAVING MAX(episode_seq) <> COUNT(*) OR MIN(episode_seq) <> 1)", k, q, k),
      order = sprintf("SELECT COUNT(*) AS n FROM %1$s AS a JOIN %1$s AS b ON %2$s AND b.episode_seq = a.episode_seq + 1 WHERE a.ended_on IS NULL OR b.first_seen < a.ended_on", q, on))
    for (name in names(checks)) {
      n <- DBI::dbGetQuery(con, checks[[name]])$n
      if (n > 0L) problems <- c(problems, sprintf("%s: %d rows fail the %s check", t, n, name))
    }
  }
  bad <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM history_snapshots
      WHERE outcome = 'processed' AND (asset IS NULL OR sha256 IS NULL)")$n
  if (bad > 0L) problems <- c(problems, sprintf("history_snapshots: %d processed rows without asset or sha256", bad))
  problems
}

# The merge-only db: exactly HISTORY_MERGE_TABLES with their indexes.
write_merge_db <- function(history_path, merge_path) {
  unlink(merge_path)
  out <- DBI::dbConnect(RSQLite::SQLite(), merge_path)
  on.exit(DBI::dbDisconnect(out), add = TRUE)
  DBI::dbExecute(out, "ATTACH DATABASE ? AS src", params = list(history_path))
  for (t in HISTORY_MERGE_TABLES) {
    sql <- DBI::dbGetQuery(out, "SELECT sql FROM src.sqlite_master WHERE type = 'table' AND name = ?",
                           params = list(t))$sql
    if (length(sql) != 1L) stop("history.db has no table ", t, call. = FALSE)
    DBI::dbExecute(out, sql)
    DBI::dbExecute(out, sprintf("INSERT INTO main.%1$s SELECT * FROM src.%1$s", history_quote(t)))
    idx <- DBI::dbGetQuery(out, "SELECT sql FROM src.sqlite_master
                                  WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL",
                           params = list(t))$sql
    for (s in idx) DBI::dbExecute(out, s)
  }
  DBI::dbExecute(out, "DETACH DATABASE src")
  DBI::dbExecute(out, "VACUUM")
  invisible(merge_path)
}

history_family_summary <- function(con) {
  fams <- DBI::dbGetQuery(con, "SELECT DISTINCT family FROM history_snapshots ORDER BY family")$family
  out <- list()
  for (f in fams) {
    ends <- DBI::dbGetQuery(con,
      "SELECT tag FROM history_snapshots WHERE family = ? AND outcome = 'processed'
        ORDER BY snapshot_on, tag", params = list(f))$tag
    n <- DBI::dbGetQuery(con,
      "SELECT outcome, COUNT(*) AS n FROM history_snapshots WHERE family = ? GROUP BY outcome",
      params = list(f))
    count <- function(o) if (o %in% n$outcome) n$n[n$outcome == o] else 0L
    out[[f]] <- list(first_tag = if (length(ends)) ends[1] else NA_character_,
                     last_tag = if (length(ends)) ends[length(ends)] else NA_character_,
                     processed = count("processed"), superseded = count("superseded"),
                     skipped = count("skipped"), failed = count("failed"))
  }
  out
}

zstd_compress <- function(src, dest) {
  unlink(dest)
  status <- system2("zstd", c("-q", "-19", "-T0", shQuote(src), "-o", shQuote(dest)))
  if (!identical(status, 0L)) stop("zstd could not compress ", src, call. = FALSE)
  dest
}

write_history_manifest <- function(path, kind, db, zst, con, now) {
  handover <- DBI::dbGetQuery(con,
    "SELECT value FROM history_settings WHERE key = 'flavor_handover_tag'")$value
  counted <- DBI::dbConnect(RSQLite::SQLite(), db, flags = RSQLite::SQLITE_RO)
  on.exit(DBI::dbDisconnect(counted), add = TRUE)
  # db_filename is what merge.yml's expand_dated selects the manifest by.
  man <- list(kind = kind, generated_at = now, db_filename = basename(db),
              db_bytes = file.size(db), db_sha256 = history_file_sha256(db),
              asset_filename = basename(zst), asset_bytes = file.size(zst),
              asset_sha256 = history_file_sha256(zst),
              tables = history_row_counts(counted),
              families = history_family_summary(con),
              flavor_handover_tag = if (length(handover)) handover else NA_character_)
  jsonlite::write_json(man, path, auto_unbox = TRUE, pretty = TRUE, digits = NA, na = "null")
  invisible(man)
}

# Writes both pairs into `out_dir`, keeping each .db beside its .zst for the
# checks and for the next publish's comparison. Returns the four asset paths.
build_history_assets <- function(history_path, out_dir, stamp, now) {
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  con <- DBI::dbConnect(RSQLite::SQLite(), history_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  problems <- validate_history_db(con)
  if (length(problems)) stop(paste(c("history.db fails its checks:", problems), collapse = "\n"),
                             call. = FALSE)
  names <- history_asset_names(stamp)
  full_db <- file.path(out_dir, sprintf("history-%s.db", stamp))
  merge_db <- file.path(out_dir, sprintf("history-merge-%s.db", stamp))
  unlink(full_db)
  DBI::dbExecute(con, "VACUUM INTO ?", params = list(full_db))
  write_merge_db(full_db, merge_db)
  for (kind in c("full", "merge")) {
    db <- if (kind == "full") full_db else merge_db
    zst <- zstd_compress(db, file.path(out_dir, names[[kind]]$db))
    write_history_manifest(file.path(out_dir, names[[kind]]$manifest), kind, db, zst, con, now)
  }
  file.path(out_dir, unlist(names, use.names = FALSE))
}

# Problems with the built pairs, empty when both expand to what their
# manifests say and hold the tables they should.
validate_history_assets <- function(out_dir, stamp) {
  problems <- character(0)
  names <- history_asset_names(stamp)
  for (kind in c("full", "merge")) {
    mpath <- file.path(out_dir, names[[kind]]$manifest)
    zpath <- file.path(out_dir, names[[kind]]$db)
    if (!file.exists(mpath) || !file.exists(zpath)) {
      problems <- c(problems, sprintf("%s: missing %s or %s", kind, basename(mpath), basename(zpath)))
      next
    }
    man <- jsonlite::read_json(mpath, simplifyVector = FALSE)
    if (!identical(man$asset_filename, basename(zpath))) problems <- c(problems, sprintf("%s: manifest names %s", kind, man$asset_filename))
    if (!identical(man$db_filename, sub("\\.zst$", "", basename(zpath)))) {
      problems <- c(problems, sprintf("%s: manifest describes %s", kind, man$db_filename))
    }
    if (file.size(zpath) != man$asset_bytes) problems <- c(problems, sprintf("%s: asset size differs from its manifest", kind))
    if (history_file_sha256(zpath) != man$asset_sha256) problems <- c(problems, sprintf("%s: asset sha256 differs from its manifest", kind))
    tmp <- tempfile(sprintf("history-%s-", kind), fileext = ".db")
    status <- system2("zstd", c("-dq", "-f", shQuote(zpath), "-o", shQuote(tmp)),
                      stdout = FALSE, stderr = FALSE)
    if (!identical(status, 0L)) {
      problems <- c(problems, sprintf("%s: asset does not decompress", kind))
      next
    }
    if (file.size(tmp) != man$db_bytes) problems <- c(problems, sprintf("%s: expanded size differs from its manifest", kind))
    if (history_file_sha256(tmp) != man$db_sha256) problems <- c(problems, sprintf("%s: expanded sha256 differs from its manifest", kind))
    con <- DBI::dbConnect(RSQLite::SQLite(), tmp, flags = RSQLite::SQLITE_RO)
    tables <- history_tables(con)
    counts <- history_row_counts(con)
    listed <- vapply(man$tables, as.numeric, numeric(1))
    if (!setequal(names(counts), names(listed)) ||
        any(unlist(counts)[names(listed)] != listed)) {
      problems <- c(problems, sprintf("%s: table rows differ from its manifest", kind))
    }
    if (kind == "merge" && !setequal(tables, HISTORY_MERGE_TABLES)) {
      problems <- c(problems, sprintf("merge: holds %s", paste(sort(tables), collapse = ", ")))
    }
    if (kind == "full") {
      missing <- setdiff(c(HISTORY_MERGE_TABLES, "history_series_observations",
                           "cran_check_flavors", "cran_check_flavor_status_history"), tables)
      if (length(missing)) problems <- c(problems, sprintf("full: lacks %s", paste(missing, collapse = ", ")))
      problems <- c(problems, validate_history_db(con))
    }
    DBI::dbDisconnect(con)
    unlink(tmp)
  }
  problems
}

# "present" or "absent" by the HTTP status of the tag lookup; anything else
# stops, so an unreadable release is never taken for a missing one.
history_release_status <- function(io, repo = HISTORY_REPO, tag = HISTORY_RELEASE_TAG) {
  status <- io$release_http_status(repo, tag)
  if (identical(status, 200L)) return("present")
  if (identical(status, 404L)) return("absent")
  stop(sprintf("could not tell whether %s@%s exists (HTTP %s)", repo, tag, status), call. = FALSE)
}

# The newest dated pair of a kind with both files uploaded, or NULL.
newest_history_pair <- function(assets, kind = "full") {
  prefix <- if (kind == "full") "history-" else "history-merge-"
  ready <- assets$name[assets$state == "uploaded"]
  pat <- sprintf("^%s([0-9]{4}-[0-9]{2}-[0-9]{2})\\.db\\.zst$", prefix)
  stamps <- sub(pat, "\\1", grep(pat, ready, value = TRUE))
  stamps <- stamps[sprintf("%s%s-manifest.json", prefix, stamps) %in% ready]
  if (length(stamps) == 0L) return(NULL)
  s <- max(stamps)
  list(kind = kind, stamp = s, db = sprintf("%s%s.db.zst", prefix, s),
       manifest = sprintf("%s%s-manifest.json", prefix, s))
}

# Downloads a pair, checks it against its manifest and expands it to `dest`.
# Any failure stops: an existing asset that cannot be read is never a fresh start.
fetch_history_pair <- function(io, pair, dest) {
  dir <- tempfile("history-pair-")
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  fail <- function(why) stop(sprintf("%s@%s holds %s but it could not be read (%s); not starting fresh",
                                     HISTORY_REPO, HISTORY_RELEASE_TAG, pair$db, why), call. = FALSE)
  for (f in c(pair$manifest, pair$db)) {
    if (!isTRUE(io$download(HISTORY_REPO, HISTORY_RELEASE_TAG, f, dir)) ||
        !file.exists(file.path(dir, f))) fail(paste("download of", f))
  }
  man <- jsonlite::read_json(file.path(dir, pair$manifest), simplifyVector = FALSE)
  zst <- file.path(dir, pair$db)
  if (!identical(man$asset_filename, pair$db)) fail("its manifest names another asset")
  if (file.size(zst) != man$asset_bytes || history_file_sha256(zst) != man$asset_sha256) {
    fail("the asset differs from its manifest")
  }
  if (!isTRUE(io$unzstd(zst, dest))) fail("zstd")
  if (file.size(dest) != man$db_bytes || history_file_sha256(dest) != man$db_sha256) {
    unlink(dest)
    fail("the expanded db differs from its manifest")
  }
  invisible(dest)
}

# Starts a run from the newest published full pair when there is no local
# history.db; a first run with no release starts fresh.
restore_prior_history <- function(io, path) {
  if (history_release_status(io) == "absent") {
    message("no history release yet; starting fresh")
    return(invisible(FALSE))
  }
  pair <- newest_history_pair(io$asset_info(HISTORY_REPO, HISTORY_RELEASE_TAG), "full")
  if (is.null(pair)) {
    stop(sprintf("%s@%s exists but holds no full pair; not starting fresh",
                 HISTORY_REPO, HISTORY_RELEASE_TAG), call. = FALSE)
  }
  fetch_history_pair(io, pair, path)
  message("continuing from ", pair$db)
  invisible(TRUE)
}

# Reasons the new history.db must not replace the prior one as newest.
history_regressions <- function(prior_path, new_path) {
  con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "ATTACH DATABASE ? AS prior", params = list(prior_path))
  DBI::dbExecute(con, "ATTACH DATABASE ? AS new", params = list(new_path))
  problems <- character(0)
  prior <- history_row_counts(con, "prior")
  new <- history_row_counts(con, "new")
  for (t in names(prior)) {
    if (is.null(new[[t]])) problems <- c(problems, sprintf("%s is gone", t))
    else if (new[[t]] < prior[[t]]) {
      problems <- c(problems, sprintf("%s fell from %.0f to %.0f rows", t, prior[[t]], new[[t]]))
    }
  }
  if ("history_snapshots" %in% names(prior) && "history_snapshots" %in% names(new)) {
    lost <- DBI::dbGetQuery(con,
      "SELECT COUNT(*) AS n FROM prior.history_snapshots AS p
        WHERE NOT EXISTS (SELECT 1 FROM new.history_snapshots AS n
                           WHERE n.family = p.family AND n.tag = p.tag AND n.outcome = p.outcome)")$n
    if (lost > 0L) problems <- c(problems, sprintf("%d recorded tags are missing or changed", lost))
  }
  problems
}
