# Per-flavor CRAN check status as episodes. This file is kept byte for byte the
# same in r-observatory/data (scripts/history/) and r-observatory/cran-metadata
# (scripts/); each repository pins its sha256 in a test.
#
# Status and flags are the compared value. The checked version is an attribute:
# it moves with the episode and never opens a new one. A change lies in
# (last_seen, ended_on].

CHECK_FLAVOR_MIN_FRACTION <- 0.5

check_flavor_ddl <- c(
  "CREATE TABLE IF NOT EXISTS cran_check_flavors (
     flavor_id INTEGER PRIMARY KEY, flavor TEXT NOT NULL UNIQUE)",
  "CREATE TABLE IF NOT EXISTS cran_check_flavor_status_history (
     package TEXT NOT NULL, flavor_id INTEGER NOT NULL, episode_seq INTEGER NOT NULL,
     status TEXT NOT NULL, flags TEXT, first_version TEXT, last_version TEXT,
     first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, ended_on TEXT,
     PRIMARY KEY (package, flavor_id, episode_seq),
     CHECK (last_seen >= first_seen),
     CHECK (ended_on IS NULL OR ended_on > last_seen)) WITHOUT ROWID",
  "CREATE UNIQUE INDEX IF NOT EXISTS ux_cran_check_flavor_status_open
     ON cran_check_flavor_status_history (package, flavor_id) WHERE ended_on IS NULL")

ensure_check_flavor_tables <- function(con) {
  for (sql in check_flavor_ddl) DBI::dbExecute(con, sql)
  invisible(NULL)
}

# CRAN_check_results() or the stored cran_check_results table, as lower-case
# columns (T_install becomes tinstall), empty text as NA, rows without package,
# flavor or status dropped, one row per (package, flavor), in byte order.
normalize_check_results <- function(results) {
  df <- as.data.frame(results, stringsAsFactors = FALSE)
  names(df) <- sub("^t_", "t", tolower(names(df)))
  keep <- intersect(c("package", "flavor", "status", "version", "flags",
                      "tinstall", "tcheck", "ttotal"), names(df))
  df <- df[keep]
  for (col in intersect(c("package", "flavor", "status", "version", "flags"), keep)) {
    x <- as.character(df[[col]])
    x[!is.na(x) & !nzchar(trimws(x))] <- NA_character_
    df[[col]] <- x
  }
  for (col in intersect(c("tinstall", "tcheck", "ttotal"), keep)) {
    df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
  }
  df <- df[!is.na(df$package) & !is.na(df$flavor) & !is.na(df$status), , drop = FALSE]
  df <- df[do.call(order, c(unname(as.list(df)), method = "radix")), , drop = FALSE]
  df <- df[!duplicated(df[c("package", "flavor")]), , drop = FALSE]
  rownames(df) <- NULL
  df
}

# md5 of the normalized table, columns in byte order. Equal fingerprints mean
# the same table, which after a failed fetch is yesterday's copy.
check_results_fingerprint <- function(results) {
  cols <- sort(names(results), method = "radix")
  text <- lapply(results[cols], function(x) {
    x <- as.character(x)
    x[is.na(x)] <- "\\N"
    x
  })
  lines <- c(paste(cols, collapse = "\t"), do.call(paste, c(unname(text), sep = "\t")))
  tmp <- tempfile("check-results-")
  on.exit(unlink(tmp), add = TRUE)
  writeLines(enc2utf8(lines), tmp, useBytes = TRUE)
  unname(tools::md5sum(tmp))
}

# "stale" for a failed fetch or the table last applied, "unhealthy" for an empty
# table or one under half the rows last applied, else "applied".
# `prior` is list(rows, fingerprint) of the last applied table, or NULL.
check_flavor_guard <- function(n_rows, fingerprint, prior = NULL, fetched = TRUE) {
  if (!isTRUE(fetched)) return("stale")
  prior_fp <- prior$fingerprint
  if (length(prior_fp) == 1L && !is.na(prior_fp) && identical(fingerprint, prior_fp)) {
    return("stale")
  }
  if (n_rows == 0L) return("unhealthy")
  prior_rows <- prior$rows
  if (length(prior_rows) == 1L && !is.na(prior_rows) &&
      n_rows < CHECK_FLAVOR_MIN_FRACTION * prior_rows) {
    return("unhealthy")
  }
  "applied"
}

# Gives each new flavor the next id, new names in byte order. Returns every
# flavor with its id.
upsert_check_flavors <- function(con, flavors) {
  ensure_check_flavor_tables(con)
  known <- DBI::dbGetQuery(con, "SELECT flavor_id, flavor FROM cran_check_flavors")
  new <- sort(setdiff(unique(flavors[!is.na(flavors)]), known$flavor), method = "radix")
  if (length(new) > 0L) {
    start <- if (nrow(known) > 0L) max(known$flavor_id) else 0L
    DBI::dbExecute(con, "INSERT INTO cran_check_flavors (flavor_id, flavor) VALUES (?, ?)",
                   params = list(start + seq_along(new), new))
  }
  DBI::dbGetQuery(con, "SELECT flavor_id, flavor FROM cran_check_flavors ORDER BY flavor_id")
}

# Loads normalized results into temp.check_flavor_snap keyed by flavor id.
load_check_flavor_snap <- function(con, results) {
  flavors <- upsert_check_flavors(con, results$flavor)
  n <- nrow(results)
  col_or_na <- function(col) if (col %in% names(results)) results[[col]] else rep(NA_character_, n)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS temp.check_flavor_snap")
  DBI::dbExecute(con, "CREATE TEMP TABLE check_flavor_snap (
    package TEXT NOT NULL, flavor_id INTEGER NOT NULL, status TEXT NOT NULL,
    flags TEXT, version TEXT, PRIMARY KEY (package, flavor_id))")
  if (n > 0L) {
    DBI::dbExecute(con, "INSERT INTO temp.check_flavor_snap VALUES (?, ?, ?, ?, ?)",
                   params = list(results$package,
                                 flavors$flavor_id[match(results$flavor, flavors$flavor)],
                                 results$status, col_or_na("flags"), col_or_na("version")))
  }
  invisible(n)
}

# Folds one applied snapshot. `fill_flags` is TRUE on the first snapshot that
# carries flags: they are written into the open episodes before comparing, so
# their arrival is not a change. Returns the counts of each kind of row write
# and `mismatches`, the rows on either side that still disagree afterwards.
fold_check_flavor_status <- function(con, results, snapshot_on, fill_flags = FALSE) {
  ensure_check_flavor_tables(con)
  load_check_flavor_snap(con, results)
  has_flags <- "flags" %in% names(results)
  has_version <- "version" %in% names(results)
  on <- "s.package = h.package AND s.flavor_id = h.flavor_id"
  filled <- 0L
  if (isTRUE(fill_flags) && has_flags) {
    filled <- DBI::dbExecute(con, paste(
      "UPDATE cran_check_flavor_status_history AS h SET flags = s.flags",
      "FROM temp.check_flavor_snap AS s WHERE h.ended_on IS NULL AND", on))
  }
  same_flags <- if (has_flags) " AND s.flags IS h.flags" else ""
  set_version <- if (has_version) ", last_version = s.version" else ""
  extended <- DBI::dbExecute(con, paste0(
    "UPDATE cran_check_flavor_status_history AS h SET last_seen = ?", set_version,
    " FROM temp.check_flavor_snap AS s WHERE h.ended_on IS NULL AND ", on,
    " AND s.status = h.status", same_flags), params = list(snapshot_on))
  closed <- DBI::dbExecute(con,
    "UPDATE cran_check_flavor_status_history SET ended_on = ?
      WHERE ended_on IS NULL AND last_seen < ?", params = list(snapshot_on, snapshot_on))
  opened <- DBI::dbExecute(con,
    "INSERT INTO cran_check_flavor_status_history
       (package, flavor_id, episode_seq, status, flags, first_version, last_version,
        first_seen, last_seen)
     SELECT s.package, s.flavor_id,
            COALESCE((SELECT MAX(h.episode_seq) FROM cran_check_flavor_status_history AS h
                       WHERE h.package = s.package AND h.flavor_id = s.flavor_id), 0) + 1,
            s.status, s.flags, s.version, s.version, ?, ?
       FROM temp.check_flavor_snap AS s
      WHERE NOT EXISTS (SELECT 1 FROM cran_check_flavor_status_history AS h
                         WHERE h.package = s.package AND h.flavor_id = s.flavor_id
                           AND h.ended_on IS NULL)", params = list(snapshot_on, snapshot_on))
  same <- paste0(on, " AND s.status = h.status", same_flags)
  mismatches <- DBI::dbGetQuery(con, paste0(
    "SELECT (SELECT COUNT(*) FROM temp.check_flavor_snap AS s WHERE NOT EXISTS (",
    "SELECT 1 FROM cran_check_flavor_status_history AS h WHERE h.ended_on IS NULL AND ", same,
    ")) + (SELECT COUNT(*) FROM cran_check_flavor_status_history AS h WHERE h.ended_on IS NULL",
    " AND NOT EXISTS (SELECT 1 FROM temp.check_flavor_snap AS s WHERE ", same, ")) AS n"))$n
  DBI::dbExecute(con, "DROP TABLE IF EXISTS temp.check_flavor_snap")
  list(filled = filled, extended = extended, closed = closed, opened = opened,
       mismatches = as.integer(mismatches))
}

# Rows on either side with no equal partner, comparing (package, flavor,
# status, flags) between the open episodes and a results table. 0 means the
# open episodes are exactly that table, as a producer seeding from the history
# asset must check.
check_flavor_open_mismatches <- function(con, results) {
  results <- normalize_check_results(results)
  if (!"flags" %in% names(results)) results$flags <- NA_character_
  known <- DBI::dbGetQuery(con, "SELECT flavor_id, flavor FROM cran_check_flavors")
  ids <- known$flavor_id[match(results$flavor, known$flavor)]
  unknown <- sum(is.na(ids))
  DBI::dbExecute(con, "DROP TABLE IF EXISTS temp.check_flavor_cmp")
  DBI::dbExecute(con, "CREATE TEMP TABLE check_flavor_cmp (
    package TEXT NOT NULL, flavor_id INTEGER NOT NULL, status TEXT NOT NULL, flags TEXT,
    PRIMARY KEY (package, flavor_id))")
  ok <- !is.na(ids)
  if (any(ok)) {
    DBI::dbExecute(con, "INSERT INTO temp.check_flavor_cmp VALUES (?, ?, ?, ?)",
                   params = list(results$package[ok], ids[ok], results$status[ok],
                                 results$flags[ok]))
  }
  missing <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM temp.check_flavor_cmp AS s
      WHERE NOT EXISTS (SELECT 1 FROM cran_check_flavor_status_history AS h
                         WHERE h.ended_on IS NULL AND h.package = s.package
                           AND h.flavor_id = s.flavor_id AND h.status = s.status
                           AND h.flags IS s.flags)")$n
  extra <- DBI::dbGetQuery(con,
    "SELECT COUNT(*) AS n FROM cran_check_flavor_status_history AS h
      WHERE h.ended_on IS NULL AND NOT EXISTS (
        SELECT 1 FROM temp.check_flavor_cmp AS s
         WHERE h.package = s.package AND h.flavor_id = s.flavor_id
           AND h.status = s.status AND h.flags IS s.flags)")$n
  DBI::dbExecute(con, "DROP TABLE IF EXISTS temp.check_flavor_cmp")
  as.integer(unknown + missing + extra)
}

# One snapshot through the guard and, when applied, the fold. `prior` is
# list(rows, fingerprint) of the last applied table (NULL before the first);
# the returned `prior` is what the next snapshot compares against.
check_flavor_step <- function(con, results, snapshot_on, prior = NULL,
                              fetched = TRUE, fill_flags = FALSE) {
  if (!isTRUE(fetched) || is.null(results)) {
    return(list(outcome = "stale", rows = NA_integer_, fingerprint = NA_character_,
                counts = NULL, prior = prior))
  }
  norm <- normalize_check_results(results)
  fp <- check_results_fingerprint(norm)
  outcome <- check_flavor_guard(nrow(norm), fp, prior)
  counts <- NULL
  if (identical(outcome, "applied")) {
    counts <- fold_check_flavor_status(con, norm, snapshot_on, fill_flags)
    prior <- list(rows = nrow(norm), fingerprint = fp)
  }
  list(outcome = outcome, rows = nrow(norm), fingerprint = fp, counts = counts, prior = prior)
}
