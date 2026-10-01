# What each history series reads from a dated release and how it is keyed.
# A snapshot database is attached as `snap`; `s` is its source table.

# The tables the merge-only asset carries, which the merger may allowlist.
# The flavor status tables stay out: cran-metadata writes them itself.
HISTORY_MERGE_TABLES <- c(
  "cran_check_issue_history", "cran_check_deadline_history", "pipeline_metadata_history",
  "conda_forge_summary_history", "bioconda_summary_history",
  "bioc_downloads_summary_history", "history_snapshots")

history_families <- function() {
  list(
    # Every run makes a new tag, so today's release is final once published.
    "cran-metadata" = list(
      name = "cran-metadata", repo = "r-observatory/cran-metadata",
      assets = c("metadata.db.zst", "metadata.db"),
      tag_pattern = "^v([0-9]{4})([0-9]{2})([0-9]{2})-[0-9]{6}$", from = NA_character_,
      replaces_same_day = FALSE),
    # A later merge the same day deletes and re-creates the day's tag.
    "data" = list(
      name = "data", repo = "r-observatory/data",
      assets = c("observatory.db.zst", "observatory.db"),
      tag_pattern = "^v([0-9]{4})-([0-9]{2})-([0-9]{2})$", from = "2026-06-28",
      replaces_same_day = TRUE))
}

conda_values <- c("origin", "identity_state", "total_30d", "total_90d", "total_365d",
                  "rank_30d", "rank_90d", "rank_365d")

history_series <- function() {
  list(
    list(name = "check_flavor_status", family = "cran-metadata", kind = "check_flavor",
         source = "cran_check_results", table = "cran_check_flavor_status_history"),
    list(name = "check_timing", family = "cran-metadata", kind = "episodes",
         source = "cran_check_results", table = "cran_check_timing_history",
         key = c(package = "s.package", flavor_id = "f.flavor_id"),
         key_types = c("TEXT", "INTEGER"), values = c("tinstall", "tcheck", "ttotal"),
         from = paste("snap.cran_check_results AS s JOIN main.cran_check_flavors AS f",
                      "ON f.flavor = s.flavor"),
         guard = "check_results"),
    list(name = "check_issue", family = "cran-metadata", kind = "episodes",
         source = "cran_check_issues", table = "cran_check_issue_history",
         key = c(package = "s.package", kind = "s.kind", href = "s.href"),
         key_types = c("TEXT", "TEXT", "TEXT"), values = "version"),
    list(name = "check_deadline", family = "cran-metadata", kind = "episodes",
         source = "cran_check_deadlines", table = "cran_check_deadline_history",
         key = c(package = "s.package", deadline_seq = "s.episode_seq"),
         key_types = c("TEXT", "INTEGER"), values = "deadline"),
    list(name = "pipeline_metadata", family = "data", kind = "episodes",
         source = "pipeline_metadata", table = "pipeline_metadata_history",
         key = c(pipeline = "s.pipeline"), key_types = "TEXT",
         values = function(cols) setdiff(cols$name, c("pipeline", "fetched_at", "last_checked"))),
    list(name = "autoobs_summary", family = "data", kind = "episodes",
         source = "autoobs_downloads_summary", table = "autoobs_summary_history",
         key = c(package = "s.package"), key_types = "TEXT",
         values = c("origin", "identity_state", "total_1d", "total_7d", "total_30d", "cnt_total"),
         as_of = "MAX(last_snapshot)"),
    list(name = "conda_forge_summary", family = "data", kind = "episodes",
         source = "conda_forge_downloads_summary", table = "conda_forge_summary_history",
         key = c(package = "s.package"), key_types = "TEXT", values = conda_values),
    list(name = "bioconda_summary", family = "data", kind = "episodes",
         source = "bioconda_downloads_summary", table = "bioconda_summary_history",
         key = c(package = "s.package"), key_types = "TEXT", values = conda_values),
    list(name = "bioc_downloads_summary", family = "data", kind = "episodes",
         source = "bioc_downloads_summary", table = "bioc_downloads_summary_history",
         key = c(package = "s.package", category = "s.category"), key_types = c("TEXT", "TEXT"),
         values = c("download_score", "total_last_month", "total_12mo", "rank_score",
                    "rank_downloads_12mo")),
    # updated_at is the run time, so it would open an episode every day.
    list(name = "bioc_packages", family = "data", kind = "episodes",
         source = "bioc_packages", table = "bioc_packages_history",
         key = c(name = "s.name"), key_types = "TEXT",
         values = function(cols) setdiff(cols$name, c("name", "updated_at"))),
    # Two author rows can be identical, so a package's rows are one value.
    list(name = "bioc_authors", family = "data", kind = "serialized",
         source = "bioc_authors", table = "bioc_authors_history",
         key = c(package = "s.package"), key_types = "TEXT", value = "authors",
         serialize = function(cols) sort(setdiff(cols$name, "package"), method = "radix")),
    list(name = "vcs_repo_attr", family = "data", kind = "episodes",
         source = "vcs_signals_summary", table = "vcs_repo_attr_history",
         key = c(package = "s.package", origin = "s.origin"), key_types = c("TEXT", "TEXT"),
         values = c("license", "topics", "is_archived")),
    list(name = "vcs_dev_tooling", family = "data", kind = "episodes",
         source = "vcs_dev_tooling", table = "vcs_dev_tooling_history",
         key = c(repo_id = "s.repo_id"), key_types = "TEXT",
         values = function(cols) {
           cols$name[grepl("^(has|ci)_", cols$name) & toupper(cols$type) == "INTEGER"]
         }),
    list(name = "coverage_summary", family = "data", kind = "episodes",
         source = "coverage_summary", table = "coverage_summary_history",
         key = c(package = "s.package", version = "s.version"), key_types = c("TEXT", "TEXT"),
         values = function(cols) setdiff(cols$name, c("package", "version"))))
}

# Tag to the UTC date it stands for, NA when the tag is not one of the family's.
tag_snapshot_on <- function(family, tags) {
  out <- rep(NA_character_, length(tags))
  hit <- grepl(family$tag_pattern, tags)
  out[hit] <- sub(family$tag_pattern, "\\1-\\2-\\3", tags[hit])
  out
}

# json_object() over the named columns of `alias`, keys in the given order.
json_object_sql <- function(cols, alias = "s") {
  paste0("json_object(", paste(sprintf("'%s', %s.%s", cols, alias, history_quote(cols)),
                               collapse = ", "), ")")
}

# A package's rows as one JSON array, rows in byte order of their JSON text.
serialized_select <- function(source, key, value, cols) {
  sprintf(paste("SELECT r.%1$s AS %1$s, json_group_array(json(r.obj) ORDER BY r.obj) AS %2$s",
                "FROM (SELECT s.%1$s, %3$s AS obj FROM snap.%4$s AS s) AS r GROUP BY r.%1$s"),
          history_quote(key), history_quote(value), json_object_sql(cols), history_quote(source))
}

# How to read a series from the attached snapshot: NULL when its source table
# or a key column is missing (the series is absent), else the SELECT, key and
# value columns with their types, and the source columns it used.
series_read_plan <- function(con, series) {
  if (!history_table_exists(con, series$source, "snap")) return(NULL)
  cols <- history_table_columns(con, series$source, "snap")
  key_src <- sub("^s\\.", "", series$key[grepl("^s\\.", series$key)])
  if (!all(key_src %in% cols$name)) return(NULL)
  key <- names(series$key)
  if (identical(series$kind, "serialized")) {
    used <- series$serialize(cols)
    return(list(key = key, key_types = series$key_types, values = series$value,
                value_types = "TEXT", columns = used,
                select = serialized_select(series$source, key, series$value, used)))
  }
  values <- if (is.function(series$values)) series$values(cols) else
    intersect(series$values, cols$name)
  types <- cols$type[match(values, cols$name)]
  from <- series$from %||% sprintf("snap.%s AS s", history_quote(series$source))
  exprs <- c(sprintf("%s AS %s", series$key, history_quote(key)),
             sprintf("s.%s", history_quote(values)))
  list(key = key, key_types = series$key_types, values = values, value_types = types,
       columns = values,
       select = sprintf("SELECT %s FROM %s", paste(exprs, collapse = ", "), from))
}

# The last applied observation of a series: list(rows, fingerprint, columns).
last_applied <- function(con, family, series) {
  r <- DBI::dbGetQuery(con,
    "SELECT o.rows_kept, o.fingerprint, o.columns
       FROM history_series_observations AS o
       JOIN history_snapshots AS s ON s.family = o.family AND s.tag = o.tag
      WHERE o.family = ? AND o.series = ? AND o.outcome = 'applied'
      ORDER BY s.snapshot_on DESC LIMIT 1", params = list(family, series))
  if (nrow(r) == 0L) return(NULL)
  list(rows = r$rows_kept, fingerprint = r$fingerprint,
       columns = if (is.na(r$columns)) character(0) else strsplit(r$columns, ",", fixed = TRUE)[[1]])
}

# Rewrites the stored value of open episodes whose rows, projected onto
# `common` columns, equal today's projection: a column the source gained or
# lost is not a change to a serialized value.
refill_serialized <- function(con, table, key, value, common) {
  proj <- sprintf(
    "(SELECT json_group_array(json(p.obj) ORDER BY p.obj) FROM (SELECT json_object(%s) AS obj FROM json_each(h.%s) AS e) AS p)",
    paste(sprintf("'%1$s', json_extract(e.value, '$.%1$s')", common), collapse = ", "),
    history_quote(value))
  on <- paste(sprintf("s.%1$s = h.%1$s", history_quote(key)), collapse = " AND ")
  DBI::dbExecute(con, sprintf(
    "UPDATE %1$s AS h SET %2$s = s.%2$s FROM temp.history_snap AS s
      WHERE h.ended_on IS NULL AND %3$s AND h.%2$s IS NOT s.%2$s AND %4$s IS s.%5$s",
    history_quote(table), history_quote(value), on, proj,
    history_quote(paste0(value, "_common"))))
}

# One episodes or serialized series against the attached snapshot.
# `forced` is a guard outcome decided elsewhere (the check results guard).
apply_episode_series <- function(con, series, snapshot_on, prior, forced = NULL) {
  plan <- series_read_plan(con, series)
  if (is.null(plan)) return(list(outcome = "absent"))
  compared <- plan$values
  loaded <- plan$values
  types <- plan$value_types
  common <- NULL
  if (identical(series$kind, "serialized") && !is.null(prior) &&
      !identical(prior$columns, plan$columns) && history_table_exists(con, series$table) &&
      length(intersect(prior$columns, plan$columns)) > 0L) {
    common <- intersect(prior$columns, plan$columns)
    alt <- serialized_select(series$source, plan$key, paste0(series$value, "_common"), common)
    plan$select <- sprintf(
      "SELECT a.*, b.%2$s FROM (%1$s) AS a LEFT JOIN (%3$s) AS b ON b.%4$s = a.%4$s",
      plan$select, history_quote(paste0(series$value, "_common")), alt,
      history_quote(plan$key))
    loaded <- c(loaded, paste0(series$value, "_common"))
    types <- c(types, "TEXT")
  }
  load <- load_history_snap(con, plan$select, plan$key, plan$key_types, loaded, types)
  as_of <- if (!is.null(series$as_of)) {
    DBI::dbGetQuery(con, sprintf("SELECT %s AS v FROM snap.%s", series$as_of,
                                 history_quote(series$source)))$v
  } else NA_character_
  outcome <- forced %||% series_guard(load$kept, prior$rows %||% NA)
  out <- list(outcome = outcome, rows_read = load$read, rows_kept = load$kept,
              columns = paste(plan$columns, collapse = ","), source_as_of = as.character(as_of))
  if (!identical(outcome, "applied")) return(out)
  fill <- ensure_episode_table(con, series$table, plan$key, plan$key_types, compared,
                               plan$value_types)
  refilled <- if (!is.null(common)) refill_serialized(con, series$table, plan$key,
                                                       series$value, common) else 0L
  counts <- fold_episodes(con, series$table, plan$key, compared, snapshot_on, fill)
  counts$filled <- counts$filled + refilled
  bad <- open_mismatches(con, series$table, plan$key, compared)
  if (bad > 0L) {
    stop(sprintf("%s: %d open episodes disagree with the snapshot after folding %s",
                 series$table, bad, snapshot_on), call. = FALSE)
  }
  c(out, counts)
}

# The check results series share one read and one guard.
apply_check_series <- function(con, family, snapshot_on, series_list, handed_over) {
  obs <- list()
  status <- Filter(function(s) identical(s$kind, "check_flavor"), series_list)[[1]]
  timing <- Filter(function(s) identical(s$name, "check_timing"), series_list)[[1]]
  if (!history_table_exists(con, "cran_check_results", "snap")) {
    obs[[status$name]] <- list(outcome = "absent")
    obs[[timing$name]] <- list(outcome = "absent")
    return(obs)
  }
  raw <- DBI::dbGetQuery(con, "SELECT * FROM snap.cran_check_results")
  norm <- normalize_check_results(raw)
  fp <- check_results_fingerprint(norm)
  cols <- paste(sort(names(norm), method = "radix"), collapse = ",")
  prior <- last_applied(con, family$name, status$name)
  if (handed_over) {
    obs[[status$name]] <- list(outcome = "handed_over", rows_read = nrow(raw),
                               rows_kept = nrow(norm), columns = cols, fingerprint = fp)
    upsert_check_flavors(con, norm$flavor)
  } else {
    fill_flags <- "flags" %in% names(norm) && !is.null(prior) && !("flags" %in% prior$columns)
    step <- check_flavor_step(con, norm, snapshot_on, prior, fill_flags = fill_flags)
    obs[[status$name]] <- c(list(outcome = step$outcome, rows_read = nrow(raw),
                                 rows_kept = nrow(norm), columns = cols, fingerprint = fp),
                            step$counts)
    if (identical(step$outcome, "applied") && step$counts$mismatches > 0L) {
      stop(sprintf("cran_check_flavor_status_history: %d open episodes disagree with the snapshot after folding %s",
                   step$counts$mismatches, snapshot_on), call. = FALSE)
    }
    if (!identical(step$outcome, "applied")) upsert_check_flavors(con, norm$flavor)
  }
  tprior <- last_applied(con, family$name, timing$name)
  forced <- check_flavor_guard(nrow(norm), fp, tprior)
  t_obs <- apply_episode_series(con, timing, snapshot_on, tprior, forced = forced)
  t_obs$fingerprint <- fp
  obs[[timing$name]] <- t_obs
  obs
}
