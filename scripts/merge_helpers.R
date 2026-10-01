# Pure helpers and merge source configuration used by merge.R, extracted so
# they can be unit-tested.

# Null-coalescing operator. Defined here so helpers work when this file is
# sourced directly in tests (pipeline_metadata.R is not loaded in that context).
`%||%` <- function(a, b) if (is.null(a)) b else a

#' Rows for copr_packages from the COPR api_3/monitor payload. Keeps only
#' R-CRAN-<pkg> entries (the CRAN packages built in iucar/cran) and strips the
#' prefix to the CRAN package name. Stable 2-column data.frame even when empty.
copr_rows_from_monitor <- function(mon) {
  pkgs <- mon$packages %||% list()
  names_raw <- vapply(pkgs, function(p) p$name %||% NA_character_, character(1))
  keep <- grepl("^R-CRAN-", names_raw)
  nm <- sub("^R-CRAN-", "", names_raw[keep])
  data.frame(name = nm, name_lower = tolower(nm), stringsAsFactors = FALSE)
}

#' Rows for r_versions from the r-hub rversions list (each element has
#' `version` and an ISO `date`). `released` is the date-only (YYYY-MM-DD).
r_versions_from_list <- function(lst) {
  ver  <- vapply(lst, function(v) v$version %||% NA_character_, character(1))
  date <- vapply(lst, function(v) substr(v$date %||% "", 1, 10), character(1))
  ok <- !is.na(ver) & nzchar(ver) & nzchar(date)
  data.frame(version = ver[ok], released = date[ok], stringsAsFactors = FALSE)
}

#' Decide which tables to ingest from a given source DB.
#'
#' @param source_name basename of the source DB (e.g. "downloads-summary.db")
#' @param config      named list mapping source_name -> character vector of
#'                    table names, or NULL meaning "all tables".
#' @return NULL (all tables) or a character vector of allowed table names.
tables_to_merge_from <- function(source_name, config) {
  if (!source_name %in% names(config)) return(NULL)
  config[[source_name]]
}

# ---------------------------------------------------------------------------
# Source databases to merge, in order.
# source_tables: NULL means "merge all tables"; a character vector means
# "merge only these tables".
# ---------------------------------------------------------------------------
source_dbs <- c(
  "feed.db",
  "metadata.db",
  "downloads-summary.db",
  "r2u-summary.db",
  "autoobs-downloads-summary.db",
  "copr-downloads-summary.db",
  "conda-forge-downloads-summary.db",
  "bioconda-downloads-summary.db",
  "c2d4u-downloads-summary.db",
  "bioconductor-summary.db",
  "queue.db",
  "bioconductor-metadata.db",
  "cran-archive.db",
  "cran-code-metrics.db",
  "cran-data-metrics.db",
  "bioc-code-metrics.db",
  "bioc-data-metrics.db",
  "cran-coverage.db",
  "vcs-signals-summary.db",
  "cran-task-views.db"
)

source_tables <- list(
  "feed.db"                      = NULL,
  "metadata.db"                  = NULL,
  "downloads-summary.db"         = c("downloads_summary"),
  "r2u-summary.db"               = c("r2u_downloads_summary"),
  # autoobs_runs tells a day MirrorCache had not aggregated from a zero. The raw
  # counters and the day ledger stay in the pipeline's own assets.
  "autoobs-downloads-summary.db" = c("autoobs_downloads_summary", "autoobs_runs"),
  "copr-downloads-summary.db"    = c("copr_downloads_summary"),
  "conda-forge-downloads-summary.db" = c("conda_forge_downloads_summary"),
  "bioconda-downloads-summary.db"    = c("bioconda_downloads_summary"),
  "c2d4u-downloads-summary.db"    = c("c2d4u_downloads_summary"),
  "bioconductor-summary.db"      = c("bioc_downloads_summary"),
  "queue.db"                     = NULL,
  # bioc_vignettes is the current release's vignette list, one row per file with its link.
  # bioc_build_reports, bioc_build_status_history and bioc_views_history are the daily
  # build report and VIEWS read as episodes; upstream keeps only the latest report.
  "bioconductor-metadata.db"     = c("bioc_packages", "bioc_authors", "bioc_releases", "bioc_view_edges", "bioc_names_all", "bioc_vignettes",
                                     "bioc_build_reports", "bioc_build_status_history", "bioc_views_history"),
  # cran_tarballs is the exact size, mtime and MD5 of every CRAN source tarball,
  # one row per file, so a same-version re-upload is its own revision.
  "cran-archive.db"              = c("cran_archive", "cran_archive_events", "cran_names_all", "cran_archive_history", "cran_archive_lineage", "cran_archive_action_counts", "cran_tarballs"),
  # Code tables only; dataset tables now live in the *-data-metrics.db sources.
  # The dataset row_sketch table is deliberately EXCLUDED: it is an offline
  # near-duplicate structure that the viewer never queries, so it stays in the
  # source db and does not inflate observatory.db.
  # cran_vignettes is one row per vignette per version: which ones a package
  # ships, what they render to, and who wrote them. The summary's n_vignettes
  # counts the same rows, so a page can show the count without this table and
  # can name the vignettes only with it.
  # *_description_fields and *_release_notes hold the latest analysed version only. The
  # per-version history stays with each pipeline (cran-release-text.db, bioc-code-metrics.db).
  "cran-code-metrics.db"         = c("cran_code_summary", "cran_api_history", "cran_functions", "cran_call_edges", "cran_code_churn", "cran_archived_meta", "cran_author_package_span", "cran_vignettes", "cran_description_fields", "cran_release_notes"),
  "cran-data-metrics.db"         = c("cran_datasets", "cran_dataset_versions", "cran_dataset_contents"),
  "bioc-code-metrics.db"         = c("bioc_code_summary", "bioc_api_history", "bioc_functions", "bioc_call_edges", "bioc_code_churn", "bioc_description_fields", "bioc_release_notes"),
  "bioc-data-metrics.db"         = c("bioc_datasets", "bioc_dataset_versions", "bioc_dataset_contents"),
  "cran-coverage.db"             = c("coverage_summary", "coverage_file", "coverage_function"),
  # vcs_ai_models is one row per repo per tool per model; vcs_ai_rule_inventory
  # states each tier's breadth so a page reads the denominator instead of
  # asserting it; vcs_ai_silent_channels says which channels found nothing
  # anywhere and whether anyone has explained it, so a quiet channel can render
  # as "not measured" rather than as a confident zero.
  # repo_package_links is the only way to reach a package's repository once the
  # package has left CRAN or Bioconductor. vcs_signals_summary is rebuilt from
  # today's listings, so a delisted package loses its row the same day while its
  # AI and dev-tooling rows stay behind, keyed only by repo_id. The link table
  # is append-only (last_seen stops moving instead of the row going) and keys on
  # (repo_id, package, origin). repo_packages stays out: it is today's mapping
  # only, which vcs_signals_summary already carries.
  "vcs-signals-summary.db"       = c("vcs_signals_summary", "vcs_ai_signals", "vcs_dev_tooling",
                                     "vcs_ai_models", "vcs_ai_rule_inventory",
                                     "vcs_ai_silent_channels", "repo_package_links",
                                     # The rule and ruleset behind each dev-tooling column.
                                     "vcs_dev_tooling_rules",
                                     # Read by the AI pages; the producer's read state and search log stay out.
                                     "vcs_ai_search_coverage", "vcs_ai_review_signals",
                                     "vcs_ai_outside_prs", "vcs_ai_ruleset_history",
                                     # The only merged table that carries node ids and current owners,
                                     # so a moved or renamed repository is counted once.
                                     "vcs_repo_owner",
                                     # Pull requests per repository and quarter, the span each
                                     # repository's counts cover, and dated renames and transfers.
                                     # The walk cursor in vcs_ai_repo_reads stays out.
                                     "vcs_pr_quarterly", "vcs_pr_coverage", "vcs_repo_name_history"),
  "cran-task-views.db"           = c("cran_task_views", "cran_task_view_events", "cran_task_view_membership")
)

#' <table>__<index>, for an index whose own name is taken in the output. Cut to
#' 64 characters, the most MySQL allows, since the viewer mirrors index names.
qualified_index_name <- function(tbl, idx) {
  substr(paste0(tbl, "__", idx), 1L, 64L)
}

#' What holds a name in the output. Tables, indexes and views share one
#' namespace, matched without regard to case.
schema_holder <- function(con, name) {
  DBI::dbGetQuery(con,
    "SELECT type, tbl_name FROM main.sqlite_master WHERE lower(name) = lower(?)",
    params = list(name))
}

#' A source's CREATE INDEX under a new name, UNIQUE and everything from ON
#' onwards kept as written. NA when the statement cannot be read.
index_sql_as <- function(sql, new_name) {
  pat <- paste0("^\\s*CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?",
                "(?:\"(?:[^\"]|\"\")+\"|\\[[^\\]]+\\]|`(?:[^`]|``)+`|[^\\s(]+)\\s*ON\\s")
  if (!grepl(pat, sql, perl = TRUE, ignore.case = TRUE)) return(NA_character_)
  quoted <- paste0('"', gsub('"', '""', new_name, fixed = TRUE), '"')
  sub(pat, paste0("CREATE \\1INDEX ", gsub("\\", "\\\\", quoted, fixed = TRUE), " ON "),
      sql, perl = TRUE, ignore.case = TRUE)
}

#' Create one source index in the output. IF NOT EXISTS quietly skipped a name
#' another table's index already had, which left the Bioconductor code-metrics
#' tables without theirs, so such a name now goes on as <table>__<index>.
#'
#' @return one-row data.frame: index, table, created_as, outcome ("created",
#'   "renamed", "present" or "failed") and note.
replay_index <- function(con, idx, tbl, sql) {
  row <- function(created_as, outcome, note = "") {
    data.frame(index = idx, table = tbl, created_as = created_as,
               outcome = outcome, note = note, stringsAsFactors = FALSE)
  }
  on_this_table <- function(holder) {
    nrow(holder) > 0 && holder$type[1] == "index" &&
      tolower(holder$tbl_name[1]) == tolower(tbl)
  }
  run <- function(stmt, created_as, outcome, note = "") {
    tryCatch({
      DBI::dbExecute(con, stmt)
      row(created_as, outcome, note)
    }, error = function(e) row(NA_character_, "failed", conditionMessage(e)))
  }

  holder <- schema_holder(con, idx)
  if (nrow(holder) == 0) return(run(sql, idx, "created"))
  if (on_this_table(holder)) {
    return(row(idx, "present", sprintf("%s already has an index by this name", tbl)))
  }

  held_by <- function(h) {
    switch(h$type[1],
           index = sprintf("an index on %s", h$tbl_name[1]),
           trigger = sprintf("a trigger on %s", h$tbl_name[1]),
           sprintf("%s %s", h$type[1], h$tbl_name[1]))
  }
  taken_by <- held_by(holder)
  new_name <- qualified_index_name(tbl, idx)
  again <- schema_holder(con, new_name)
  if (on_this_table(again)) {
    return(row(new_name, "present", sprintf("%s already has %s", tbl, new_name)))
  }
  if (nrow(again) > 0) {
    return(row(NA_character_, "failed", sprintf(
      "the name is taken by %s and %s by %s", taken_by, new_name, held_by(again))))
  }
  new_sql <- index_sql_as(sql, new_name)
  if (is.na(new_sql)) {
    return(row(NA_character_, "failed", sprintf(
      "the name is taken by %s and its CREATE INDEX could not be renamed", taken_by)))
  }
  run(new_sql, new_name, "renamed", sprintf("the name is taken by %s", taken_by))
}

#' Copy one source DB into the output connection: every allowlisted table the
#' source actually has, created from its own CREATE TABLE so keys and
#' WITHOUT ROWID survive, then every index the source declares on a table that
#' copied.
#'
#' A listed table the source does not have is not an error. The allowlist
#' filters the source's own table list, so a table the producer has not
#' published yet simply copies nothing, and adding it here can land before the
#' producer does. An index on a table that did not copy is skipped with a note.
#'
#' Each table copies in its own transaction, so a table that fails is rolled
#' back, table and all, and named in `failed` without costing the others.
#'
#' When something outside a table's copy fails, whatever it has not committed is
#' rolled back and src is detached before the error propagates, so the next
#' source can still attach.
#'
#' @param con     output connection.
#' @param src_path path to the source SQLite file.
#' @param allow   NULL for every table, or the allowlisted table names.
#' @return list: tables (rows copied per table, in source order), failed (named
#'   character, table -> error) and indexes (one row per source index, see
#'   replay_index, with "skipped" for a table that did not copy).
merge_source_db <- function(con, src_path, allow) {
  DBI::dbExecute(con, "ATTACH DATABASE ? AS src", params = list(src_path))

  # SQLite refuses to detach a database while a transaction is open ("database
  # src is locked"), so a failed copy has to roll back before it detaches.
  # merge.R's error handler used to try them the other way round: the detach
  # failed, src stayed attached, and every later source then failed to attach
  # with "database src is already in use". One bad source cost every source
  # after it, cran-task-views always among them since it is last, and merge.R
  # refuses the release when the task-view tables are missing. ATTACH cannot
  # run inside a transaction, so any transaction open here is this copy's own.
  done <- FALSE
  on.exit(if (!done) {
    tryCatch(DBI::dbExecute(con, "ROLLBACK"), error = function(e) NULL)
    tryCatch(DBI::dbExecute(con, "DETACH DATABASE src"), error = function(e) NULL)
  }, add = TRUE)

  tables <- DBI::dbGetQuery(con,
    "SELECT name, sql FROM src.sqlite_master
     WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
  )

  if (!is.null(allow)) {
    tables <- tables[tables$name %in% allow, , drop = FALSE]
    cat("  Allowlist: copying only [", paste(allow, collapse = ", "), "]\n")
  }

  table_stats <- list()
  failed <- character()

  for (i in seq_len(nrow(tables))) {
    tbl_name <- tables$name[i]
    tbl_sql  <- tables$sql[i]

    cat("  Table:", tbl_name)

    # Create table if not exists, from the source's own CREATE TABLE
    create_sql <- sub(
      "^CREATE TABLE ",
      "CREATE TABLE IF NOT EXISTS ",
      tbl_sql,
      ignore.case = TRUE
    )

    copied <- tryCatch({
      DBI::dbExecute(con, "BEGIN TRANSACTION")
      DBI::dbExecute(con, create_sql)

      # Get column list from source table for INSERT
      col_info <- DBI::dbGetQuery(con, sprintf('PRAGMA src.table_info("%s")', tbl_name))
      cols_str <- paste(sprintf('"%s"', col_info$name), collapse = ", ")

      n_rows <- DBI::dbExecute(con, sprintf(
        'INSERT OR REPLACE INTO "%s" (%s) SELECT %s FROM src."%s"',
        tbl_name, cols_str, cols_str, tbl_name
      ))
      DBI::dbExecute(con, "COMMIT")
      n_rows
    }, error = function(e) {
      tryCatch(DBI::dbExecute(con, "ROLLBACK"), error = function(e2) NULL)
      structure(conditionMessage(e), class = "copy_failed")
    })

    if (inherits(copied, "copy_failed")) {
      failed[[tbl_name]] <- unclass(copied)
      cat(" -> FAILED, not copied:", unclass(copied), "\n")
    } else {
      table_stats[[tbl_name]] <- copied
      cat(" ->", copied, "rows\n")
    }
  }

  indexes <- DBI::dbGetQuery(con,
    "SELECT name, tbl_name, sql FROM src.sqlite_master
     WHERE type = 'index' AND sql IS NOT NULL"
  )
  index_rows <- list(data.frame(index = character(), table = character(),
                                created_as = character(), outcome = character(),
                                note = character(), stringsAsFactors = FALSE))
  for (j in seq_len(nrow(indexes))) {
    idx <- indexes$name[j]
    tbl <- indexes$tbl_name[j]
    res <- if (tbl %in% names(table_stats)) {
      replay_index(con, idx, tbl, indexes$sql[j])
    } else {
      why <- if (tbl %in% names(failed)) "its table failed to copy" else "its table is not copied"
      data.frame(index = idx, table = tbl, created_as = NA_character_,
                 outcome = "skipped", note = why, stringsAsFactors = FALSE)
    }
    if (res$outcome == "renamed") {
      cat(sprintf("  Index %s: %s, so created on %s as %s\n", idx, res$note, tbl, res$created_as))
    } else if (res$outcome != "created") {
      cat(sprintf("  Index %s on %s %s: %s\n", idx, tbl, res$outcome, res$note))
    }
    index_rows[[length(index_rows) + 1L]] <- res
  }
  index_rows <- do.call(rbind, index_rows)
  if (nrow(index_rows) > 0) {
    counts <- table(factor(index_rows$outcome,
                           levels = c("created", "renamed", "present", "skipped", "failed")))
    counts <- counts[counts > 0]
    cat("  Indexes: ", paste(counts, names(counts), collapse = ", "), "\n", sep = "")
  }

  DBI::dbExecute(con, "DETACH DATABASE src")
  done <- TRUE

  list(tables = table_stats, failed = failed, indexes = index_rows)
}

#' What went wrong with one source's copy, as one line, or "" when nothing did.
source_failure_detail <- function(stats) {
  if (is.null(stats) || !identical(stats$status, "error")) return("")
  parts <- character()
  failed <- stats$failed_tables %||% character()
  if (length(failed)) {
    parts <- c(parts, sprintf("table %s: %s", names(failed), failed))
  }
  idx <- stats$indexes
  if (!is.null(idx) && nrow(idx)) {
    bad <- idx[idx$outcome == "failed", , drop = FALSE]
    if (nrow(bad)) parts <- c(parts, sprintf("index %s on %s: %s", bad$index, bad$table, bad$note))
  }
  if (!length(parts)) parts <- sprintf("nothing merged: %s", stats$reason %||% "unknown error")
  gsub("[\t\r\n]+", " ", paste(parts, collapse = "; "))
}

#' Merge every source DB in order into the output connection. A source that is
#' missing is skipped and one that fails is reported, and either way the loop
#' goes on to the next source.
#'
#' @param con         output connection.
#' @param sources_dir directory the source DBs were downloaded into.
#' @param dbs         source DB file names, in merge order.
#' @param tables      per-source allowlists, shaped like source_tables.
#' @return named list keyed by source file: status "merged", "skipped" or
#'   "error". A source that copied carries file_size, tables (rows per table),
#'   failed_tables and indexes, and is "error" when any table or index failed.
#'   One that failed outright carries only the reason.
merge_sources <- function(con, sources_dir, dbs = source_dbs, tables = source_tables) {
  merge_stats <- list()

  for (db_file in dbs) {
    src_path <- file.path(sources_dir, db_file)
    cat("--- Processing:", db_file, "---\n")

    if (!file.exists(src_path)) {
      warning("Source DB not found, skipping: ", src_path, call. = FALSE)
      merge_stats[[db_file]] <- list(
        status = "skipped",
        reason = "file not found"
      )
      next
    }

    file_size <- file.info(src_path)$size
    cat("  File size:", format(file_size, big.mark = ","), "bytes\n")

    merge_stats[[db_file]] <- tryCatch({
      res <- merge_source_db(con, src_path, tables_to_merge_from(db_file, tables))
      whole <- !length(res$failed) && !any(res$indexes$outcome == "failed")
      list(
        status = if (whole) "merged" else "error",
        file_size = file_size,
        tables = res$tables,
        failed_tables = res$failed,
        indexes = res$indexes
      )
    }, error = function(e) {
      # merge_source_db has already rolled back and detached.
      list(
        status = "error",
        reason = conditionMessage(e)
      )
    })
    if (identical(merge_stats[[db_file]]$status, "error")) {
      warning("Error processing ", db_file, ": ",
              source_failure_detail(merge_stats[[db_file]]), call. = FALSE)
    }

    cat("\n")
  }

  merge_stats
}

#' Every source that did not merge whole, as source -> what failed. The gate
#' reads this, so a source that published without some of its tables fails the
#' run rather than passing as present.
merge_failures <- function(merge_stats) {
  out <- character()
  for (db_file in names(merge_stats)) {
    detail <- source_failure_detail(merge_stats[[db_file]])
    if (nzchar(detail)) out[[db_file]] <- detail
  }
  out
}

#' merge.R and check-freshness.R run as separate steps, so the failures travel
#' as a file beside .integrity-failed: one "source<TAB>detail" line each. No
#' failures means no file, and a stale one is removed.
write_merge_failures <- function(path, failures) {
  if (file.exists(path)) unlink(path)
  if (length(failures)) writeLines(paste(names(failures), failures, sep = "\t"), path)
  invisible(path)
}

read_merge_failures <- function(path) {
  if (!file.exists(path)) return(character())
  lines <- readLines(path, warn = FALSE)
  lines <- lines[nzchar(trimws(lines))]
  if (!length(lines)) return(character())
  src <- sub("\t.*$", "", lines)
  detail <- ifelse(grepl("\t", lines, fixed = TRUE), sub("^[^\t]*\t", "", lines), "")
  stats::setNames(detail, src)
}

#' One source's row in the release notes table.
source_note_row <- function(db_file, stats) {
  if (is.null(stats)) return(sprintf("| %s | unknown | - | - | - |", db_file))
  if (identical(stats$status, "skipped")) {
    return(sprintf("| %s | skipped (%s) | - | - | - |", db_file, stats$reason))
  }
  if (is.null(stats$tables)) return(sprintf("| %s | error | - | - | - |", db_file))
  status <- stats$status
  failed <- names(stats$failed_tables %||% character())
  if (length(failed)) status <- sprintf("%s, failed: %s", status, paste(failed, collapse = ", "))
  bad_idx <- if (!is.null(stats$indexes)) stats$indexes$index[stats$indexes$outcome == "failed"] else character()
  if (length(bad_idx)) {
    status <- sprintf("%s, index failed: %s", status, paste(bad_idx, collapse = ", "))
  }
  tbl_names <- names(stats$tables)
  sprintf("| %s | %s | %s | %s (%d) | %s |",
          db_file, status,
          format(stats$file_size, big.mark = ","),
          paste(tbl_names, collapse = ", "),
          length(tbl_names),
          format(sum(unlist(stats$tables)), big.mark = ","))
}

#' Post-merge safety check. When a source DB was present, verify its expected
#' tables landed in the output; returns the missing names (character(0) if none).
#' Guards against a partial edit silently dropping the task-view tables.
missing_expected_tables <- function(source_present, expected, present_tables) {
  if (!isTRUE(source_present)) return(character(0))
  setdiff(expected, present_tables)
}
