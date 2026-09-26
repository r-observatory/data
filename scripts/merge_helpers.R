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
  "autoobs-downloads-summary.db" = c("autoobs_downloads_summary"),
  "copr-downloads-summary.db"    = c("copr_downloads_summary"),
  "conda-forge-downloads-summary.db" = c("conda_forge_downloads_summary"),
  "bioconda-downloads-summary.db"    = c("bioconda_downloads_summary"),
  "c2d4u-downloads-summary.db"    = c("c2d4u_downloads_summary"),
  "bioconductor-summary.db"      = c("bioc_downloads_summary"),
  "queue.db"                     = NULL,
  # bioc_vignettes is the current release's vignette list, one row per file with its link.
  "bioconductor-metadata.db"     = c("bioc_packages", "bioc_authors", "bioc_releases", "bioc_view_edges", "bioc_names_all", "bioc_vignettes"),
  "cran-archive.db"              = c("cran_archive", "cran_archive_events", "cran_names_all", "cran_archive_history", "cran_archive_lineage", "cran_archive_action_counts"),
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
                                     "vcs_ai_silent_channels", "repo_package_links"),
  "cran-task-views.db"           = c("cran_task_views", "cran_task_view_events", "cran_task_view_membership")
)

#' Copy one source DB into the output connection: every allowlisted table the
#' source actually has, created from its own CREATE TABLE so keys and
#' WITHOUT ROWID survive, then every index the source declares.
#'
#' A listed table the source does not have is not an error. The allowlist
#' filters the source's own table list, so a table the producer has not
#' published yet simply copies nothing, and adding it here can land before the
#' producer does. An index on a table that was not copied fails to create and is
#' skipped with a note.
#'
#' When the copy fails, whatever it has not committed is rolled back and src is
#' detached before the error propagates, so the next source can still attach.
#'
#' @param con     output connection.
#' @param src_path path to the source SQLite file.
#' @param allow   NULL for every table, or the allowlisted table names.
#' @return named list of rows copied per table, in source order.
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

  if (nrow(tables) > 0) {
    DBI::dbExecute(con, "BEGIN TRANSACTION")

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
      DBI::dbExecute(con, create_sql)

      # Get column list from source table for INSERT
      col_info <- DBI::dbGetQuery(con, sprintf('PRAGMA src.table_info("%s")', tbl_name))
      cols <- col_info$name
      cols_str <- paste(sprintf('"%s"', cols), collapse = ", ")

      # Copy data
      insert_sql <- sprintf(
        'INSERT OR REPLACE INTO "%s" (%s) SELECT %s FROM src."%s"',
        tbl_name, cols_str, cols_str, tbl_name
      )
      n_rows <- DBI::dbExecute(con, insert_sql)
      cat(" ->", n_rows, "rows\n")

      table_stats[[tbl_name]] <- n_rows
    }

    DBI::dbExecute(con, "COMMIT")
  }

  # Copy indexes
  indexes <- DBI::dbGetQuery(con,
    "SELECT sql FROM src.sqlite_master
     WHERE type = 'index' AND sql IS NOT NULL"
  )
  if (nrow(indexes) > 0) {
    for (j in seq_len(nrow(indexes))) {
      idx_sql <- sub(
        "^CREATE INDEX ",
        "CREATE INDEX IF NOT EXISTS ",
        indexes$sql[j],
        ignore.case = TRUE
      )
      # Also handle UNIQUE indexes
      idx_sql <- sub(
        "^CREATE UNIQUE INDEX ",
        "CREATE UNIQUE INDEX IF NOT EXISTS ",
        idx_sql,
        ignore.case = TRUE
      )
      tryCatch(
        DBI::dbExecute(con, idx_sql),
        error = function(e) {
          cat("  Warning: index creation skipped:", conditionMessage(e), "\n")
        }
      )
    }
    cat("  Copied", nrow(indexes), "indexes\n")
  }

  DBI::dbExecute(con, "DETACH DATABASE src")
  done <- TRUE

  table_stats
}

#' Merge every source DB in order into the output connection. A source that is
#' missing is skipped and one that fails is reported, and either way the loop
#' goes on to the next source.
#'
#' @param con         output connection.
#' @param sources_dir directory the source DBs were downloaded into.
#' @param dbs         source DB file names, in merge order.
#' @param tables      per-source allowlists, shaped like source_tables.
#' @return named list keyed by source file: status "merged" (with file_size and
#'   rows per table), "skipped" or "error" (with a reason).
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
      list(
        status = "merged",
        file_size = file_size,
        tables = merge_source_db(con, src_path, tables_to_merge_from(db_file, tables))
      )
    }, error = function(e) {
      # merge_source_db has already rolled back and detached.
      warning("Error processing ", db_file, ": ", conditionMessage(e), call. = FALSE)
      list(
        status = "error",
        reason = conditionMessage(e)
      )
    })

    cat("\n")
  }

  merge_stats
}

#' Post-merge safety check. When a source DB was present, verify its expected
#' tables landed in the output; returns the missing names (character(0) if none).
#' Guards against a partial edit silently dropping the task-view tables.
missing_expected_tables <- function(source_present, expected, present_tables) {
  if (!isTRUE(source_present)) return(character(0))
  setdiff(expected, present_tables)
}
