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
  "bioconductor-metadata.db"     = c("bioc_packages", "bioc_authors", "bioc_releases", "bioc_view_edges", "bioc_names_all"),
  "cran-archive.db"              = c("cran_archive", "cran_archive_events", "cran_names_all", "cran_archive_history", "cran_archive_lineage", "cran_archive_action_counts"),
  # Code tables only; dataset tables now live in the *-data-metrics.db sources.
  # The dataset row_sketch table is deliberately EXCLUDED: it is an offline
  # near-duplicate structure that the viewer never queries, so it stays in the
  # source db and does not inflate observatory.db.
  "cran-code-metrics.db"         = c("cran_code_summary", "cran_api_history", "cran_functions", "cran_call_edges", "cran_archived_meta", "cran_author_package_span"),
  "cran-data-metrics.db"         = c("cran_datasets", "cran_dataset_versions", "cran_dataset_contents"),
  "bioc-code-metrics.db"         = c("bioc_code_summary", "bioc_api_history", "bioc_functions", "bioc_call_edges"),
  "bioc-data-metrics.db"         = c("bioc_datasets", "bioc_dataset_versions", "bioc_dataset_contents"),
  "cran-coverage.db"             = c("coverage_summary", "coverage_file", "coverage_function"),
  "vcs-signals-summary.db"       = c("vcs_signals_summary", "vcs_ai_signals", "vcs_dev_tooling"),
  "cran-task-views.db"           = c("cran_task_views", "cran_task_view_events", "cran_task_view_membership")
)

#' Post-merge safety check. When a source DB was present, verify its expected
#' tables landed in the output; returns the missing names (character(0) if none).
#' Guards against a partial edit silently dropping the task-view tables.
missing_expected_tables <- function(source_present, expected, present_tables) {
  if (!isTRUE(source_present)) return(character(0))
  setdiff(expected, present_tables)
}
