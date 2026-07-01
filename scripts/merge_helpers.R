# Pure helpers used by merge.R, extracted so they can be unit-tested.

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
