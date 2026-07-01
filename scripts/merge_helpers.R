# Pure helpers used by merge.R, extracted so they can be unit-tested.

#' Turn an available.packages() matrix into bioc_packages registry rows.
#' Uses the matrix rownames as the canonical package name (available.packages
#' keys rows by Package), the Version column for version, and the supplied
#' category. Returns a data.frame with a stable column order even when empty.
bioc_rows_from_available <- function(mat, category) {
  nm  <- rownames(mat)
  if (is.null(nm)) nm <- character(0)
  ver <- if ("Version" %in% colnames(mat)) unname(mat[, "Version"]) else rep(NA_character_, length(nm))
  data.frame(
    name       = as.character(nm),
    name_lower = tolower(as.character(nm)),
    version    = as.character(ver),
    category   = rep(category, length(nm)),
    stringsAsFactors = FALSE
  )
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
