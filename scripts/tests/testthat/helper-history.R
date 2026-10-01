# Fixtures shared by the test-history-*.R files.

# The producers' CREATE TABLE statements, by table name.
history_source_ddl <- local({
  path <- file.path(getwd(), "fixtures", "history-source-ddl.sql")
  if (!file.exists(path)) return(character(0))
  text <- readLines(path)
  text <- paste(text[!grepl("^--", text)], collapse = "\n")
  stmts <- trimws(strsplit(text, ";", fixed = TRUE)[[1]])
  stmts <- stmts[nzchar(stmts)]
  names(stmts) <- gsub("`", "", regmatches(stmts, regexpr("(?<=CREATE TABLE )`?[A-Za-z0-9_]+",
                                                            stmts, perl = TRUE)))
  stmts
})

# A db at `path` holding `tables` (name -> data.frame), each made by its
# producer's DDL unless `ddl` names another statement.
write_source_db <- function(path, tables, ddl = list()) {
  unlink(path)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con))
  for (t in names(tables)) {
    DBI::dbExecute(con, ddl[[t]] %||% history_source_ddl[[t]])
    if (nrow(tables[[t]]) > 0L) DBI::dbAppendTable(con, t, tables[[t]])
  }
  path
}

history_test_db <- function(env = parent.frame()) {
  path <- withr::local_tempfile(fileext = ".db", .local_envir = env)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  withr::defer(DBI::dbDisconnect(con), envir = env)
  ensure_history_ledger(con)
  con
}

# A gh release listing.
releases <- function(tags, published, draft = FALSE) {
  data.frame(tag = tags, published_at = published, is_draft = draft, stringsAsFactors = FALSE)
}
