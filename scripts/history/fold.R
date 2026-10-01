# Episode fold for the history series. An episode is one (key, episode_seq)
# row whose value held from first_seen to last_seen; ended_on is the first
# snapshot where it no longer held, so a change lies in (last_seen, ended_on].

HISTORY_MIN_FRACTION <- 0.5

history_quote <- function(x) paste0('"', gsub('"', '""', x, fixed = TRUE), '"')

history_ledger_ddl <- c(
  "CREATE TABLE IF NOT EXISTS history_snapshots (
     family TEXT NOT NULL, tag TEXT NOT NULL,
     snapshot_at TEXT, snapshot_on TEXT NOT NULL,
     asset TEXT, bytes INTEGER, sha256 TEXT,
     outcome TEXT NOT NULL, note TEXT, processed_at TEXT NOT NULL,
     PRIMARY KEY (family, tag),
     CHECK (outcome IN ('processed', 'superseded', 'skipped', 'failed'))) WITHOUT ROWID",
  "CREATE TABLE IF NOT EXISTS history_series_observations (
     family TEXT NOT NULL, tag TEXT NOT NULL, series TEXT NOT NULL,
     rows_read INTEGER, rows_kept INTEGER, outcome TEXT NOT NULL,
     source_as_of TEXT, columns TEXT, fingerprint TEXT,
     filled INTEGER, extended INTEGER, closed INTEGER, opened INTEGER,
     PRIMARY KEY (family, tag, series),
     CHECK (outcome IN ('applied', 'stale', 'unhealthy', 'absent', 'handed_over'))) WITHOUT ROWID",
  "CREATE TABLE IF NOT EXISTS history_settings (
     key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")

ensure_history_ledger <- function(con) {
  for (sql in history_ledger_ddl) DBI::dbExecute(con, sql)
  invisible(NULL)
}

# Columns of a table as data.frame(name, type), empty when it does not exist.
history_table_columns <- function(con, table, schema = "main") {
  info <- DBI::dbGetQuery(con, sprintf("PRAGMA %s.table_info(%s)", schema, history_quote(table)))
  data.frame(name = as.character(info$name), type = as.character(info$type),
             stringsAsFactors = FALSE)
}

history_table_exists <- function(con, table, schema = "main") {
  n <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM %s.sqlite_master WHERE type = 'table' AND name = ?",
    schema), params = list(table))$n
  n > 0L
}

episode_table_sql <- function(table, key, key_types, values, value_types) {
  cols <- c(sprintf("%s %s NOT NULL", history_quote(key), key_types),
            "episode_seq INTEGER NOT NULL",
            trimws(sprintf("%s %s", history_quote(values), value_types)),
            "first_seen TEXT NOT NULL", "last_seen TEXT NOT NULL", "ended_on TEXT")
  keys <- paste(history_quote(key), collapse = ", ")
  c(sprintf(paste0("CREATE TABLE %s (\n  %s,\n  PRIMARY KEY (%s, episode_seq),\n",
                   "  CHECK (last_seen >= first_seen),\n",
                   "  CHECK (ended_on IS NULL OR ended_on > last_seen)) WITHOUT ROWID"),
            history_quote(table), paste(cols, collapse = ",\n  "), keys),
    sprintf("CREATE UNIQUE INDEX %s ON %s (%s) WHERE ended_on IS NULL",
            history_quote(paste0("ux_", table, "_open")), history_quote(table), keys))
}

# Creates the episode table on first use and adds any value column it lacks.
# Returns the added columns, which the fold fills rather than compares.
ensure_episode_table <- function(con, table, key, key_types, values, value_types) {
  if (!history_table_exists(con, table)) {
    for (sql in episode_table_sql(table, key, key_types, values, value_types)) {
      DBI::dbExecute(con, sql)
    }
    return(character(0))
  }
  have <- history_table_columns(con, table)$name
  added <- setdiff(values, have)
  for (col in added) {
    type <- value_types[match(col, values)]
    DBI::dbExecute(con, trimws(sprintf("ALTER TABLE %s ADD COLUMN %s %s",
                                       history_quote(table), history_quote(col), type)))
  }
  added
}

# temp.history_snap from a SELECT returning key columns then value columns.
# A row with a NULL key, or a key already loaded, is left out: rows are taken
# in the order of every column, so the row kept is always the same one.
load_history_snap <- function(con, select_sql, key, key_types, values, value_types) {
  DBI::dbExecute(con, "DROP TABLE IF EXISTS temp.history_snap")
  cols <- c(sprintf("%s %s NOT NULL", history_quote(key), key_types),
            trimws(sprintf("%s %s", history_quote(values), value_types)))
  DBI::dbExecute(con, sprintf("CREATE TEMP TABLE history_snap (%s, PRIMARY KEY (%s))",
                              paste(cols, collapse = ", "),
                              paste(history_quote(key), collapse = ", ")))
  read <- DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) AS n FROM (%s)", select_sql))$n
  order_by <- paste(seq_len(length(key) + length(values)), collapse = ", ")
  kept <- DBI::dbExecute(con, sprintf(
    "INSERT OR IGNORE INTO temp.history_snap SELECT * FROM (%s) ORDER BY %s",
    select_sql, order_by))
  list(read = as.integer(read), kept = as.integer(kept))
}

series_guard <- function(n_rows, prior_rows = NA) {
  if (n_rows == 0L) return("unhealthy")
  if (length(prior_rows) == 1L && !is.na(prior_rows) &&
      n_rows < HISTORY_MIN_FRACTION * prior_rows) {
    return("unhealthy")
  }
  "applied"
}

# Folds temp.history_snap into `table`. `values` are the columns compared;
# `fill` are columns new to the table, written into matching open episodes
# first so that their arrival is not a change. A column the snapshot lacks is
# not compared, and new episodes leave it NULL.
fold_episodes <- function(con, table, key, values, snapshot_on, fill = character(0)) {
  t <- history_quote(table)
  on <- paste(sprintf("s.%1$s = h.%1$s", history_quote(key)), collapse = " AND ")
  filled <- 0L
  if (length(fill) > 0L) {
    sets <- paste(sprintf("%1$s = s.%1$s", history_quote(fill)), collapse = ", ")
    filled <- DBI::dbExecute(con, sprintf(
      "UPDATE %s AS h SET %s FROM temp.history_snap AS s WHERE h.ended_on IS NULL AND %s",
      t, sets, on))
  }
  same <- paste(c(on, sprintf("s.%1$s IS h.%1$s", history_quote(values))), collapse = " AND ")
  extended <- DBI::dbExecute(con, sprintf(
    "UPDATE %s AS h SET last_seen = ? FROM temp.history_snap AS s
      WHERE h.ended_on IS NULL AND %s", t, same), params = list(snapshot_on))
  closed <- DBI::dbExecute(con, sprintf(
    "UPDATE %s SET ended_on = ? WHERE ended_on IS NULL AND last_seen < ?", t),
    params = list(snapshot_on, snapshot_on))
  cols <- paste(history_quote(c(key, values)), collapse = ", ")
  s_cols <- paste0("s.", history_quote(c(key, values)), collapse = ", ")
  opened <- DBI::dbExecute(con, sprintf(
    "INSERT INTO %1$s (%2$s, episode_seq, first_seen, last_seen)
     SELECT %3$s, COALESCE((SELECT MAX(h.episode_seq) FROM %1$s AS h WHERE %4$s), 0) + 1, ?, ?
       FROM temp.history_snap AS s
      WHERE NOT EXISTS (SELECT 1 FROM %1$s AS h WHERE %4$s AND h.ended_on IS NULL)",
    t, cols, s_cols, on), params = list(snapshot_on, snapshot_on))
  list(filled = filled, extended = extended, closed = closed, opened = opened)
}

# Rows on either side with no equal partner between the open episodes and
# temp.history_snap. 0 means the open episodes are exactly the snapshot.
open_mismatches <- function(con, table, key, values) {
  t <- history_quote(table)
  same <- paste(c(sprintf("s.%1$s = h.%1$s", history_quote(key)),
                  sprintf("s.%1$s IS h.%1$s", history_quote(values))), collapse = " AND ")
  missing <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM temp.history_snap AS s
      WHERE NOT EXISTS (SELECT 1 FROM %s AS h WHERE h.ended_on IS NULL AND %s)",
    t, same))$n
  extra <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n FROM %s AS h WHERE h.ended_on IS NULL
        AND NOT EXISTS (SELECT 1 FROM temp.history_snap AS s WHERE %s)", t, same))$n
  as.integer(missing + extra)
}
