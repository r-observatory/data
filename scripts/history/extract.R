# Folds the history series out of dated releases, oldest first, one snapshot
# on disk at a time. Resumable: a tag already in history_snapshots is never
# read again, and each snapshot commits in one transaction.

HISTORY_LIST_LIMIT <- 1000L

history_gh <- function(args) {
  err <- tempfile("gh-err-")
  on.exit(unlink(err), add = TRUE)
  out <- suppressWarnings(system2("gh", shQuote(args), stdout = TRUE, stderr = err))
  status <- attr(out, "status")
  list(status = if (is.null(status)) 0L else as.integer(status), out = out,
       err = if (file.exists(err)) readLines(err, warn = FALSE) else character(0))
}

history_gh_json <- function(args) {
  res <- history_gh(args)
  if (res$status != 0L) {
    stop(sprintf("gh %s failed: %s", paste(args[1:2], collapse = " "),
                 paste(res$err, collapse = " ")), call. = FALSE)
  }
  jsonlite::fromJSON(paste(res$out, collapse = "\n"), simplifyVector = TRUE)
}

history_list_args <- function(repo) {
  c("release", "list", "--repo", repo, "--exclude-drafts",
    "--limit", as.character(HISTORY_LIST_LIMIT), "--json", "tagName,publishedAt,isDraft")
}

history_file_sha256 <- function(path) {
  if ("sha256sum" %in% getNamespaceExports("tools")) {
    return(unname(getExportedValue("tools", "sha256sum")(path)))
  }
  tool <- if (nzchar(Sys.which("sha256sum"))) c("sha256sum") else c("shasum", "-a", "256")
  out <- system2(tool[1], c(tool[-1], shQuote(path)), stdout = TRUE)
  sub(" .*$", "", out[1])
}

default_history_io <- function() {
  list(
    list_releases = function(repo) {
      r <- history_gh_json(history_list_args(repo))
      if (length(r) == 0L) {
        return(data.frame(tag = character(0), published_at = character(0),
                          is_draft = logical(0), stringsAsFactors = FALSE))
      }
      data.frame(tag = r$tagName, published_at = r$publishedAt, is_draft = r$isDraft,
                 stringsAsFactors = FALSE)
    },
    asset_info = function(repo, tag) {
      r <- history_gh_json(c("release", "view", tag, "--repo", repo, "--json", "assets"))$assets
      if (length(r) == 0L || nrow(r) == 0L) {
        return(data.frame(name = character(0), size = numeric(0), digest = character(0),
                          state = character(0), stringsAsFactors = FALSE))
      }
      if (is.null(r$digest)) r$digest <- NA_character_
      data.frame(name = r$name, size = as.numeric(r$size), digest = r$digest,
                 state = r$state, stringsAsFactors = FALSE)
    },
    download = function(repo, tag, asset, dir) {
      history_gh(c("release", "download", tag, "--repo", repo, "--pattern", asset,
                   "--dir", dir, "--clobber"))$status == 0L
    },
    unzstd = function(src, dest) {
      identical(system2("zstd", c("-dq", "-f", shQuote(src), "-o", shQuote(dest))), 0L)
    },
    free_gib = function(path) {
      out <- system2("df", c("-Pk", shQuote(path)), stdout = TRUE)
      as.numeric(strsplit(trimws(out[length(out)]), "\\s+")[[1]][4]) / 1024^2
    },
    release_http_status = function(repo, tag) {
      res <- history_gh(c("api", "-i", sprintf("repos/%s/releases/tags/%s", repo, tag)))
      first <- grep("^HTTP/", res$out, value = TRUE)[1]
      if (is.na(first)) NA_integer_ else as.integer(strsplit(first, " ")[[1]][2])
    },
    create_release = function(args) history_gh(args)$status == 0L,
    upload = function(repo, tag, files) {
      history_gh(c("release", "upload", tag, files, "--repo", repo))$status == 0L
    },
    sleep = Sys.sleep,
    today = function() format(Sys.time(), "%Y-%m-%d", tz = "UTC"),
    now = function() format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
}

# Which listed tags to fold, record as skipped or superseded, or leave for a
# later run. `recorded` is the family's history_snapshots rows. Returns the
# rows still to record, oldest first, with action and note. A family whose
# day's tag can still be replaced waits until the day is over.
plan_family_snapshots <- function(listing, family, recorded, today) {
  l <- listing[!listing$is_draft, c("tag", "published_at"), drop = FALSE]
  l$snapshot_on <- tag_snapshot_on(family, l$tag)
  settled <- if (isTRUE(family$replaces_same_day)) l$snapshot_on < today else l$snapshot_on <= today
  l <- l[!is.na(l$snapshot_on) & !(l$tag %in% recorded$tag) & settled, , drop = FALSE]
  l <- l[order(l$snapshot_on, l$published_at, l$tag, method = "radix"), , drop = FALSE]
  l$action <- rep("process", nrow(l))
  l$note <- rep(NA_character_, nrow(l))
  l$superseded_by <- rep(NA_character_, nrow(l))
  if (nrow(l) == 0L) return(l)
  pre <- !is.na(family$from) & l$snapshot_on < family$from
  l$action[pre] <- "skipped"
  l$note[pre] <- "predates the history series"
  done <- recorded$snapshot_on[recorded$outcome == "processed"]
  if (length(done) > 0L) {
    last <- max(done)
    late <- !pre & l$snapshot_on <= last
    l$action[late] <- "superseded"
    l$note[late] <- sprintf("published after %s was folded", last)
  }
  todo <- which(l$action == "process")
  last_of_day <- !duplicated(l$snapshot_on[todo], fromLast = TRUE)
  earlier <- todo[!last_of_day]
  if (length(earlier) > 0L) {
    keep <- todo[last_of_day]
    l$superseded_by[earlier] <- l$tag[keep][match(l$snapshot_on[earlier], l$snapshot_on[keep])]
    l$action[earlier] <- "superseded"
    l$note[earlier] <- sprintf("a later release the same day: %s", l$superseded_by[earlier])
  }
  rownames(l) <- NULL
  l
}

HISTORY_DOWNLOAD_WAITS <- c(15, 60)

# Downloads a tag's database asset, checks its size and sha256 against the
# release, expands a .zst, and returns list(path, asset, bytes, sha256). Stops
# before downloading when free space would fall under the floor.
fetch_snapshot <- function(io, family, tag, workdir, min_free_gib) {
  assets <- io$asset_info(family$repo, tag)
  ready <- assets[assets$state == "uploaded", , drop = FALSE]
  pick <- family$assets[family$assets %in% ready$name][1]
  if (is.na(pick)) {
    stop(sprintf("%s@%s has no uploaded %s; record it with --give-up=%s:%s",
                 family$repo, tag, paste(family$assets, collapse = " or "),
                 family$name, tag), call. = FALSE)
  }
  row <- ready[ready$name == pick, , drop = FALSE][1, ]
  zst <- grepl("\\.zst$", pick)
  need <- row$size * (if (zst) 8 else 1) / 1024^3
  free <- io$free_gib(workdir)
  if (free - need < min_free_gib) {
    stop(sprintf(paste("stopping before %s@%s: %.1f GiB free, it needs about %.1f GiB",
                       "and the floor is %s GiB; free some space and re-run"),
                 family$repo, tag, free, need, min_free_gib), call. = FALSE)
  }
  digest <- sub("^sha256:", "", tolower(row$digest %||% NA_character_))
  dir <- file.path(workdir, "snapshot")
  problem <- NA_character_
  for (attempt in 1:3) {
    unlink(dir, recursive = TRUE)
    dir.create(dir, recursive = TRUE, showWarnings = FALSE)
    path <- file.path(dir, pick)
    got_sha <- NA_character_
    problem <- if (!isTRUE(io$download(family$repo, tag, pick, dir))) "the download failed"
      else if (!file.exists(path)) "no file arrived"
      else if (file.size(path) != row$size) sprintf("%.0f bytes, the release says %.0f",
                                                    file.size(path), row$size)
      else NA_character_
    if (is.na(problem)) {
      got_sha <- history_file_sha256(path)
      if (!is.na(digest) && nzchar(digest) && !identical(got_sha, digest)) {
        problem <- "its sha256 differs from the release's digest"
      }
    }
    if (is.na(problem) && zst) {
      db <- sub("\\.zst$", "", path)
      if (!isTRUE(io$unzstd(path, db))) problem <- "it did not decompress"
      unlink(path)
      path <- db
    }
    if (is.na(problem)) {
      return(list(path = path, asset = pick, bytes = row$size, sha256 = got_sha))
    }
    if (attempt < 3L) io$sleep(HISTORY_DOWNLOAD_WAITS[attempt])
  }
  unlink(dir, recursive = TRUE)
  stop(sprintf("could not read %s from %s@%s after 3 attempts (%s); re-run, or record it with --give-up=%s:%s",
               pick, family$repo, tag, problem, family$name, tag), call. = FALSE)
}

attach_snapshot <- function(con, path) {
  full <- normalizePath(path, mustWork = TRUE)
  if (!grepl("^[A-Za-z0-9/._-]+$", full)) {
    stop("snapshot path has characters a file: URI would misread: ", full, call. = FALSE)
  }
  DBI::dbExecute(con, "ATTACH DATABASE ? AS snap",
                 params = list(paste0("file:", full, "?mode=ro&immutable=1")))
  invisible(NULL)
}

detach_snapshot <- function(con) {
  attached <- DBI::dbGetQuery(con, "PRAGMA database_list")$name
  if ("snap" %in% attached) DBI::dbExecute(con, "DETACH DATABASE snap")
  invisible(NULL)
}

history_setting <- function(con, key) {
  v <- DBI::dbGetQuery(con, "SELECT value FROM history_settings WHERE key = ?",
                       params = list(key))$value
  if (length(v) == 0L) NA_character_ else v
}

record_observation <- function(con, family, tag, series, o) {
  num <- function(x) if (is.null(x)) NA_integer_ else as.integer(x)
  chr <- function(x) if (is.null(x) || length(x) == 0L) NA_character_ else as.character(x)
  DBI::dbExecute(con,
    "INSERT OR REPLACE INTO history_series_observations
       (family, tag, series, rows_read, rows_kept, outcome, source_as_of, columns,
        fingerprint, filled, extended, closed, opened)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    params = list(family, tag, series, num(o$rows_read), num(o$rows_kept), o$outcome,
                  chr(o$source_as_of), chr(o$columns), chr(o$fingerprint), num(o$filled),
                  num(o$extended), num(o$closed), num(o$opened)))
}

record_snapshot <- function(con, family, row, outcome, now, got = NULL, note = NA_character_) {
  DBI::dbExecute(con,
    "INSERT OR REPLACE INTO history_snapshots
       (family, tag, snapshot_at, snapshot_on, asset, bytes, sha256, outcome, note, processed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    params = list(family, row$tag, row$published_at, row$snapshot_on,
                  got$asset %||% NA_character_, got$bytes %||% NA_real_,
                  got$sha256 %||% NA_character_, outcome, note, now))
}

newest_folded_tag <- function(con, family) {
  tag <- DBI::dbGetQuery(con,
    "SELECT tag FROM history_snapshots WHERE family = ? AND outcome = 'processed'
      ORDER BY snapshot_on DESC LIMIT 1", params = list(family))$tag
  if (length(tag) == 0L) NA_character_ else tag
}

# Whether the attached cran-metadata release is past the flavor handover.
# cran-metadata carries its own per-flavor tables from the run that seeded
# them, so such a release waits for the handover, and the first release after
# a handover must carry them.
check_flavor_handover <- function(con, tag) {
  handover <- history_setting(con, "flavor_handover_tag")
  carries <- history_table_exists(con, "cran_check_flavor_status_history", "snap")
  newest <- newest_folded_tag(con, "cran-metadata")
  if (is.na(handover) && carries) {
    stop(sprintf(paste("cran-metadata %s carries its own per-flavor status history, so it seeded",
                       "from %s; once its release notes say so, run again with --flavor-handover=%s"),
                 tag, newest, newest), call. = FALSE)
  }
  if (!is.na(handover) && !carries && identical(newest, handover)) {
    stop(sprintf(paste("cran-metadata %s has no per-flavor status history, so nothing seeded since",
                       "the handover at %s; if no run seeded from it, run again with",
                       "--withdraw-flavor-handover=%s"), tag, handover, handover), call. = FALSE)
  }
  !is.na(handover)
}

# Folds every series of the family from one snapshot, plus its ledger rows
# and any same-day releases it supersedes, in one transaction.
process_snapshot <- function(con, family, row, got, series_list, now, siblings = NULL) {
  attach_snapshot(con, got$path)
  on.exit(detach_snapshot(con), add = TRUE)
  mine <- Filter(function(s) identical(s$family, family$name), series_list)
  handed_over <- identical(family$name, "cran-metadata") && check_flavor_handover(con, row$tag)
  DBI::dbBegin(con)
  tryCatch({
    obs <- list()
    if (any(vapply(mine, function(s) identical(s$kind, "check_flavor"), logical(1)))) {
      obs <- apply_check_series(con, family, row$snapshot_on, mine, handed_over)
    }
    for (s in mine) {
      if (!is.null(obs[[s$name]])) next
      prior <- last_applied(con, family$name, s$name)
      obs[[s$name]] <- apply_episode_series(con, s, row$snapshot_on, prior)
    }
    for (name in names(obs)) record_observation(con, family$name, row$tag, name, obs[[name]])
    record_snapshot(con, family$name, row, "processed", now, got)
    if (!is.null(siblings) && nrow(siblings) > 0L) {
      for (i in seq_len(nrow(siblings))) {
        record_snapshot(con, family$name, siblings[i, ], "superseded", now,
                        note = siblings$note[i])
      }
    }
    DBI::dbCommit(con)
  }, error = function(e) {
    try(DBI::dbRollback(con), silent = TRUE)
    stop(sprintf("%s %s: %s", family$name, row$tag, conditionMessage(e)), call. = FALSE)
  })
  invisible(obs)
}

record_flavor_handover <- function(con, tag) {
  current <- history_setting(con, "flavor_handover_tag")
  if (!is.na(current)) {
    if (identical(current, tag)) return(invisible(tag))
    stop("the flavor status handover is already recorded at ", current, call. = FALSE)
  }
  last <- newest_folded_tag(con, "cran-metadata")
  if (!identical(last, tag)) {
    stop(sprintf("the handover tag must be the newest folded cran-metadata release (%s), not %s",
                 if (is.na(last)) "none" else last, tag), call. = FALSE)
  }
  DBI::dbExecute(con, "INSERT INTO history_settings (key, value) VALUES ('flavor_handover_tag', ?)",
                 params = list(tag))
  invisible(tag)
}

# Undoes a handover no seed followed, while nothing after it is folded. The
# row is kept under a dated key as the record of it.
withdraw_flavor_handover <- function(con, tag, now) {
  current <- history_setting(con, "flavor_handover_tag")
  if (!identical(current, tag)) {
    stop(sprintf("no flavor status handover is recorded at %s (recorded: %s)", tag,
                 if (is.na(current)) "none" else current), call. = FALSE)
  }
  if (!identical(newest_folded_tag(con, "cran-metadata"), tag)) {
    stop(sprintf(paste("cran-metadata releases after %s carried their own per-flavor status",
                       "history when folded, so the handover stands"), tag), call. = FALSE)
  }
  DBI::dbExecute(con, "UPDATE history_settings SET key = ? WHERE key = 'flavor_handover_tag'",
                 params = list(paste0("flavor_handover_withdrawn:", now)))
  invisible(tag)
}

record_note <- function(con, family, tag, note) {
  n <- DBI::dbExecute(con,
    "UPDATE history_snapshots SET note = CASE WHEN note IS NULL THEN ? ELSE note || '; ' || ? END
      WHERE family = ? AND tag = ? AND outcome = 'processed'",
    params = list(note, note, family, tag))
  if (n != 1L) stop(sprintf("no folded %s release %s to note", family, tag), call. = FALSE)
  invisible(n)
}

run_extraction <- function(con, workdir, io = default_history_io(),
                           families = history_families(), series_list = history_series(),
                           min_free_gib = 20, give_up = character(0)) {
  ensure_history_ledger(con)
  today <- io$today()
  for (family in families) {
    listing <- io$list_releases(family$repo)
    if (nrow(listing) >= HISTORY_LIST_LIMIT) {
      stop(sprintf("%s lists %d releases, the most one listing returns; raise HISTORY_LIST_LIMIT",
                   family$repo, nrow(listing)), call. = FALSE)
    }
    recorded <- DBI::dbGetQuery(con,
      "SELECT tag, snapshot_on, outcome FROM history_snapshots WHERE family = ?",
      params = list(family$name))
    plan <- plan_family_snapshots(listing, family, recorded, today)
    upfront <- plan[plan$action != "process" & is.na(plan$superseded_by), , drop = FALSE]
    if (nrow(upfront) > 0L) {
      DBI::dbWithTransaction(con, for (i in seq_len(nrow(upfront))) {
        record_snapshot(con, family$name, upfront[i, ], upfront$action[i], io$now(),
                        note = upfront$note[i])
      })
    }
    todo <- plan[plan$action == "process", , drop = FALSE]
    for (i in seq_len(nrow(todo))) {
      row <- todo[i, ]
      siblings <- plan[plan$action == "superseded" & !is.na(plan$superseded_by) &
                         plan$superseded_by == row$tag, , drop = FALSE]
      if (paste0(family$name, ":", row$tag) %in% give_up) {
        record_snapshot(con, family$name, row, "failed", io$now(),
                        note = "given up after its asset could not be read")
        next
      }
      got <- fetch_snapshot(io, family, row$tag, workdir, min_free_gib)
      process_snapshot(con, family, row, got, series_list, io$now(), siblings)
      unlink(dirname(got$path), recursive = TRUE)
      message(sprintf("%s %s folded (%d of %d)", family$name, row$tag, i, nrow(todo)))
    }
  }
  invisible(TRUE)
}

# Command line:
#   Rscript scripts/history/extract.R --workdir=DIR [--min-free-gib=20]
#     [--only=FAMILY] [--give-up=FAMILY:TAG] [--note=FAMILY:TAG=TEXT]
#     [--flavor-handover=TAG | --withdraw-flavor-handover=TAG]
history_args <- function(args) {
  arg_value <- function(flag) sub(paste0("^--", flag, "="), "", grep(paste0("^--", flag, "="), args, value = TRUE))
  workdir <- arg_value("workdir")
  if (length(workdir) != 1L) stop("give --workdir=DIR", call. = FALSE)
  min_free <- arg_value("min-free-gib")
  list(workdir = workdir, min_free_gib = if (length(min_free)) as.numeric(min_free) else 20,
       only = arg_value("only"), give_up = arg_value("give-up"), notes = arg_value("note"),
       handover = arg_value("flavor-handover"), withdraw = arg_value("withdraw-flavor-handover"))
}

history_main <- function(args = commandArgs(trailingOnly = TRUE), io = default_history_io()) {
  a <- history_args(args)
  families <- history_families()
  if (length(a$only)) {
    if (!all(a$only %in% names(families))) stop("--only takes cran-metadata or data", call. = FALSE)
    families <- families[a$only]
  }
  if (length(a$handover) && length(a$withdraw)) {
    stop("give --flavor-handover or --withdraw-flavor-handover, not both", call. = FALSE)
  }
  dir.create(a$workdir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(a$workdir, "history.db")
  if (!file.exists(path)) restore_prior_history(io, path)
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  ensure_history_ledger(con)
  if (length(a$withdraw) == 1L) withdraw_flavor_handover(con, a$withdraw, io$now())
  if (length(a$handover) == 1L) record_flavor_handover(con, a$handover)
  run_extraction(con, a$workdir, io, families, history_series(), a$min_free_gib, a$give_up)
  for (n in a$notes) {
    parts <- regmatches(n, regexec("^([^:]+):([^=]+)=(.+)$", n))[[1]]
    if (length(parts) != 4L) stop("give --note=FAMILY:TAG=TEXT", call. = FALSE)
    record_note(con, parts[2], parts[3], parts[4])
  }
  print(DBI::dbGetQuery(con,
    "SELECT family, outcome, COUNT(*) AS n, MIN(tag) AS first_tag, MAX(tag) AS last_tag
       FROM history_snapshots GROUP BY family, outcome ORDER BY family, outcome"))
  invisible(TRUE)
}

if (sys.nframe() == 0L) {
  here <- dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)))
  for (f in c("fold.R", "check_flavor_fold.R", "series.R", "assets.R")) source(file.path(here, f))
  history_main()
}
