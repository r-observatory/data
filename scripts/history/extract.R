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
