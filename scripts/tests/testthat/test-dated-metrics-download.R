# The code and data metrics repos publish a dated metrics-* release rather
# than a rolling "current" tag, so the merge has to list their releases to find
# the newest one and then download from it. Both halves are shell functions
# inside merge.yml, which is why these tests read them out of the workflow and
# run them against a stand-in `gh` on PATH. Nothing here touches the network or
# the real sources/ directory, which matters because this suite also runs
# inside the merge job itself, after the real downloads.

workflow_lines <- function() {
  readLines(file.path(getwd(), "..", "..", "..",
                      ".github", "workflows", "merge.yml"), warn = FALSE)
}

# Shell commands with comment lines dropped and backslash continuations
# joined, so a flag on the second line of a command still counts as part of it.
workflow_commands <- function(lines) {
  code <- lines[!grepl("^\\s*#", lines)]
  joined <- gsub("\\\\\n\\s*", " ", paste(code, collapse = "\n"))
  strsplit(joined, "\n", fixed = TRUE)[[1]]
}

# The text of one shell function, from its `name() {` line to the closing brace
# at the same indentation.
workflow_function <- function(lines, name) {
  start <- grep(sprintf("^\\s*%s\\(\\) \\{", name), lines)
  if (length(start) != 1L) stop("expected exactly one definition of ", name, "()")
  indent <- sub("^(\\s*).*$", "\\1", lines[start])
  end <- start - 1L + which(lines[start:length(lines)] == paste0(indent, "}"))[1]
  lines[start:end]
}

test_that("every release listing skips drafts and reads past gh's default page", {
  # `gh release list` includes drafts unless told otherwise. On 2026-09-13
  # cran-code-metrics was left holding a draft metrics-2026-09-13 with no
  # databases in it, after GitHub returned 500s partway through creating the
  # release, and that tag sorts above every published one. This workflow only
  # escaped because its token cannot see another repository's drafts. A token
  # that can would resolve the draft, fail to download from it and publish
  # observatory.db without the code metrics tables.
  #
  # The limit is the same kind of trap. gh stops at 30 releases by default and
  # orders them by the tagged commit's date, which many metrics releases share
  # (eighteen carried 2026-08-26T17:50:00Z on 2026-09-15), so the newest tag
  # can sit past row 30.
  cmds <- grep("gh release list", workflow_commands(workflow_lines()),
               fixed = TRUE, value = TRUE)
  expect_gte(length(cmds), 1L)
  for (cmd in cmds) {
    expect_true(grepl("--exclude-drafts", cmd, fixed = TRUE), info = cmd)
    limit <- regmatches(cmd, regexpr("(--limit|-L)[ =]+[0-9]+", cmd))
    expect_equal(length(limit), 1L, info = cmd)
    expect_true(isTRUE(as.numeric(sub("^\\D+", "", limit)) >= 1000), info = cmd)
  }
})

test_that("the dated metrics download sits inside a retry loop", {
  body <- paste(workflow_function(workflow_lines(), "dl_dated"), collapse = "\n")
  loop <- regexpr("for attempt in 1 2 3", body, fixed = TRUE)
  expect_gt(loop, 0L)
  expect_gt(regexpr("gh release download", body, fixed = TRUE), loop)
  expect_true(grepl("sleep", body, fixed = TRUE))
})

# ---------------------------------------------------------------------------
# The same functions, run
# ---------------------------------------------------------------------------

payload <- strrep("SQLite format 3 stand-in\n", 400)
payload_bytes <- nchar(payload, type = "bytes")

# The stand-in `gh` reads its answers through the real jq, and the compressed
# fixtures are made and read with the real zstd, both of which the runner has.
needs_tools <- function(...) {
  for (tool in c("bash", "jq", ...)) {
    skip_if(!nzchar(Sys.which(tool)), paste(tool, "is not available"))
  }
}

# The same bytes with the last one flipped. On a .zst that is the frame's
# checksum, so the file keeps its size and fails only when it is decoded.
write_corrupt <- function(path) {
  bytes <- readBin(path, "raw", file.size(path))
  n <- length(bytes)
  bytes[n] <- as.raw(bitwXor(as.integer(bytes[n]), 255L))
  writeBin(bytes, paste0(path, ".corrupt"))
}

zstd_file <- function(input, output) {
  status <- system2("zstd", c("-q", "-3", "-f", shQuote(input), "-o", shQuote(output)))
  if (!identical(status, 0L)) stop("zstd could not compress ", input)
  output
}

# Writes a stand-in `gh` that answers the calls these functions make, then runs
# each line of `calls` in one shell, the way the workflow step runs all four.
#
# `releases` is gh's own order (newest commit date first), with drafts marked.
# `listings` says what each successive `gh release list` does, and runs out into
# "ok": "ok" lists the releases, anything else fails with a 502. `views` does
# the same for `gh release view`.
# `plan` says what each successive database download does, and runs out into
# "ok":
#   ok       writes the asset
#   empty    writes a zero-byte file and exits 0
#   short    writes the first half of it and exits 0
#   partial  writes the first half of it and exits 1, as a cut stream does
#   corrupt  writes it with its last byte flipped and exits 0
#   anything else fails without writing
# Like real gh, a download refuses to write over a file that is already there.
# `declared` is the size `gh release view` reports for a database, NA for its
# real size and NULL for none at all.
#
# `forms` is what every release carries of each database: "plain" (<name>),
# "zst" (<name>.zst) or "both". `zst` is the file published as <name>.zst, by
# default the payload compressed at level 3. `switch_at` publishes the plain
# form until that point of the stand-in clock and the .zst from then on.
# `manifest` holds fields for <series>-manifest.json on top of the true
# db_filename and db_bytes, NULL for a release with no manifest. Until
# `manifest_until` the manifest describes an older database instead.
# `manifest_plan` does for each successive manifest download what `plan` does
# for the databases.
#
# `gap` is a stretch of the stand-in clock, c(from, to) in seconds, during which
# the release does not have the database, the way a `gh release upload --clobber`
# leaves it between deleting the old file and finishing the new one. While it
# lasts a download finds nothing to match and the asset's state reads as
# `gap_state`, "" being not listed at all. Nothing is known about which of those
# GitHub shows during an upload, so both are worth running.
#
# `sleep` is stubbed and moves that clock forward rather than waiting. `stat`
# is stubbed too, because `stat --format` is GNU-only and a macOS checkout would
# otherwise fail the size check for reasons unrelated to what is being tested.
run_dl_dated <- function(releases, plan = character(), listings = character(),
                         calls = "dl_dated cran-code-metrics code cran-code-metrics.db",
                         declared = NA, gap = NULL, gap_state = "",
                         forms = "plain", zst = NULL, switch_at = NULL,
                         manifest = list(), manifest_until = NULL,
                         manifest_plan = character(), views = character()) {
  root <- withr::local_tempdir(.local_envir = parent.frame())
  bin <- file.path(root, "bin")
  state <- file.path(root, "state")
  assets <- file.path(root, "assets")
  work <- file.path(root, "work")
  dir.create(bin)
  dir.create(state)
  dir.create(assets)
  dir.create(file.path(work, "sources"), recursive = TRUE)

  never <- 1e12
  plain_file <- file.path(assets, "payload.db")
  writeBin(charToRaw(payload), plain_file)
  write_corrupt(plain_file)
  with_zst <- forms %in% c("zst", "both") || !is.null(switch_at)
  zst_file <- file.path(assets, "payload.db.zst")
  if (with_zst) {
    if (is.null(zst)) zstd_file(plain_file, zst_file) else file.copy(zst, zst_file)
    write_corrupt(zst_file)
  }

  rows <- list()
  add <- function(name, file, st, from, to) {
    rows[[length(rows) + 1L]] <<- c(name, file, st,
                                    format(c(from, to), scientific = FALSE))
  }
  # A database is listed from `from` to `to`, less the gap.
  add_db <- function(name, file, from = 0, to = never) {
    if (is.null(gap) || max(from, gap[1]) >= min(to, gap[2])) {
      return(add(name, file, "uploaded", from, to))
    }
    g1 <- max(from, gap[1])
    g2 <- min(to, gap[2])
    if (from < g1) add(name, file, "uploaded", from, g1)
    if (nzchar(gap_state)) add(name, file, gap_state, g1, g2)
    if (g2 < to) add(name, file, "uploaded", g2, to)
  }
  targets <- c(code = "cran-code-metrics.db", data = "cran-data-metrics.db")
  for (series in names(targets)) {
    target <- targets[[series]]
    if (!is.null(switch_at)) {
      add_db(target, plain_file, 0, switch_at)
      add_db(paste0(target, ".zst"), zst_file, switch_at, never)
    } else {
      if (forms %in% c("plain", "both")) add_db(target, plain_file)
      if (forms %in% c("zst", "both")) add_db(paste0(target, ".zst"), zst_file)
    }
    if (!is.null(manifest)) {
      fields <- utils::modifyList(list(db_filename = target, db_bytes = payload_bytes),
                                  manifest)
      now_file <- file.path(assets, paste0(series, "-manifest.json"))
      jsonlite::write_json(fields, now_file, auto_unbox = TRUE, pretty = TRUE, digits = NA)
      if (is.null(manifest_until)) {
        add(basename(now_file), now_file, "uploaded", 0, never)
      } else {
        old_file <- paste0(now_file, ".old")
        jsonlite::write_json(list(db_filename = target, db_bytes = payload_bytes - 1L),
                             old_file, auto_unbox = TRUE, pretty = TRUE, digits = NA)
        add(basename(now_file), old_file, "uploaded", 0, manifest_until)
        add(basename(now_file), now_file, "uploaded", manifest_until, never)
      }
    }
  }
  writeLines(vapply(rows, paste, character(1), collapse = "\t"),
             file.path(state, "assets"))

  writeLines(sprintf("%s\t%s", releases$tag, releases$kind),
             file.path(state, "releases"))
  writeLines(plan, file.path(state, "plan"))
  writeLines(listings, file.path(state, "listings"))
  writeLines(views, file.path(state, "view_plan"))
  writeLines(manifest_plan, file.path(state, "manifest_plan"))
  writeLines(if (is.null(declared)) "none" else if (is.na(declared)) character()
             else format(declared, scientific = FALSE),
             file.path(state, "declared"))
  file.create(file.path(state, c("sleeps", "downloads", "fetched", "lists", "views",
                                 "manifest_downloads")))

  writeLines(r"---(#!/usr/bin/env bash
state="$FAKE_GH_STATE"
tab=$(printf '\t')
sub="$1 $2"; shift 2
tag="" exclude="" limit=30 dir="" pattern="" jq=""
while [ $# -gt 0 ]; do
  case "$1" in
    --exclude-drafts) exclude=1 ;;
    --limit|-L) limit="$2"; shift ;;
    --pattern|-p) pattern="$2"; shift ;;
    --dir|-D) dir="$2"; shift ;;
    --jq|-q) jq="$2"; shift ;;
    --repo|-R|--json) shift ;;
    -*) ;;
    *) tag="$1" ;;
  esac
  shift
done
now=$(awk '{ s += $1 } END { print s + 0 }' "$state/sleeps")
declared=$(cat "$state/declared")
# The assets listed at this point of the stand-in clock: name, file, state, size.
active() {
  while IFS="$tab" read -r name file st from to; do
    awk -v n="$now" -v f="$from" -v t="$to" 'BEGIN { exit !(n >= f && n < t) }' || continue
    size=$(wc -c < "$file" | tr -d ' ')
    case "$name:$declared" in
      *.db:|*.db.zst:|*:none) ;;
      *.db:*|*.db.zst:*) size="$declared" ;;
    esac
    printf '%s\t%s\t%s\t%s\n' "$name" "$file" "$st" "$size"
  done < "$state/assets"
}
# The next line of a plan, counting calls in a log.
next_step() {  # $1=log  $2=plan
  echo x >> "$state/$1"
  n=$(wc -l < "$state/$1")
  sed -n "$((n))p" "$state/$2"
}
case "$sub" in
  "release list")
    step=$(next_step lists listings)
    if [ -n "$step" ] && [ "$step" != ok ]; then
      echo "HTTP 502: Bad Gateway (https://api.github.com/graphql)" >&2; exit 1
    fi
    while IFS="$tab" read -r name kind; do
      if [ -n "$exclude" ] && [ "$kind" = draft ]; then continue; fi
      printf "%s\n" "$name"
    done < "$state/releases" | head -n "$limit" ;;
  "release view")
    step=$(next_step views view_plan)
    if [ -n "$step" ] && [ "$step" != ok ]; then
      echo "HTTP 502: Bad Gateway (https://api.github.com/graphql)" >&2; exit 1
    fi
    case "$jq:$declared" in
      *.size*:none) exit 0 ;;
    esac
    active | jq -Rn '{assets: [inputs | split("\t") | {name: .[0], state: .[2], size: (.[3] | tonumber)}]}' |
      jq -r "$jq" ;;
  "release download")
    echo "$pattern" >> "$state/fetched"
    step=ok
    case "$pattern" in
      *.db|*.db.zst)
        echo "$tag" >> "$state/downloads"
        n=$(wc -l < "$state/downloads")
        step=$(sed -n "$((n))p" "$state/plan") ;;
      *-manifest.json)
        step=$(next_step manifest_downloads manifest_plan) ;;
    esac
    file=$(active | awk -F "$tab" -v p="$pattern" '$1 == p && $3 == "uploaded" { print $2; exit }')
    if [ -z "$file" ]; then
      echo "no assets match the file pattern" >&2; exit 1
    fi
    if [ -e "$dir/$pattern" ]; then
      echo "$dir/$pattern already exists (use \`--clobber\` to overwrite file or \`--skip-existing\` to skip file)" >&2
      exit 1
    fi
    half=$(( $(wc -c < "$file") / 2 ))
    case "$step" in
      ""|ok) cp "$file" "$dir/$pattern" ;;
      empty) : > "$dir/$pattern" ;;
      short) head -c "$half" "$file" > "$dir/$pattern" ;;
      partial) head -c "$half" "$file" > "$dir/$pattern"
               echo "stream error: unexpected EOF" >&2; exit 1 ;;
      corrupt) cp "$file.corrupt" "$dir/$pattern" ;;
      *) echo "HTTP 500: Internal Server Error" >&2; exit 1 ;;
    esac ;;
esac)---", file.path(bin, "gh"))
  # A wait that never ends would hang this suite, and the merge job that runs
  # it, so the stand-in clock refuses to go past a day and the step fails.
  writeLines(c("#!/bin/sh", 'echo "$1" >> "$FAKE_GH_STATE/sleeps"',
               'now=$(awk \'{ s += $1 } END { print s + 0 }\' "$FAKE_GH_STATE/sleeps")',
               'if [ "$now" -gt 86400 ]; then',
               '  echo "the stand-in clock ran past a day" >&2; exit 1',
               'fi'),
             file.path(bin, "sleep"))
  writeLines(c("#!/bin/sh", 'for last; do :; done', 'wc -c < "$last" | tr -d " "'),
             file.path(bin, "stat"))
  Sys.chmod(file.path(bin, c("gh", "sleep", "stat")), "0755")

  lines <- workflow_lines()
  script <- file.path(root, "run.sh")
  writeLines(c(
    sprintf("cd %s", shQuote(work)),
    workflow_function(lines, "verify_size"),
    workflow_function(lines, "latest_tag"),
    workflow_function(lines, "dated_asset"),
    workflow_function(lines, "expand_dated"),
    workflow_function(lines, "dl_dated"),
    calls,
    "echo 'dl_dated returned'"
  ), script)

  withr::local_envvar(
    PATH = paste(bin, Sys.getenv("PATH"), sep = .Platform$path.sep),
    FAKE_GH_STATE = state,
    TMPDIR = root
  )
  # `bash -e`, as GitHub runs a step, so a command that fails outside a
  # condition ends the step here exactly as it would there.
  out <- suppressWarnings(system2("bash", c("-e", shQuote(script)),
                                  stdout = TRUE, stderr = TRUE))
  read_state <- function(f) {
    p <- file.path(state, f)
    if (file.exists(p)) readLines(p) else character(0)
  }
  landed <- list.files(file.path(work, "sources"), full.names = TRUE)
  target <- file.path(work, "sources", "cran-code-metrics.db")
  list(
    output = out,
    log = paste(out, collapse = "\n"),
    finished = any(out == "dl_dated returned"),
    downloads = read_state("downloads"),
    fetched = read_state("fetched"),
    lists = read_state("lists"),
    sleeps = as.numeric(read_state("sleeps")),
    sources = stats::setNames(file.size(landed), basename(landed)),
    target_bytes = if (file.exists(target)) file.size(target) else NA_real_,
    target_whole = file.exists(target) &&
      identical(readBin(target, "raw", file.size(target) + 1), charToRaw(payload))
  )
}

published <- function(tags) data.frame(tag = tags, kind = "published")

test_that("a draft that sorts above the published releases is never resolved", {
  needs_tools()
  releases <- rbind(
    data.frame(tag = "metrics-2026-09-13", kind = "draft"),
    published(c("metrics-2026-09-12", "metrics-2026-09-11", "current"))
  )
  res <- run_dl_dated(releases, plan = "ok")
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, "metrics-2026-09-12")
  expect_equal(res$target_bytes, payload_bytes)
})

test_that("the newest tag is found even when gh's order puts it past row 30", {
  needs_tools()
  # Ties on commit date leave gh's order unrelated to the tag's own date.
  older <- sprintf("metrics-2026-08-%02d", 1:30)
  releases <- published(c(older, "metrics-2026-09-12"))
  res <- run_dl_dated(releases, plan = "ok")
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, "metrics-2026-09-12")
})

test_that("a repo still on the per-series tags is read from those", {
  needs_tools()
  releases <- published(c("current", "code-2026-01-02", "data-2026-01-02"))
  res <- run_dl_dated(releases, plan = "ok")
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, "code-2026-01-02")
  expect_equal(res$target_bytes, payload_bytes)
})

test_that("a repo with no dated release is skipped without failing the step", {
  needs_tools()
  res <- run_dl_dated(published("current"))
  expect_true(res$finished, info = res$log)
  expect_equal(length(res$downloads), 0L)
  expect_match(res$log, "no metrics-*/code-* release", fixed = TRUE)
})

test_that("a release listing that fails is retried rather than read as no release", {
  needs_tools()
  # `gh release list` can make more than one request, and any of them can come
  # back as a 5xx the way GitHub's API did on 2026-09-13. Piped through grep,
  # sort and head, a failed listing looked exactly like a repo with no dated
  # release: nothing was retried, no download was attempted, and the log
  # blamed a release that was there all along.
  res <- run_dl_dated(published("metrics-2026-09-12"), listings = c("fail", "ok"))
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, "metrics-2026-09-12")
  expect_equal(res$target_bytes, payload_bytes)
  expect_match(res$log, "HTTP 502", fixed = TRUE)
  expect_no_match(res$log, "no metrics-*/code-* release", fixed = TRUE)
})

test_that("releases that cannot be listed at all are reported as that", {
  needs_tools()
  # Non-fatal, like a download that fails every attempt, and bounded by the
  # same per-database wait, so the gate reports the database missing and the
  # log says why.
  res <- run_dl_dated(published("metrics-2026-09-12"), listings = rep("fail", 10))
  expect_true(res$finished, info = res$log)
  expect_equal(length(res$lists), 3L)
  expect_equal(length(res$downloads), 0L)
  expect_true(is.na(res$target_bytes))
  expect_match(res$log,
               "could not list the releases of r-observatory/cran-code-metrics after 3 attempts",
               fixed = TRUE)
  expect_no_match(res$log, "no metrics-*/code-* release", fixed = TRUE)
  expect_lte(sum(res$sleeps), 240)
})

test_that("a failed, partial or empty download is retried until a real file arrives", {
  needs_tools()
  # A cut stream leaves part of the file behind, and gh refuses to download
  # over a file that is already there, so each retry needs the directory
  # cleared first or it can never succeed.
  releases <- published(c("metrics-2026-09-12", "metrics-2026-09-11"))
  res <- run_dl_dated(releases, plan = c("partial", "empty", "ok"))
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, rep("metrics-2026-09-12", 3L))
  expect_equal(res$target_bytes, payload_bytes)
  # gh's own reason stays in the log next to the warning. A clobber gap, a
  # 5xx and an empty file need different responses, and a warning that only
  # says the download failed cannot tell them apart.
  expect_match(res$log, "unexpected EOF", fixed = TRUE)
  expect_match(res$log, "empty", fixed = TRUE)
})

test_that("a database being re-uploaded is waited for rather than dropped", {
  needs_tools()
  # cran-code-metrics re-publishes into the same day's release with
  # `gh release upload --clobber`, which deletes its 1.9 GB database before
  # uploading the replacement. On 2026-09-12 that landed at 18:21, after the
  # merge's 18:00 start, and across the 25 re-uploads from 2026-08-19 to then
  # the database took 34 to 89 seconds to come back. Any fixed schedule of
  # waits is a guess at that number that the next slow upload can outlast.
  releases <- published("metrics-2026-09-12")
  for (shape in c("", "starter")) {
    res <- run_dl_dated(releases, gap = c(0, 150), gap_state = shape)
    expect_true(res$finished, info = res$log)
    expect_equal(res$target_bytes, payload_bytes, info = shape)
    # Waited out the whole upload, and went again soon after it finished
    # rather than sitting out the rest of some fixed interval.
    expect_gte(sum(res$sleeps), 150)
    expect_lt(sum(res$sleeps), 180)
  }
})

test_that("a database that never comes back is given up on inside a bounded wait", {
  needs_tools()
  # The release can lose its database for good, when a --clobber deletes it
  # and the replacement upload fails. The merge then goes ahead without it
  # and the gate reports it missing. The whole merge job has 30 minutes, and
  # a merge takes up to 12 of them, so the wait for each of the four dated
  # databases has to stay short enough for all four to fit around it. A
  # listing that needed retries first comes out of the same allowance rather
  # than adding to it.
  for (listings in list(character(), c("fail", "fail"))) {
    res <- run_dl_dated(published("metrics-2026-09-12"), gap = c(0, 1e9),
                        listings = listings)
    expect_true(res$finished, info = res$log)
    expect_equal(length(res$downloads), 3L)
    expect_true(is.na(res$target_bytes))
    expect_match(res$log, "after 3 attempts", fixed = TRUE)
    expect_lte(sum(res$sleeps), 210)
  }
})

test_that("an empty download of a non-empty asset still aborts the merge", {
  needs_tools()
  # Before these retries a zero-byte download went straight to verify_size,
  # which aborts the merge when the release declares a non-empty asset. That
  # abort publishes nothing, so the next hourly tick tries again once the
  # asset is whole. Publishing without the tables instead would leave them
  # off the site until a once-daily source publishes after that release,
  # since only that makes readiness merge again. So an empty file is retried,
  # and one that is still empty on the last attempt is judged exactly as
  # before.
  res <- run_dl_dated(published("metrics-2026-09-12"),
                      plan = c("empty", "empty", "empty"))
  expect_false(res$finished, info = res$log)
  expect_equal(length(res$downloads), 3L)
  expect_match(res$log, "Torn download", fixed = TRUE)
})

test_that("an empty download verify_size cannot judge is left out of sources/", {
  needs_tools()
  # verify_size lets a file through when the release declares zero bytes too,
  # or when its size cannot be read. A zero-byte file passes PRAGMA
  # integrity_check and counts as present to the freshness gate, so left in
  # place it would publish without the tables and without reddening the run.
  # Absent, the gate reports the source missing.
  for (declared in list(0, NULL)) {
    res <- run_dl_dated(published("metrics-2026-09-12"),
                        plan = c("empty", "empty", "empty"), declared = declared)
    expect_true(res$finished, info = res$log)
    expect_true(is.na(res$target_bytes), info = res$log)
  }
})

test_that("a download that disagrees with the declared size aborts the merge", {
  needs_tools()
  res <- run_dl_dated(published("metrics-2026-09-12"), plan = "short")
  expect_false(res$finished, info = res$log)
  expect_match(res$log, "Torn download", fixed = TRUE)
})

test_that("one database failing does not carry into the next call", {
  needs_tools()
  # The step calls dl_dated four times in one shell. Anything one call leaves
  # set can change what the next one does, and a failed database that reads as
  # downloaded would end the whole step on a file that is not there.
  res <- run_dl_dated(
    published("metrics-2026-09-12"),
    plan = c("ok", "fail", "fail", "fail"),
    calls = c("dl_dated cran-code-metrics code cran-code-metrics.db",
              "dl_dated cran-code-metrics data cran-data-metrics.db")
  )
  expect_true(res$finished, info = res$log)
  expect_equal(unname(res$sources["cran-code-metrics.db"]), payload_bytes)
  expect_false("cran-data-metrics.db" %in% names(res$sources))
  expect_match(res$log, "cran-data-metrics.db from cran-code-metrics@metrics-2026-09-12 after 3 attempts",
               fixed = TRUE)
})

# ---------------------------------------------------------------------------
# The compressed form
# ---------------------------------------------------------------------------

test_that("a release carrying only the .zst is expanded and held against its manifest", {
  needs_tools("zstd")
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst")
  expect_true(res$finished, info = res$log)
  expect_true(res$target_whole, info = res$log)
  expect_equal(names(res$sources), "cran-code-metrics.db")
  expect_equal(res$fetched, c("cran-code-metrics.db.zst", "code-manifest.json"))
  # The release's size is the compressed asset's, the manifest's the database's.
  expect_match(res$log, "size OK: cran-code-metrics.db.zst", fixed = TRUE)
  expect_match(res$log, sprintf("manifest OK: cran-code-metrics.db = %d bytes", payload_bytes),
               fixed = TRUE)
  expect_match(res$log, sprintf("-> sources/cran-code-metrics.db from metrics-2026-09-28 (%d bytes, from cran-code-metrics.db.zst)",
                                payload_bytes), fixed = TRUE)
})

test_that("a release carrying both forms is read from the .zst", {
  needs_tools("zstd")
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "both")
  expect_true(res$finished, info = res$log)
  expect_true(res$target_whole, info = res$log)
  expect_equal(res$fetched, c("cran-code-metrics.db.zst", "code-manifest.json"))
})

test_that("a release carrying only the plain database is read as it always was", {
  needs_tools()
  res <- run_dl_dated(published("metrics-2026-09-27"), forms = "plain")
  expect_true(res$finished, info = res$log)
  expect_true(res$target_whole, info = res$log)
  expect_equal(res$fetched, "cran-code-metrics.db")
  expect_match(res$log, sprintf("-> sources/cran-code-metrics.db from metrics-2026-09-27 (%d bytes)\n",
                                payload_bytes), fixed = TRUE)
  expect_match(res$log, sprintf("size OK: cran-code-metrics.db = %d bytes", payload_bytes),
               fixed = TRUE)
  expect_no_match(res$log, "manifest", fixed = TRUE)
})

test_that("a truncated .zst aborts the merge instead of merging part of it", {
  needs_tools("zstd")
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst", plan = "short")
  expect_false(res$finished, info = res$log)
  expect_match(res$log, "Torn download: cran-code-metrics.db.zst", fixed = TRUE)
  expect_equal(length(res$sources), 0L)

  # With no size to hold it against, zstd refuses the cut frame, and a file
  # that is still cut on the last attempt stops the step.
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst",
                      plan = rep("short", 3), declared = NULL)
  expect_false(res$finished, info = res$log)
  expect_equal(length(res$downloads), 3L)
  expect_match(res$log, "cran-code-metrics.db.zst from cran-code-metrics@metrics-2026-09-28 was not whole after 3 attempts",
               fixed = TRUE)
  expect_equal(length(res$sources), 0L)
})

test_that("a cut or unreadable .zst is retried and merged once a whole one arrives", {
  needs_tools("zstd")
  for (plan in list(c("partial", "ok"), c("short", "ok"), c("corrupt", "empty", "ok"))) {
    res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst", plan = plan,
                        declared = NULL)
    expect_true(res$finished, info = res$log)
    expect_true(res$target_whole, info = res$log)
    expect_equal(length(res$downloads), length(plan), info = res$log)
  }
})

test_that("a .zst of the right size that fails its checksum is never merged", {
  needs_tools("zstd")
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst",
                      plan = rep("corrupt", 3))
  expect_false(res$finished, info = res$log)
  expect_equal(length(res$downloads), 3L)
  expect_match(res$log, "did not decompress", fixed = TRUE)
  expect_match(res$log, "Aborting merge", fixed = TRUE)
  expect_equal(length(res$sources), 0L)
})

test_that("a .zst that expands to less than its manifest says is never merged", {
  needs_tools("zstd")
  # A stream cut between two frames decodes cleanly, so only the manifest's
  # size can tell that half the database is missing.
  root <- withr::local_tempdir()
  half <- file.path(root, "half.db")
  writeBin(charToRaw(substr(payload, 1, payload_bytes / 2)), half)
  first_frame <- zstd_file(half, file.path(root, "half.db.zst"))
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst", zst = first_frame)
  expect_false(res$finished, info = res$log)
  expect_equal(length(res$downloads), 3L)
  expect_match(res$log, sprintf("expands to %d bytes, but code-manifest.json says %d",
                                payload_bytes / 2, payload_bytes), fixed = TRUE)
  expect_equal(length(res$sources), 0L)
})

test_that("a manifest that trails its database is waited for", {
  needs_tools("zstd")
  # The pipeline replaces each database before the manifests, so a merge can
  # read the new database next to the old manifest for a few seconds.
  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst", manifest_until = 15)
  expect_true(res$finished, info = res$log)
  expect_true(res$target_whole, info = res$log)
  expect_equal(length(res$downloads), 2L)
  expect_match(res$log, "manifest OK", fixed = TRUE)
})

test_that("the manifest's sha256 is checked when it carries one", {
  needs_tools("zstd")
  skip_if(!exists("sha256sum", envir = asNamespace("tools")), "tools::sha256sum needs R 4.5")
  root <- withr::local_tempdir()
  f <- file.path(root, "payload.db")
  writeBin(charToRaw(payload), f)
  sha <- unname(tools::sha256sum(f))

  for (declared_sha in c(sha, toupper(sha), paste0("sha256:", sha))) {
    res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst",
                        manifest = list(db_sha256 = declared_sha))
    expect_true(res$finished, info = res$log)
    expect_true(res$target_whole, info = res$log)
    expect_match(res$log, paste("sha256", sha), fixed = TRUE)
  }

  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst",
                      manifest = list(db_sha256 = strrep("0", 64)))
  expect_false(res$finished, info = res$log)
  expect_equal(length(res$sources), 0L)
})

test_that("a .zst with no manifest to hold it against is merged on its own checks", {
  needs_tools("zstd")
  for (manifest in list(NULL, list(db_filename = "something-else.db"))) {
    res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst", manifest = manifest)
    expect_true(res$finished, info = res$log)
    expect_true(res$target_whole, info = res$log)
    expect_match(res$log, "relying on the zstd checksum", fixed = TRUE)
  }
})

test_that("a manifest the release lists but that cannot be read is retried, not skipped", {
  needs_tools("zstd")
  for (plan in list(c("fail", "ok"), c("short", "ok"))) {
    res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst", manifest_plan = plan)
    expect_true(res$finished, info = res$log)
    expect_true(res$target_whole, info = res$log)
    expect_equal(length(res$downloads), 2L, info = res$log)
    expect_match(res$log, "manifest OK", fixed = TRUE)
    expect_no_match(res$log, "relying on the zstd checksum", fixed = TRUE)
  }

  res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst",
                      manifest_plan = rep("fail", 3))
  expect_false(res$finished, info = res$log)
  expect_match(res$log, "could not read code-manifest.json", fixed = TRUE)
  expect_equal(length(res$sources), 0L)
})

test_that("a .zst being re-uploaded is waited for on that name", {
  needs_tools("zstd")
  for (shape in c("", "starter")) {
    res <- run_dl_dated(published("metrics-2026-09-28"), forms = "zst",
                        gap = c(0, 150), gap_state = shape)
    expect_true(res$finished, info = res$log)
    expect_true(res$target_whole, info = shape)
    expect_gte(sum(res$sleeps), 150)
    expect_lt(sum(res$sleeps), 180)
  }
})

test_that("a release that changes form during the merge is read in its new form", {
  needs_tools("zstd")
  # The plain database goes once the .zst is up, so the name is chosen again
  # on every attempt rather than once.
  res <- run_dl_dated(published("metrics-2026-09-28"), switch_at = 10, plan = "fail")
  expect_true(res$finished, info = res$log)
  expect_true(res$target_whole, info = res$log)
  expect_equal(res$fetched,
               c("cran-code-metrics.db", "cran-code-metrics.db.zst", "code-manifest.json"))
})

test_that("an asset listing that fails falls back to the plain name", {
  needs_tools()
  res <- run_dl_dated(published("metrics-2026-09-27"), views = "fail")
  expect_true(res$finished, info = res$log)
  expect_true(res$target_whole, info = res$log)
  expect_match(res$log, "could not list the assets of cran-code-metrics@metrics-2026-09-27",
               fixed = TRUE)
})

test_that("both databases of one release can be read from their .zst in one step", {
  needs_tools("zstd")
  res <- run_dl_dated(
    published("metrics-2026-09-28"), forms = "zst",
    calls = c("dl_dated cran-code-metrics code cran-code-metrics.db",
              "dl_dated cran-code-metrics data cran-data-metrics.db")
  )
  expect_true(res$finished, info = res$log)
  expect_equal(unname(res$sources[c("cran-code-metrics.db", "cran-data-metrics.db")]),
               rep(payload_bytes, 2))
  expect_match(res$log, "matches data-manifest.json", fixed = TRUE)
})
