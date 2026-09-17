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

payload <- "SQLite format 3 stand-in\n"
payload_bytes <- nchar(payload, type = "bytes")

# Writes a stand-in `gh` that answers the calls these functions make, then runs
# each line of `calls` in one shell, the way the workflow step runs all four.
#
# `releases` is gh's own order (newest commit date first), with drafts marked.
# `listings` says what each successive `gh release list` does, and runs out into
# "ok": "ok" lists the releases, anything else fails with a 502.
# `plan` says what each successive download does, and runs out into "ok":
#   ok       writes the asset
#   empty    writes a zero-byte file and exits 0
#   short    writes a few bytes of it and exits 0
#   partial  writes a few bytes of it and exits 1, as a cut stream does
#   anything else fails without writing
# Like real gh, a download refuses to write over a file that is already there.
# `declared` is the size `gh release view` reports, NULL for none at all.
#
# `gap` is a stretch of the stand-in clock, c(from, to) in seconds, during which
# the release does not have the asset, the way a `gh release upload --clobber`
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
                         declared = payload_bytes, gap = NULL, gap_state = "") {
  root <- withr::local_tempdir(.local_envir = parent.frame())
  bin <- file.path(root, "bin")
  state <- file.path(root, "state")
  work <- file.path(root, "work")
  dir.create(bin)
  dir.create(state)
  dir.create(file.path(work, "sources"), recursive = TRUE)

  writeLines(sprintf("%s\t%s", releases$tag, releases$kind),
             file.path(state, "releases"))
  writeLines(plan, file.path(state, "plan"))
  writeLines(listings, file.path(state, "listings"))
  writeLines(if (is.null(declared)) character() else format(declared),
             file.path(state, "declared"))
  writeLines(if (is.null(gap)) character() else format(gap, scientific = FALSE),
             file.path(state, "gap"))
  writeLines(gap_state, file.path(state, "gap_state"))
  file.create(file.path(state, c("sleeps", "downloads", "lists")))

  writeLines(c(
    "#!/usr/bin/env bash",
    'state="$FAKE_GH_STATE"',
    'sub="$1 $2"; shift 2',
    'tag="" exclude="" limit=30 dir="" pattern="" jq=""',
    'while [ $# -gt 0 ]; do',
    '  case "$1" in',
    '    --exclude-drafts) exclude=1 ;;',
    '    --limit|-L) limit="$2"; shift ;;',
    '    --pattern|-p) pattern="$2"; shift ;;',
    '    --dir|-D) dir="$2"; shift ;;',
    '    --jq|-q) jq="$2"; shift ;;',
    '    --repo|-R|--json) shift ;;',
    '    -*) ;;',
    '    *) tag="$1" ;;',
    '  esac',
    '  shift',
    'done',
    'now=$(awk \'{ s += $1 } END { print s + 0 }\' "$state/sleeps")',
    'in_gap=$(awk -v now="$now" \'NR == 1 { f = $1 } NR == 2 { t = $1 }',
    '         END { if (NR == 2 && now >= f && now < t) print 1 }\' "$state/gap")',
    'case "$sub" in',
    '  "release list")',
    '    echo list >> "$state/lists"',
    '    n=$(wc -l < "$state/lists")',
    '    step=$(sed -n "$((n))p" "$state/listings")',
    '    if [ -n "$step" ] && [ "$step" != ok ]; then',
    '      echo "HTTP 502: Bad Gateway (https://api.github.com/graphql)" >&2; exit 1',
    '    fi',
    '    while IFS="$(printf \'\\t\')" read -r name kind; do',
    '      if [ -n "$exclude" ] && [ "$kind" = draft ]; then continue; fi',
    '      printf "%s\\n" "$name"',
    '    done < "$state/releases" | head -n "$limit" ;;',
    '  "release download")',
    '    echo "$tag" >> "$state/downloads"',
    '    n=$(wc -l < "$state/downloads")',
    '    step=$(sed -n "$((n))p" "$state/plan")',
    '    if [ -n "$in_gap" ]; then',
    '      echo "no assets match the file pattern" >&2; exit 1',
    '    fi',
    '    if [ -e "$dir/$pattern" ]; then',
    '      echo "$dir/$pattern already exists (use \\`--clobber\\` to overwrite file or \\`--skip-existing\\` to skip file)" >&2',
    '      exit 1',
    '    fi',
    '    case "$step" in',
    sprintf('      ""|ok) printf "%%s" %s > "$dir/$pattern" ;;', shQuote(payload)),
    '      empty) : > "$dir/$pattern" ;;',
    '      short) printf "%s" SQLite > "$dir/$pattern" ;;',
    '      partial) printf "%s" SQLite > "$dir/$pattern"',
    '               echo "stream error: unexpected EOF" >&2; exit 1 ;;',
    '      *) echo "HTTP 500: Internal Server Error" >&2; exit 1 ;;',
    '    esac ;;',
    '  "release view")',
    '    case "$jq" in',
    '      *.state*) if [ -n "$in_gap" ]; then cat "$state/gap_state"; else echo uploaded; fi ;;',
    '      *.size*) if [ -z "$in_gap" ]; then cat "$state/declared"; fi ;;',
    '    esac ;;',
    'esac'
  ), file.path(bin, "gh"))
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
  list(
    output = out,
    log = paste(out, collapse = "\n"),
    finished = any(out == "dl_dated returned"),
    downloads = read_state("downloads"),
    lists = read_state("lists"),
    sleeps = as.numeric(read_state("sleeps")),
    sources = stats::setNames(file.size(landed), basename(landed)),
    target_bytes = {
      target <- file.path(work, "sources", "cran-code-metrics.db")
      if (file.exists(target)) file.size(target) else NA_real_
    }
  )
}

published <- function(tags) data.frame(tag = tags, kind = "published")

test_that("a draft that sorts above the published releases is never resolved", {
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
  # Ties on commit date leave gh's order unrelated to the tag's own date.
  older <- sprintf("metrics-2026-08-%02d", 1:30)
  releases <- published(c(older, "metrics-2026-09-12"))
  res <- run_dl_dated(releases, plan = "ok")
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, "metrics-2026-09-12")
})

test_that("a repo still on the per-series tags is read from those", {
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
  releases <- published(c("current", "code-2026-01-02", "data-2026-01-02"))
  res <- run_dl_dated(releases, plan = "ok")
  expect_true(res$finished, info = res$log)
  expect_equal(res$downloads, "code-2026-01-02")
  expect_equal(res$target_bytes, payload_bytes)
})

test_that("a repo with no dated release is skipped without failing the step", {
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
  res <- run_dl_dated(published("current"))
  expect_true(res$finished, info = res$log)
  expect_equal(length(res$downloads), 0L)
  expect_match(res$log, "no metrics-*/code-* release", fixed = TRUE)
})

test_that("a release listing that fails is retried rather than read as no release", {
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
  res <- run_dl_dated(published("metrics-2026-09-12"), plan = "short")
  expect_false(res$finished, info = res$log)
  expect_match(res$log, "Torn download", fixed = TRUE)
})

test_that("one database failing does not carry into the next call", {
  skip_if(!nzchar(Sys.which("bash")), "bash is not available")
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
