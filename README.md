# R Observatory Data

Combined CRAN Observatory database, merged daily from four pipeline repositories.

The `observatory.db` SQLite database is published as a GitHub Release every day at 08:00 UTC. It contains package metadata, download statistics, CRAN feed events, and incoming/outgoing queue snapshots — all in a single, queryable file.

## Data Access

### CLI

```bash
# Download the latest observatory.db
gh release download --repo r-observatory/data --pattern "observatory.db"
```

### R

```r
# Download and query
tmp <- tempfile(fileext = ".db")
download.file(
 "https://github.com/r-observatory/data/releases/latest/download/observatory.db",
  tmp, mode = "wb"
)
library(DBI)
con <- dbConnect(RSQLite::SQLite(), tmp)
dbListTables(con)
```

### Python

```python
import urllib.request, sqlite3

urllib.request.urlretrieve(
    "https://github.com/r-observatory/data/releases/latest/download/observatory.db",
    "observatory.db"
)
con = sqlite3.connect("observatory.db")
print(con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())
```

## Example Queries (R)

### Search packages

```r
# Full-text search using the FTS5 index
dbGetQuery(con, "
  SELECT name, title
  FROM packages_fts
  WHERE packages_fts MATCH 'bayesian regression'
  LIMIT 10
")
```

### Package details with downloads

```r
dbGetQuery(con, "
  SELECT p.name, p.title, d.total_30d, d.rank_30d
  FROM packages p
  LEFT JOIN downloads_summary d ON p.name = d.package
  ORDER BY d.total_30d DESC LIMIT 20
")
```

### Check package health

```r
dbGetQuery(con, "
  SELECT package, event_type, detected_at, version
  FROM package_versions
  WHERE package = 'dplyr'
  ORDER BY detected_at DESC
  LIMIT 5
")
```

### Recent feed events

```r
dbGetQuery(con, "
  SELECT package, version, event_type, detected_at
  FROM package_versions
  ORDER BY detected_at DESC LIMIT 20
")
```

## Data Sources

| Source | Repository | Schedule | Description |
|--------|-----------|----------|-------------|
| `feed.db` | [r-observatory/cran-feed](https://github.com/r-observatory/cran-feed) | Every 6 hours | Package additions, updates, removals, reverse dependencies |
| `metadata.db` | [r-observatory/cran-metadata](https://github.com/r-observatory/cran-metadata) | Daily at 06:00 UTC | Check results, authors, enrichment, check status history |
| `downloads.db` | [r-observatory/cran-downloads](https://github.com/r-observatory/cran-downloads) | Daily at 07:00 UTC | Download counts from CRAN logs |
| `autoobs-downloads-summary.db` | [r-observatory/autoobs-downloads](https://github.com/r-observatory/autoobs-downloads) | Daily at 04:00 UTC | Per-package download counts for openSUSE OBS autoCRAN (via MirrorCache) |
| `copr-downloads-summary.db` | [r-observatory/copr-downloads](https://github.com/r-observatory/copr-downloads) | Daily at 05:30 UTC | Per-chroot download counts for the Fedora COPR iucar/cran project |
| `conda-forge-downloads-summary.db` | [r-observatory/conda-forge-downloads](https://github.com/r-observatory/conda-forge-downloads) | Daily at 05:00 UTC | Per-package download counts for R packages on conda-forge |
| `bioconda-downloads-summary.db` | [r-observatory/bioconda-downloads](https://github.com/r-observatory/bioconda-downloads) | Daily at 05:15 UTC | Per-package download counts for R packages on bioconda |
| `queue.db` | [r-observatory/cran-queue](https://github.com/r-observatory/cran-queue) | Every 2 hours | CRAN incoming queue snapshots |

## Combined Schema

### From `feed.db` (cran-feed)

- **packages** — Current CRAN packages (name, version, title, description, maintainer, license, depends, imports, suggests, published, etc.)
- **package_versions**: Append-only version history (package, version, event_type, previous_version, removal_reason, detected_at). A removal's `removal_reason` is CRAN's own, from the `cran_archive_history` episode archived nearest the removal within 7 days; with no such episode it stays "no longer on CRAN".
- **reverse_dependencies** — Reverse dependency relationships (package, rev_package, type)

### From `metadata.db` (cran-metadata)

- **cran_check_results** — CRAN check results per package and flavor (package, flavor, status, tinstall, tcheck, ttotal)
- **cran_check_details** — Detailed check output (package, flavor, check_name, status, output)
- **cran_check_issues** — Packages with check issues (package, version, kind, href)
- **authors** — CRAN author database (package, given, family, email, role, orcid)
- **packages_enrichment** — URL and bug report links (name, url, bug_reports)
- **check_status_history** — Append-only status change log (package, status, flavor_summary, details, detected_at)
- **removal_reasons**: Archival reasons for removed packages (package, reason). cran-metadata publishes it with no rows and the merge no longer reads it. A removal's reason is in `package_versions`.
- **package_news** — NEWS entries for recently-updated packages (package, version, news_text)
- **cran_maintainer_bounces**: Episodes of CRAN's email to a package's maintainer bouncing (package, episode_seq, version, onset_known, first_seen, last_seen, resolved_on, outcome, archived_on). `onset_known = 0` marks an episode already open when recording began. Not in `observatory.db` until cran-metadata publishes it.
- **cran_check_flavors**, **cran_check_flavor_status_history**: Each CRAN check flavor, and each package's check status and flags per flavor as episodes, with the checked version kept alongside. Not in `observatory.db` until cran-metadata publishes them.

### From `downloads.db` (cran-downloads)

- **downloads_daily** — Daily download counts per package (package, date, count)
- **downloads_summary** — Computed download stats (package, total_30d, total_90d, total_365d, rank_30d, rank_90d, rank_365d, avg_daily_30d, trend)

### From `autoobs-downloads-summary.db` (autoobs-downloads)

- **autoobs_downloads_summary** — Per-package openSUSE autoCRAN download stats (package, package_lower, id, total_1d, total_7d, total_30d, cnt_total, avg_daily_30d, rank_30d, trend, autocran_only, first_seen, last_snapshot). `autocran_only = 1` marks names served only by autoCRAN (the count is exact); `0` means the name is also shipped elsewhere on openSUSE, so the name-aggregated count is a superset. `cnt_total` is MirrorCache's retained total, not a lifetime count, and `total_1d` is NULL until MirrorCache has counted the day before the snapshot.
- **autoobs_runs**: One row per autoobs-downloads run, heartbeats included: what was asked of MirrorCache and what came back, whether the day before had been aggregated upstream (`day_aggregated`), whether the counters kept from earlier runs loaded (`counters_prior`: `loaded`, `none` or `download_failed`), and whether this run's counters were saved (`counters_published`). A day MirrorCache had not aggregated reads differently from a day of zero downloads.

### From `copr-downloads-summary.db` (copr-downloads)

- **copr_downloads_summary** — Per-chroot RPM download stats for the Fedora COPR iucar/cran project (chroot, release, arch, rpms_total, dl_7d, dl_30d, dl_90d, avg_daily_30d, rank_30d, trend, first_date, last_date). Keyed by chroot (Fedora release plus architecture), not by package: COPR exposes no per-package counts.

### From `conda-forge-downloads-summary.db` (conda-forge-downloads)

- **conda_forge_downloads_summary** — Per-package conda-forge download stats for R packages.

### From `bioconda-downloads-summary.db` (bioconda-downloads)

- **bioconda_downloads_summary** — Per-package bioconda download stats for R packages.

### From `queue.db` (cran-queue)

- **queue_snapshots** — Point-in-time snapshots of CRAN incoming queue (snapshot_time, package, version, folder, howlong)
- **queue_stats** — Monthly queue statistics by folder (month, folder, median_hours, p80_hours, p95_hours, total_packages)
- **queue_archive_episodes**: Tarballs seen in CRAN's `incoming/archive/` folder, read once a day (package, version, mtime on CRAN's Europe/Vienna clock, size_kb, first_seen and last_seen in UTC). A `first_seen` equal to the earliest read is censored. Not in `observatory.db` until cran-queue publishes it.
- **queue_archive_reads**: Each daily read of that folder and how many tarballs it listed, so a file that left the folder can be told from a day that was not read. Not in `observatory.db` until cran-queue publishes it.

### Generated at merge time

- **packages_fts** — FTS5 full-text search index over `packages` (name, title, description, maintainer). Uses porter stemming and unicode61 tokenization.
- **pipeline_metadata**: One row per pipeline for the freshness page: schedule, release, when it last ran and last changed, and `data_through`. `data_through` is the value the producer declares (a day, or a month as `YYYY-MM` for a monthly source such as Bioconductor downloads), else its summary value, else the newest day its shards hold.

## History release

The prerelease `history` holds small series folded out of the dated `metadata.db` and `observatory.db` releases, kept as episodes: a value held from `first_seen` to `last_seen`, and `ended_on` is the first snapshot where it no longer held, so a change happened after `last_seen` and by `ended_on`. The release is never marked Latest, and its assets are added, never replaced or deleted.

- `history-YYYY-MM-DD.db.zst` holds every series, per-flavor check status and check timings included, with `history-YYYY-MM-DD-manifest.json` (sha256, rows per table, and the first and last release folded from each source).
- `history-merge-YYYY-MM-DD.db.zst` holds only the tables observatory.db may merge (check issues, deadline moves, pipeline freshness, the conda-forge, bioconda and Bioconductor download summaries, and the list of releases read), with its own manifest.

The code is in `scripts/history/`: `extract.R` folds new dated releases into a local `history.db`, `assets.R` builds and checks both pairs, and `publish.R` uploads them.

## Feedback

Found a bug, a wrong number, or a missing package? Report it at [r-observatory/feedback](https://github.com/r-observatory/feedback/issues/new/choose). All feedback about R Observatory, the site, the data, and the pipelines, is tracked in one place.
