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
| `queue.db` | [r-observatory/cran-queue](https://github.com/r-observatory/cran-queue) | Every 2 hours | CRAN incoming queue snapshots |

## Combined Schema

### From `feed.db` (cran-feed)

- **packages** — Current CRAN packages (name, version, title, description, maintainer, license, depends, imports, suggests, published, etc.)
- **package_versions** — Append-only version history (package, version, event_type, previous_version, removal_reason, detected_at)
- **reverse_dependencies** — Reverse dependency relationships (package, rev_package, type)

### From `metadata.db` (cran-metadata)

- **cran_check_results** — CRAN check results per package and flavor (package, flavor, status, tinstall, tcheck, ttotal)
- **cran_check_details** — Detailed check output (package, flavor, check_name, status, output)
- **cran_check_issues** — Packages with check issues (package, version, kind, href)
- **authors** — CRAN author database (package, given, family, email, role, orcid)
- **packages_enrichment** — URL and bug report links (name, url, bug_reports)
- **check_status_history** — Append-only status change log (package, status, flavor_summary, details, detected_at)
- **removal_reasons** — Archival reasons for removed packages (package, reason)
- **package_news** — NEWS entries for recently-updated packages (package, version, news_text)

### From `downloads.db` (cran-downloads)

- **downloads_daily** — Daily download counts per package (package, date, count)
- **downloads_summary** — Computed download stats (package, total_30d, total_90d, total_365d, rank_30d, rank_90d, rank_365d, avg_daily_30d, trend)

### From `queue.db` (cran-queue)

- **queue_snapshots** — Point-in-time snapshots of CRAN incoming queue (snapshot_time, package, version, folder, howlong)
- **queue_stats** — Monthly queue statistics by folder (month, folder, median_hours, p80_hours, p95_hours, total_packages)

### Generated at merge time

- **packages_fts** — FTS5 full-text search index over `packages` (name, title, description, maintainer). Uses porter stemming and unicode61 tokenization.
