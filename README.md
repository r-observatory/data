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
  SELECT p.name, p.title, p.version, d.total_downloads
  FROM packages p
  LEFT JOIN downloads d ON p.name = d.package
  WHERE p.name = 'ggplot2'
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
  SELECT title, pub_date, category
  FROM feed_items
  ORDER BY pub_date DESC
  LIMIT 20
")
```

## Data Sources

| Source | Repository | Schedule | Description |
|--------|-----------|----------|-------------|
| `feed.db` | [r-observatory/cran-feed](https://github.com/r-observatory/cran-feed) | Every 6 hours | CRAN RSS feed events (new, updated, removed packages) |
| `metadata.db` | [r-observatory/cran-metadata](https://github.com/r-observatory/cran-metadata) | Daily at 06:00 UTC | Package metadata, descriptions, maintainers, URLs |
| `downloads.db` | [r-observatory/cran-downloads](https://github.com/r-observatory/cran-downloads) | Daily at 07:00 UTC | Download counts from CRAN logs |
| `queue.db` | [r-observatory/cran-queue](https://github.com/r-observatory/cran-queue) | Every 6 hours | CRAN incoming/outgoing queue snapshots |

## Combined Schema

### From `feed.db` (cran-feed)

- **feed_items** — Individual RSS feed entries (new packages, updates, removals)
- **package_versions** — Version history derived from feed events
- **removal_reasons** — Reasons for package removal from CRAN

### From `metadata.db` (cran-metadata)

- **packages** — Current CRAN package metadata (name, title, description, version, maintainer, license, etc.)
- **packages_enrichment** — Supplementary URL and bug report data

### From `downloads.db` (cran-downloads)

- **downloads** — Daily and total download counts per package

### From `queue.db` (cran-queue)

- **queue_snapshots** — Point-in-time snapshots of the CRAN incoming queue
- **queue_history** — Package lifecycle through queue folders (pretest, inspect, etc.)

### Generated at merge time

- **packages_fts** — FTS5 full-text search index over `packages` (name, title, description, maintainer). Uses porter stemming and unicode61 tokenization.

> **Note:** The `packages_fts` table is a virtual table generated during the merge process. It enables fast full-text search across package metadata.
