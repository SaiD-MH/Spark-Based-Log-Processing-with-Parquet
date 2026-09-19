# Spark Log Analytics Pipeline

An end-to-end data engineering pipeline that ingests raw nginx web server access logs, cleanses and structures them with Apache Spark, lands them as partitioned Parquet, exposes them as queryable tables through Hive, and serves DevOps-facing dashboards on traffic patterns and error rates.

## Business Goal

Web server access logs contain the raw signal needed to answer two operational questions:

- **Is traffic behaving normally, or is something spiking?**
- **Are users hitting errors, and where?**

Without a pipeline, this data sits as flat text files that nobody can query. This project turns that raw log data into structured, partitioned tables that a BI tool can query directly, so DevOps can monitor traffic and error trends without manually grepping log files.

## Architecture

![Architecture](architecture/log%20analysis%20data%20pipeline.png)

The pipeline follows a layered (bronze → silver) design:

```
Source Logs → Ingestion (Python) → Bronze (HDFS, raw) → Transformation (Spark)
   → Silver (HDFS, Parquet) → Hive (metastore + query engine) → Apache Superest
```

Orchestrated end-to-end by **Airflow**, running in Docker.

## Pipeline Stages

### 1. Ingestion (`src/ingestion/ingestion.py`)

Moves the raw daily log file from the source system into the HDFS bronze zone, untouched.

**Why a plain Python script, not Spark?** Ingestion is a file-copy operation, not a distributed computation — there's nothing to parallelize. Spark's driver/executor overhead would add cost with no benefit here.

**Why keep the bronze zone at all, instead of transforming straight from source?** It's the pipeline's insurance policy. If a transformation bug is discovered weeks later, or the source system only retains logs briefly, the untouched raw file is still available to reprocess. Nothing is ever overwritten in bronze.

**Idempotency (safe to retry):**
- Before doing anything, the script checks whether the target file already exists at its final bronze path (`/bronze/date=<run_date>/access_logs.txt`). If it does, the run is a no-op — no re-read, no re-transfer.
- If it doesn't exist, the file is written to a **staging path** first (`hdfs dfs -put`), and only **after** that succeeds is it moved into place with an atomic `hdfs dfs -mv`.
- A rename in HDFS is a metadata-only operation — it either completes fully or not at all. This guarantees the final bronze path can never contain a partially-written file, regardless of *when* the process crashes (mid-copy or mid-rename).
- The staging directory is cleared idempotently at the start of each run using `hdfs dfs -rm -r -f` (`-f` suppresses "does not exist" errors on a brand-new environment, while still failing loudly on real errors like permissions).
- All HDFS subprocess calls use `check=True`, and failures are caught and re-raised (`sys.exit(1)`) so the process exit code correctly signals failure to Airflow — a swallowed exception would let Airflow see a "successful" task that silently did nothing.

**Partitioning:** by **ingestion/run date**, not the date inside the log content. At this layer, the script never opens or parses the file — parsing the file just to decide a folder name would turn a simple copy step into a transformation step, which belongs later in the pipeline.

### 2. Transformation (`src/transformation/transformation.py`)

Spark parses raw log lines into structured rows and produces the curated (silver) tables.

**Parsing:** Each line is matched against a regex with named groups (`ip`, `time`, `method`, `path`, `protocol`, `status`, `size`, `referer`, `agent`) to correctly handle spaces embedded inside bracketed timestamps and quoted fields — a naive `split(" ")` breaks on both.

**Handling malformed lines:** Malformed/bot-mangled/truncated lines are an expected, routine part of this dataset, not a rare edge case — so a single bad line must never crash the whole job. The parsing function uses **`flatMap`** (not `map`, which forces exactly one output per input) so each line can produce either one structured row or zero rows, in a single pass over the data. Each line is tagged (`valid`/`invalid`) as it's parsed once, and the result is split via `filter` into:
- **Valid rows** → continue to cleansing and the silver tables.
- **Invalid/quarantined rows** → written to a separate rejected-records path, preserving the original line and the failure reason, instead of silently discarding it — the same "don't destroy information" principle applied at the row level, not just the file level.

**Partitioning (silver):** by the **event date/hour parsed from the log line itself**, not the date the Spark job happened to run — a file processed a day late must still land its data under the date it actually occurred, or the dashboard would show it in the wrong place.

### 3. Loading / Hive (`src/loading/loading.py`)

Registers and maintains the Hive layer on top of the silver Parquet tables.

**Why Hive instead of pointing  uperest directly at Parquet files?**
- It turns a set of scattered partition folders into a single named, queryable table (`SELECT ... FROM traffic_table`) — the analyst never needs to know about file paths.
- It provides query intelligence that raw Parquet-on-HDFS doesn't have on its own: partition pruning, concurrent-read handling, and a standard SQL interface for BI tools.

**Metastore mechanics:**
- Tables are registered once via `CREATE EXTERNAL TABLE ... PARTITIONED BY (date STRING, hour STRING) LOCATION '/silver/<table_name>/'`.
- New partitions written by Spark are **not** automatically visible to Hive — the metastore only knows what it's explicitly told. After each run, a partition-registration step (`ALTER TABLE ... ADD PARTITION`, preferred over `MSCK REPAIR TABLE` at scale, since repair re-scans every partition in the table rather than just the new one) makes the new data queryable.

### 4. Orchestration (`orchestration/`)

Airflow (Docker) owns the full daily flow: trigger ingestion → trigger Spark transformation → register new Hive partitions. It does **not** own refreshing the BI tool's dataset — that's a separate concern outside this pipeline's scope.

- Each task is independently retryable (`retries`, `retry_delay`) because every underlying script/job is idempotent — retrying never causes duplication or partial state.
- Failures propagate via process exit codes / raised exceptions, so Airflow can detect, retry, and alert correctly.

### 5. Curated Tables

| Table | Grain | Purpose |
|---|---|---|
| `traffic_table` | `date`, `hour`, `is_bot`, `count` | Request volume over time, split by human vs. bot traffic so a bot flood isn't mistaken for (or hidden from) a genuine user spike |
| `status_code_table` | `date`, `hour`, `status`, `count` | HTTP status code distribution over time, for spotting error surges |

`is_bot` is a best-effort heuristic based on user-agent/IP signals — bots that deliberately spoof a real browser's user-agent cannot be reliably detected this way; this is an accepted limitation, not a correctness guarantee.

## Project Structure

```
├── architecture/
│   └── log analysis data pipeline.png     # Architecture diagram
├── orchestration/                          # Airflow DAGs (Docker)
├── servers_run_cmd/
│   └── cmd.txt                             # Commands to start local HDFS/Spark/Hive services
├── source_system/
│   └── access_logs.txt                     # Simulated source system drop location
├── src/
│   ├── ingestion/
│   │   └── ingestion.py                    # Bronze zone loader (idempotent, atomic write)
│   ├── transformation/
│   │   ├── transformation.py               # Spark parsing, cleansing, silver write
│   │   └── spark-warehouse/
│   └── loading/
│       └── loading.py                      # Hive table + partition registration
├── tests/
├── utlities/
│   ├── hdfs-cheatsheet.md
│   └── hdfs-cheatsheet.html
├── requirements.txt
└── README.md
```

## Tech Stack

- **Apache Spark (PySpark)** — log parsing and transformation
- **HDFS** — raw (bronze) and curated (silver) storage
- **Hive** — metastore + SQL query layer
- **Airflow (Docker)** — orchestration and scheduling
- **Apache Superest** — dashboarding

## Key Design Decisions

- **Bronze is immutable, silver is overwrite-per-partition.** Raw files are never modified once landed — they're the only copy of ground truth. Curated partitions, by contrast, are safely re-derivable from bronze at any time, so re-running a day's transformation and overwriting just that partition is both correct and idempotent.
- **Every ingestion/transform step is designed to be safely retried** by an orchestrator without producing duplicates or partial state — this was treated as a first-class design constraint, not an afterthought.
- **Bad data is quarantined, never silently dropped** — at both the file level (bronze as insurance) and the row level (malformed log lines routed to a rejected-records path).

## Setup

See `servers_run_cmd/cmd.txt` for commands to start the local HDFS/Spark/Hive environment, and `utlities/hdfs-cheatsheet.md` for common HDFS operations used throughout this project.

```bash
pip install -r requirements.txt
```

## Possible Extensions

- A third dashboard/table for top requested and top error-generating endpoints (`path`-level grain), deferred from the current scope.
- Backfill support in the ingestion script (currently defaults to a fixed run-date offset; should accept a date parameter from Airflow's logical execution date).
- Migrating the bronze/silver storage layer to S3 with a transactional table format (Delta/Iceberg/Hudi) if this pipeline ever moved off a single HDFS cluster.