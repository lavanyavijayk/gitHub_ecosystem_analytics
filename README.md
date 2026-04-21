# GitHub OSS Ecosystem Analytics

**DAMG 7370 Final Project** — End-to-end GCP data pipeline analyzing 10 years (2016--2025) of GitHub open-source activity to uncover language trends, the AI/ML boom, COVID-19 impacts, and repository health patterns.

---

## Project Overview

This project ingests GitHub Archive event data and GitHub REST API metadata, transforms it through a medallion architecture (raw → silver → gold), trains ML models for abandonment risk prediction and trend forecasting, and surfaces insights through 8 interactive Looker Studio dashboards — all orchestrated on Google Cloud Platform.

### Key Analytical Objectives

| # | Objective | Data Source |
|---|-----------|-------------|
| 1 | Programming Language Adoption Over a Decade | `analysis_language_evolution` |
| 2 | The AI/ML Boom Effect (2022--2025) | `analysis_ai_boom` |
| 3 | COVID-19 Impact on Developer Activity | `analysis_covid_impact` |
| 4 | Repository Health Scorecard | `analysis_repo_health` |
| 5 | Contributor Dynamics (New vs. Returning) | `analysis_contributor_dynamics` |
| 6 | Repository Abandonment Risk (ML) | `analysis_ml_risk_scores` |
| 7 | Language Market Share Forecast (2026--2027) | `analysis_language_forecast` |
| 8 | Repository Growth Trajectories | `analysis_repo_trajectories` |

---

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────┐
 │  DATA INGESTION (one-time, local)                               │
 │                                                                 │
 │  python fetch_data.py              ← GH Archive (60 files)     │
 │  python github_api_enrichment.py   ← GitHub REST API (top 2K)  │
 │  python upload_to_gcs.py           ← files land in GCS raw/    │
 └──────────────────────────┬──────────────────────────────────────┘
                            │  Pub/Sub notification
                            ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  ORCHESTRATION (Cloud Workflows + Cloud Scheduler)              │
 │                                                                 │
 │  Daily  (03:00 UTC) — batch_pipeline:                           │
 │    Pull Pub/Sub queue → create Dataproc cluster                 │
 │      → silver_layer.py  (raw JSON → Delta silver tables)        │
 │      → github_api_ingestion.py (enrich repos via API)           │
 │      → gold_layer.py   (silver → star-schema gold + BigQuery)   │
 │      → ml_score.py     (score repos with trained model)         │
 │    → delete cluster                                             │
 │                                                                 │
 │  Monthly (1st, 02:00 UTC) — monthly_pipeline:                   │
 │    Create cluster → run in parallel:                            │
 │      → ml_train.py          (Random Forest training)            │
 │      → ml_trajectory.py     (K-Means clustering)                │
 │      → ml_language_trends.py(language forecasting)              │
 │    → ml_score.py → delete cluster                               │
 └──────────────────────────┬──────────────────────────────────────┘
                            │  Gold tables in BigQuery
                            ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │  ANALYTICS LAYER                                                │
 │  BigQuery — 17 views (8 analysis + dimensions + facts)          │
 │       ↓                                                         │
 │  Looker Studio — 8 interactive dashboards                       │
 └─────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Service | Purpose |
|-------|---------|---------|
| **Storage** | Cloud Storage (GCS) | Data lake — raw / silver / gold / scripts buckets |
| **Processing** | Dataproc (PySpark) | Distributed ETL, Delta Lake tables |
| **Warehouse** | BigQuery | Gold tables, analysis views, ML output |
| **Orchestration** | Cloud Workflows | Pipeline DAGs (batch + monthly) |
| **Scheduling** | Cloud Scheduler | Daily (03:00 UTC) + Monthly (1st, 02:00 UTC) |
| **Messaging** | Pub/Sub | GCS upload event notifications + dead-letter queue |
| **Secrets** | Secret Manager | 4x GitHub PATs for round-robin API calls |
| **Monitoring** | Cloud Monitoring | Email alerts on pipeline failures |
| **Analytics** | Looker Studio | 8 interactive dashboard pages |
| **IaC** | Terraform (>= 1.6) | Provisions all GCP resources |

---

## Data Pipeline

### Data Sources

- **GitHub Archive** — 66 `.json.gz` files covering 6 peak hours/day (14:00--19:00 UTC), 1 mid-week day per year, 2016--2026
- **GitHub REST API** — metadata for the top 2,000 repositories (language, license, stars, forks, README, CI)

### Event Types

`PushEvent` | `PullRequestEvent` | `IssuesEvent` | `WatchEvent` | `ForkEvent` | `CreateEvent`

### Medallion Layers

| Layer | Location | Format | Description |
|-------|----------|--------|-------------|
| **Raw** | `gs://{prefix}-raw/` | `.json.gz` | Unmodified GH Archive files |
| **Silver** | `gs://{prefix}-silver/` | Delta Lake | Cleaned, validated, schema-enforced event tables (push, PR, issue, watch, fork, create) + API-enriched repositories & contributors |
| **Gold** | `gs://{prefix}-gold/` + BigQuery | Delta Lake + BQ | Star schema — dimension tables (`dim_date`, `dim_repository`, `dim_contributor`, `dim_language`) and fact tables (`fact_repo_activity_daily`, `fact_push_events`, `fact_pr_events`, `fact_issue_events`, `fact_language_yearly`) |

### ML Pipeline

| Model | Algorithm | Schedule | Output |
|-------|-----------|----------|--------|
| **Abandonment Risk** | Random Forest (Spark MLlib) | Monthly | `ml_abandonment_risk` — probability (0--1) per repo |
| **Growth Trajectories** | K-Means Clustering (k=2--6, silhouette-optimized) | Monthly | `ml_repo_trajectories` — labels: Viral, Established, Stable, Declining, Dormant |
| **Language Forecast** | Linear Regression (scikit-learn) | Monthly | `ml_language_trend_forecasts` — 2026--2027 market share predictions |

---

## How It Works — Detailed Flows

### 1. Data Ingestion Flow

Data ingestion runs locally (one-time) before the cloud pipeline takes over.

```
fetch_data.py                          upload_to_gcs.py
    │                                       │
    │  1. Select 1 mid-week day/year        │  4. Upload .gz files to
    │     (2016-2025, no holidays)           │     gs://{prefix}-raw/gharchive/{YYYY}/
    │                                       │
    │  2. Download 6 peak hours/day         │  5. GCS fires OBJECT_FINALIZE event
    │     (14:00-19:00 UTC) from            │     → Pub/Sub topic receives message
    │     data.gharchive.org                │     (bucket + object path)
    │                                       │
    │  3. Filter to relevant events:        │  6. Messages queue in subscription
    │     Push, PR, Issues, Watch,          │     (7-day retention, DLQ after 5 failures)
    │     Fork, Create                      │
    │     → data/gharchive_filtered/        │
    └───────────────────────────────────────┘
```

**Why these specific hours?** 14:00--19:00 UTC captures the overlap of US afternoon and European evening developer activity, maximizing event density per file.

**Why 1 day per year?** Keeps data manageable (~2 GB) while still spanning a decade of trends. Each day is carefully chosen mid-week to avoid weekend/holiday dips.

### 2. Event-Driven Architecture (Pub/Sub)

The system uses a fully event-driven architecture to decouple file uploads from processing:

```
┌─────────┐    OBJECT_FINALIZE    ┌───────────────┐     pull     ┌─────────────────┐
│ GCS Raw │ ───────────────────→  │  Pub/Sub      │ ←──────────  │ batch_pipeline   │
│ Bucket  │                       │  Topic        │              │ (Cloud Workflow) │
└─────────┘                       │     │         │              └─────────────────┘
                                  │     ▼         │
                                  │ Subscription  │ ── 5 failures ──→ Dead Letter Queue
                                  │ (7-day retain)│                   (14-day retain)
                                  └───────────────┘
```

- **Ack deadline**: 10 minutes — messages are acknowledged *before* cluster creation to avoid deadline expiry during long pipeline runs
- **Max messages per pull**: 50 — batches multiple uploads into a single cluster run
- **Dead-letter queue (DLQ)**: After 5 failed delivery attempts, messages route to a separate DLQ topic retained for 14 days for investigation
- **Cost optimization**: If the queue is empty when the scheduler fires, the workflow exits immediately — no Dataproc cluster is created, no cost incurred

### 3. Daily Batch Pipeline — Step-by-Step

Triggered by Cloud Scheduler daily at **03:00 UTC**. The workflow (`batch_pipeline.yaml`) executes:

```
Cloud Scheduler (03:00 UTC)
    │
    ▼
┌─ Pull Pub/Sub (up to 50 messages) ──────────────────────────────────┐
│                                                                      │
│  Queue empty? ── YES ──→ Return "skipped" (no cluster, no cost)     │
│       │                                                              │
│      NO                                                              │
│       │                                                              │
│  ┌─ Extract file paths + ack IDs from messages ─┐                   │
│  │  Extract year from first file name (YYYY)     │                   │
│  │  Acknowledge ALL messages immediately         │                   │
│  └───────────────────────────────────────────────┘                   │
│       │                                                              │
│  ┌─ Create ephemeral Dataproc cluster ──────────────────────────┐   │
│  │  Name: {prefix}-batch-{unix_timestamp}                        │   │
│  │  Master: e2-standard-2 + 2 workers                           │   │
│  │  init_cluster.sh installs Delta Lake JARs + Python packages  │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  ┌─ FOR EACH FILE (sequential — avoids Delta write conflicts) ──┐   │
│  │                                                               │   │
│  │  silver_layer.py --triggered_file gs://.../{file}.json.gz    │   │
│  │    • Read raw JSON with enforced schema                       │   │
│  │    • Filter to valid event types, null checks, date range     │   │
│  │    • Dedup by event_id                                        │   │
│  │    • Parse nested payload JSON per event type                 │   │
│  │    • Delta MERGE (insert-only) into silver/{table}            │   │
│  │    • Writes: push_events, pr_events, issues_events,          │   │
│  │             watch_events, fork_events, create_events,        │   │
│  │             release_events                                    │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  ┌─ github_api_ingestion.py (runs once after all silver) ───────┐   │
│  │  • Rank repos by event count for the batch year               │   │
│  │  • Select top 40% (capped at 4,000 repos)                    │   │
│  │  • Incremental: skip repos already in silver/repositories     │   │
│  │  • Fetch via GitHub REST API (4-PAT round-robin)              │   │
│  │  • Extract: language, license, stars, forks, topics, etc.     │   │
│  │  • Delta MERGE → silver/repositories                          │   │
│  │  • Derive contributors from all silver event tables           │   │
│  │  • Delta MERGE → silver/contributors                          │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  ┌─ gold_layer.py (runs once) ──────────────────────────────────┐   │
│  │  • Build star schema dimensions + facts (see Gold Layer below)│   │
│  │  • Delta MERGE into gold/ tables                              │   │
│  │  • OPTIMIZE + VACUUM gold Delta tables (7-day retention)      │   │
│  │  • Write ALL gold tables to BigQuery (overwrite mirror)       │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  ┌─ ml_score.py (runs once) ────────────────────────────────────┐   │
│  │  • Load latest trained model from gold/ml_model_current       │   │
│  │  • Feature engineer all repos (shared ml_features.py)         │   │
│  │  • Score every repo → abandonment_probability + risk_tier     │   │
│  │  • Delta MERGE → gold/ml_abandonment_risk                     │   │
│  │  • Write to BigQuery: ml_abandonment_risk                     │   │
│  │  (Gracefully skips if no trained model exists yet)            │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  ┌─ Delete Dataproc cluster (always — even on failure) ─────────┐   │
│  │  try/except wraps the entire pipeline                         │   │
│  │  Cleanup runs in a finally-equivalent block                   │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  Return: "batch_complete" or raise captured error                    │
└──────────────────────────────────────────────────────────────────────┘
```

### 4. Monthly ML Pipeline — Step-by-Step

Triggered by Cloud Scheduler on the **1st of every month at 02:00 UTC**:

```
Cloud Scheduler (1st, 02:00 UTC)
    │
    ▼
┌─ Create ephemeral Dataproc cluster ─────────────────────────────────┐
│  Name: {prefix}-monthly-{unix_timestamp}                            │
│       │                                                              │
│  ┌─ PARALLEL EXECUTION (3 jobs simultaneously) ─────────────────┐   │
│  │                                                               │   │
│  │  ┌─ ml_train.py ─────────────┐  ┌─ ml_trajectory.py ───┐   │   │
│  │  │ Random Forest training     │  │ K-Means clustering   │   │   │
│  │  │ (see ML details below)     │  │ (see ML details)     │   │   │
│  │  └────────────────────────────┘  └──────────────────────┘   │   │
│  │                                                               │   │
│  │  ┌─ ml_language_trends.py ────┐                              │   │
│  │  │ Linear regression per lang │                              │   │
│  │  │ (see ML details below)     │                              │   │
│  │  └────────────────────────────┘                              │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │ (wait for all 3 to complete)                                 │
│       ▼                                                              │
│  ┌─ ml_score.py ────────────────────────────────────────────────┐   │
│  │  Score all repos with the FRESHLY TRAINED model               │   │
│  └───────────────────────────────────────────────────────────────┘   │
│       │                                                              │
│  Delete cluster (always — even on failure)                           │
└──────────────────────────────────────────────────────────────────────┘
```

### 5. Silver Layer — Transformations in Detail

`silver_layer.py` processes raw GH Archive JSON into 7 clean Delta Lake tables:

| Silver Table | Source Event | Key Fields Extracted | Transformations |
|---|---|---|---|
| `push_events` | PushEvent | commit_count, branch_ref | Parse payload JSON, coalesce nulls to 0/`"unknown"` |
| `pr_events` | PullRequestEvent | action, is_merged, additions, deletions, changed_files, review_comments, merged_at | Flatten nested `pull_request` object, coalesce booleans |
| `issues_events` | IssuesEvent | action, state, comments_count, label_list, closed_at | Flatten nested `issue` object, transform label array to CSV |
| `watch_events` | WatchEvent | (starring event) | Filter to `action == "started"` only |
| `fork_events` | ForkEvent | fork_id, fork_name | Flatten nested `forkee` object |
| `create_events` | CreateEvent | ref_type (branch/tag), description | Simple payload extraction |
| `release_events` | ReleaseEvent | tag_name, is_prerelease, is_draft, published_at | Flatten nested `release` object |

**Common transformations applied to all tables:**
- Schema enforcement via explicit `StructType` definitions
- Timestamp validation (must be between 2008-01-01 and current time)
- Null filtering on `actor_id`, `repo_id`, `created_at`
- Global deduplication on `event_id`
- Year column extraction + partitioning by year
- Source file tracking for audit

**Write strategy:**
- **Incremental mode** (default): Delta MERGE — insert new rows only, skip existing `event_id`s. Retries up to 5 times on `ConcurrentAppendException` with exponential backoff
- **Full refresh mode**: Overwrite entire table, then run data quality audit + VACUUM (7-day retention)

### 6. Gold Layer — Star Schema Construction

`gold_layer.py` transforms silver tables into a dimensional model:

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │─────────────────│
                    │ date_key (PK)   │
                    │ year, quarter   │
                    │ month, hour     │
                    │ is_weekend      │
                    │ is_covid_period │
                    └────────┬────────┘
                             │
┌──────────────────┐    ┌────┴─────────────────────────┐    ┌──────────────────┐
│ dim_repository   │    │  fact_repo_activity_daily     │    │ dim_contributor  │
│──────────────────│    │──────────────────────────────│    │──────────────────│
│ repo_key (PK)    │◄───│ date_key (FK)                │───►│ contributor_key  │
│ repo_name        │    │ repo_key (FK)                │    │ login            │
│ language         │    │ language_key (FK)             │    │ is_bot           │
│ license, topics  │    │ total_pushes, total_commits  │    │ account_type     │
│ is_ai_ml_repo    │    │ unique_contributors          │    └──────────────────┘
│ has_readme       │    │ total_prs_opened/merged      │
│ has_ci           │    │ total_issues_opened/closed    │    ┌──────────────────┐
│ stars_at_extract │    │ total_stars, total_forks     │    │ dim_language     │
│ created_year     │    │ total_releases               │    │──────────────────│
│ repo_age_days    │    │ avg_pr_merge_hours           │    │ language_key (PK)│
└──────────────────┘    │ avg_issue_close_hours        │───►│ language_name    │
                        └──────────────────────────────┘    │ paradigm         │
                                                            │ type_system      │
              Additional Fact Tables:                       │ first_appeared   │
              ┌─────────────────────────────┐              │ is_emerging      │
              │ fact_push_events            │              └──────────────────┘
              │ fact_pr_events              │
              │ fact_issues_events          │
              │ fact_release_events         │
              │ fact_language_yearly        │
              └─────────────────────────────┘
```

**Key transformations in the gold layer:**
- **Surrogate keys**: All dimension tables use surrogate keys (`repo_key`, `contributor_key`, `language_key`, `date_key`) for efficient joins
- **AI/ML detection**: Repos flagged as AI/ML based on regex pattern matching against topics and repo name (`ai|ml|llm|gpt|deep.learning|machine.learning|neural|transformer|diffusion|langchain`)
- **Bot detection**: Contributors flagged as bots via login pattern matching (`[bot]`, `bot$`, `github.actions`)
- **PR size bucketing**: PRs classified as XS/S/M/L/XL based on lines changed (<=10 / <=50 / <=250 / <=1000 / >1000)
- **Time-to-merge/close**: Computed in hours for PRs and issues
- **Language market share**: Calculated as `language_push_events / total_year_push_events * 100` with year-over-year growth
- **COVID period flag**: `is_covid_period = true` for dates between 2020-03-01 and 2021-06-30
- **Activity spine**: `fact_repo_activity_daily` is built from a union of ALL event types, so repos with only stars (no pushes) are included
- **OPTIMIZE + VACUUM**: After all writes, Delta tables are compacted and old versions vacuumed (7-day retention)

**BigQuery sync**: Every gold table is written to BigQuery as a read-only mirror. Each row is tagged with `_pipeline_run_id` (UUID) and `_loaded_at` timestamp for audit.

### 7. GitHub API Enrichment — Round-Robin PAT Strategy

The API enrichment job (`github_api_ingestion.py`) uses a sophisticated multi-token strategy:

```
┌─────────────────────────────────────────────────────────────────┐
│  4 GitHub PATs stored in Secret Manager                         │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐                       │
│  │ PAT1 │  │ PAT2 │  │ PAT3 │  │ PAT4 │   5,000 calls/hr each│
│  └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘                       │
│     └──────┴──────┴──────┴──────────┐                          │
│                                     │ Round-robin (request % 4)│
│                                     ▼                          │
│  Request 1 → PAT1    Request 2 → PAT2    Request 3 → PAT3 ... │
│                                                                 │
│  Rate limit hit on PAT N?                                       │
│    → Primary exhausted: cycle to PAT (N+1) % 4                 │
│    → Secondary (per-IP): respect Retry-After header, then retry│
│    → All tokens exhausted: sleep until earliest reset time     │
│                                                                 │
│  Safety guards:                                                 │
│    • Max 4,800 calls/token (hard stop before GitHub's 5,000)   │
│    • Max 435 min runtime (leaves 45 min for Delta writes)      │
│    • All tokens < 100 remaining → save partial results          │
│    • Every 200 repos: log progress + check all token quotas    │
└─────────────────────────────────────────────────────────────────┘
```

**What gets enriched**: For each top repo, the job calls `GET /repos/{owner}/{repo}` and extracts:
- `language`, `license` (SPDX ID), `topics` (comma-separated)
- `stargazers_count`, `forks_count`, `archived` status
- `description` (truncated to 500 chars), `created_at`

**Contributor derivation**: After API enrichment, all unique `actor_id` values are extracted from every silver event table and written to `silver/contributors` with a simple `account_type = "User"` default.

### 8. ML Models — Detailed Working

#### 8a. Abandonment Risk Prediction (`ml_train.py` + `ml_score.py`)

**Training (monthly):**

```
Gold Tables                Feature Engineering              ML Pipeline
─────────────             ─────────────────────           ─────────────────
dim_repository    ───┐    25+ features computed:          Imputer (median)
fact_daily        ───┤                                         │
fact_push_events  ───┤    Early Activity (first 90 days):     StringIndexer
fact_pr_events    ───┘    • early_commits, early_pushes       (language, license)
                          • early_stars, early_forks            │
                          • early_prs_opened/merged            OneHotEncoder
                          • early_issues                        │
                          • avg_daily_contributors             VectorAssembler
                          • active_days_in_90                   │
                                                               StandardScaler
                          Push Features (90 days):              │
                          • unique_branches                    RandomForest
                          • default_branch_push_ratio           (100 trees, depth=8)
                                                                │
                          PR Features (90 days):               CrossValidator
                          • avg_pr_additions                    (3-fold, AUC-ROC)
                          • avg_review_comments                 │
                          • pr_merge_rate                    Grid Search:
                                                             numTrees: [50, 100, 200]
                          Contributor Quality:                maxDepth: [5, 8, 12]
                          • total_unique_contributors
                            (non-bots only)

                          Trajectory:
                          • commit_momentum
                            (recent_avg / early_avg + 1)
                          • avg_commits_early
                          • avg_commits_recent

                          Lifetime:
                          • years_observed
                          • total_active_days

                          Derived Ratios:
                          • fork_to_star_ratio
                          • commits_per_active_day

                          Metadata:
                          • repo_age_days, created_year
                          • is_ai_ml_repo, has_readme, has_ci
```

**Labeling strategy:**
- `label = 1` (abandoned): Repo is archived, OR repo is 2+ years old with no activity in last 2 years
- `label = 0` (active): Repo is 2+ years old with recent activity
- `label = NULL` (excluded): Repo is less than 2 years old (too young to judge)

**Temporal train/test split** (prevents data leakage):
- **Train**: Repos created on or before `current_year - 4` (e.g., <=2022)
- **Test**: Repos created between `current_year - 3` and `current_year - 2` (e.g., 2023--2024)

**Class imbalance handling**: Weighted Random Forest — weight ratio = `active_count / abandoned_count`, applied via `class_weight` column.

**Model outputs saved to GCS/BigQuery:**
- Versioned model: `gs://{prefix}-gold/ml_model_{YYYYMM}/`
- Model pointer: `gs://{prefix}-gold/ml_model_current/` (Delta table with latest model path)
- Model metadata: `ml_model_metadata` (AUC, F1, accuracy, hyperparameters)
- Feature importance: `ml_feature_importance` (feature name + importance score)
- Test predictions: `ml_test_predictions` (with True/False Positive/Negative labels)

**Scoring (daily + monthly):**
- Loads the latest model via the `ml_model_current` pointer
- Runs feature engineering on ALL repos (not just the test set)
- Outputs: `abandonment_probability` (0--1) + `risk_tier` (High Risk >= 0.70, Moderate >= 0.40, Low Risk, Too Young < 2 years)
- Also adds `trajectory_label` based on commit momentum (Accelerating / Stable / Declining / Stalled)

#### 8b. Repository Growth Trajectories (`ml_trajectory.py`)

```
fact_repo_activity_daily
    │
    │  Pivot: yearly commit counts (2016-2025) per repo
    │  Filter: repos with >= 2 years of data
    │
    ▼
┌─ MinMaxScaler → K-Means ────────────────────────────────┐
│                                                          │
│  Silhouette analysis (k=2 to k=6):                      │
│    Evaluate each k → pick highest silhouette score       │
│                                                          │
│  Label clusters by growth ratio:                         │
│    growth_ratio = avg_commits_recent / (avg_early + 1)   │
│    Rank clusters by mean growth ratio (highest first)    │
│                                                          │
│  Labels assigned:                                        │
│    Rank 1 → "Viral"        (explosive recent growth)     │
│    Rank 2 → "Established"  (strong, sustained activity)  │
│    Rank 3 → "Stable"       (consistent over time)        │
│    Rank 4 → "Declining"    (decreasing activity)         │
│    Rank 5 → "Dormant"      (minimal recent activity)     │
│    Rank 6 → "Inactive"     (effectively abandoned)       │
└──────────────────────────────────────────────────────────┘
    │
    ▼
  BigQuery: ml_repo_trajectories
```

#### 8c. Language Market Share Forecasting (`ml_language_trends.py`)

```
fact_language_yearly + dim_language
    │
    │  Filter: languages with >= 3 years of data
    │
    │  Per-language scikit-learn LinearRegression:
    │    X = year (2016-2025)
    │    y = market_share_pct
    │
    ▼
  Outputs per language:
    • slope_pct_per_year: rate of change
    • r_squared: model fit quality
    • forecast_2026_share: predicted market share
    • forecast_2027_share: predicted market share
    • trend_label:
        slope >  0.5 → "Strongly Growing"
        slope >  0.1 → "Growing"
        slope > -0.1 → "Stable"
        slope > -0.5 → "Declining"
        else         → "Strongly Declining"
    │
    ▼
  BigQuery: ml_language_trend_forecasts
```

### 9. BigQuery Analytics Views

17 analysis views are deployed to BigQuery on top of the gold tables, providing pre-computed analytics for Looker Studio:

| View | Purpose | Key Logic |
|---|---|---|
| `analysis_language_evolution` | Language market share trends | Joins `fact_language_yearly` + `dim_language`, filters 2016--2025 |
| `analysis_ai_boom` | AI/ML activity share | Groups by `is_ai_ml_repo`, computes `ai_activity_share_pct` per year |
| `analysis_covid_impact` | COVID-era developer patterns | Filters 2019--2021, groups by hour + weekend flag |
| `analysis_repo_health` | Repo quality scorecard | Composite score: 25% PR merge speed + 25% issue close speed + 25% merge rate + 25% community (log-scaled stars+forks) |
| `analysis_pr_lifecycle` | PR size/merge/review patterns | Groups by year + language + PR size bucket (XS/S/M/L/XL) |
| `analysis_bus_factor` | Contributor concentration risk | Top-1/3/5 contributor push percentage per repo |
| `analysis_ecosystem_growth` | Year-over-year GitHub growth | Aggregates all activity metrics by year |
| `analysis_hourly_heatmap` | Peak activity hour x day-of-week | Push/PR counts by hour and day-of-week |
| `analysis_ml_predictions` | Abandonment risk per repo | Joins `ml_abandonment_risk` + `dim_repository`, adds confidence tiers |
| `analysis_ml_by_language` | Risk breakdown by language | Aggregates risk tiers per language (min 10 repos) |
| `analysis_ml_feature_importance` | Model feature rankings | Ranks features by importance with percentage |
| `analysis_abandonment_risk_current` | Latest model snapshot | Filters to latest `model_version` only |
| `analysis_abandonment_by_language` | Risk distribution per language | Current model version, grouped by language |
| `analysis_abandonment_trend` | Risk trend across model versions | Compares risk tier distributions over time |
| `analysis_repo_trajectories` | Growth cluster labels | Joins `ml_repo_trajectories` + `dim_repository` |
| `analysis_language_forecast` | 2026--2027 language predictions | Joins `ml_language_trend_forecasts` + `dim_language` |
| `analysis_contributor_dynamics` | New vs. returning contributors | CTE-based: first-year classification, year-over-year retention rate |

### 10. Infrastructure & IAM

Two dedicated service accounts enforce least-privilege access:

| Service Account | Used By | Permissions |
|---|---|---|
| `{prefix}-dataproc` | Dataproc cluster nodes | `storage.objectAdmin` (GCS read/write), `secretmanager.secretAccessor` (PATs), `bigquery.dataEditor` + `bigquery.jobUser` (BQ write), `dataproc.worker` |
| `{prefix}-workflows` | Cloud Workflows | `dataproc.editor` (create/delete clusters, submit jobs), `workflows.invoker` (scheduler trigger), `pubsub.subscriber` (pull messages), `iam.serviceAccountUser` on dataproc SA |

**Cluster configuration:**
- Machine type: `e2-standard-2` (master + 2 workers)
- Image: Dataproc 2.1 (Spark 3.x, Python 3.11)
- Init action: `init_cluster.sh` installs Delta Lake 2.3.0 JARs + Python packages (`delta-spark`, `google-cloud-secret-manager`, `google-cloud-storage`)
- Ephemeral: Created per pipeline run, deleted after completion

### 11. Error Handling & Resilience

| Component | Strategy |
|---|---|
| **Workflow-level** | `try/except` wraps entire pipeline; cluster cleanup runs unconditionally; captured error is re-raised after cleanup |
| **Silver Delta writes** | Retry up to 5 times on `ConcurrentAppendException` with exponential backoff + jitter |
| **API rate limits** | Round-robin across 4 PATs; respect `Retry-After` header; sleep until reset on full exhaustion; save partial results on timeout |
| **API runtime** | Hard 435-min limit (leaves 45 min for Delta writes within workflow's 8-hr timeout) |
| **Pub/Sub** | 10-min ack deadline; 7-day message retention; DLQ after 5 delivery failures |
| **Model scoring** | Graceful skip if `ml_model_current` pointer doesn't exist (first run before any training) |
| **Monitoring** | Cloud Monitoring alerts on Workflow execution failures + Dataproc job failures → email notification |
| **Data quality** | Full-refresh runs produce a `dq_audit` table comparing raw counts vs. accepted counts per event type |

### 12. Data Quality Assurance

On full-refresh runs, `silver_layer.py` produces a `dq_audit` Delta table:

| Column | Description |
|---|---|
| `event_type` | PushEvent, PullRequestEvent, etc. |
| `raw_count` | Total events of this type in raw input |
| `accepted_count` | Events that passed validation and were written to silver |
| `run_mode` | `incremental` or `full_refresh` |
| `audited_at` | UTC timestamp |

Additional quality measures:
- **Schema enforcement**: Every event type has an explicit `StructType` — malformed payloads are dropped, not silently coerced
- **Deduplication**: Global `dropDuplicates(["event_id"])` prevents duplicate events across file boundaries
- **Null filtering**: Events without `actor_id`, `repo_id`, or `created_at` are dropped before processing
- **Date range validation**: Events before 2008-01-01 or after the current timestamp are rejected
- **VACUUM**: Old Delta versions cleaned up after 7 days on full-refresh runs

---

## Project Structure

```
gitHub_ecosystem_analytics/
├── fetch_data.py                          # Download & filter GH Archive files
├── LOOKER_STUDIO_GUIDE.md                 # Dashboard setup guide (fields, visuals, metrics)
│
├── dataproc/                              # PySpark jobs (run on Dataproc)
│   ├── silver_layer.py                    # Raw → Silver (clean, validate, partition)
│   ├── gold_layer.py                      # Silver → Gold star schema + BigQuery write
│   ├── github_api_ingestion.py            # Enrich top repos via GitHub API (4-PAT round-robin)
│   ├── ml_train.py                        # Random Forest model training
│   ├── ml_score.py                        # Score all repos with trained model
│   ├── ml_features.py                     # Shared feature engineering (25+ features)
│   ├── ml_trajectory.py                   # K-Means repo growth clustering
│   ├── ml_language_trends.py              # Language market share forecasting
│   └── init_cluster.sh                    # Dataproc cluster initialization script
│
├── scripts/                               # Utility scripts
│   ├── upload_to_gcs.py                   # Upload filtered data to GCS raw bucket
│   ├── github_api_enrichment.py           # Local GitHub API enrichment (top 2K repos)
│   ├── create_bq_views.sql                # 8 BigQuery analysis views for Looker Studio
│   ├── apply_bq_views.sh                  # Deploy views to BigQuery
│   ├── run_from_api.sh                    # Manual pipeline execution
│   └── run_from_api_parallel.sh           # Parallel API enrichment runner
│
├── workflows/                             # Cloud Workflows definitions
│   ├── batch_pipeline.yaml                # Daily: Pub/Sub → silver → API → gold → ML score
│   └── monthly_pipeline.yaml              # Monthly: ML train + trajectory + forecast → score
│
├── terraform/                             # Infrastructure as Code
│   ├── main.tf                            # GCP APIs, GCS buckets
│   ├── variables.tf                       # Project config (project_id, PATs, region)
│   ├── bigquery.tf                        # BigQuery dataset
│   ├── workflows.tf                       # Cloud Workflows + Cloud Scheduler
│   ├── iam.tf                             # Service accounts & IAM bindings
│   ├── pubsub.tf                          # Pub/Sub topic, subscription, DLQ
│   ├── secrets.tf                         # Secret Manager (4x GitHub PATs)
│   ├── monitoring.tf                      # Pipeline failure alert policy
│   ├── scripts_upload.tf                  # Upload Dataproc scripts to GCS
│   └── output.tf                          # Terraform outputs
│
└── data/                                  # Local data (not committed)
    ├── gharchive_raw/                     # Raw GH Archive downloads
    └── gharchive_filtered/                # Filtered event files
```

---

## Prerequisites

- GCP account (Google Cloud for Students — $300 credits)
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.6
- [Google Cloud CLI (`gcloud`)](https://cloud.google.com/sdk/docs/install)
- Python 3.11+
- 4 GitHub Personal Access Tokens (for round-robin API enrichment)

---

## Setup & Deployment

### Step 1 — Clone & Configure

```bash
git clone <repo-url>
cd gitHub_ecosystem_analytics/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
project_id  = "your-gcp-project-id"
github_pat   = "ghp_xxxx"
github_pat_2 = "ghp_yyyy"
github_pat_3 = "ghp_zzzz"
github_pat_4 = "ghp_wwww"
alert_email  = "you@example.com"   # optional
```

### Step 2 — Deploy Infrastructure

```bash
gcloud auth login
gcloud auth application-default login
terraform init
terraform plan
terraform apply    # ~8 minutes
```

Terraform provisions (in dependency order):
1. GCP APIs enabled (Dataproc, Storage, BigQuery, Workflows, Pub/Sub, etc.)
2. GCS buckets — `raw`, `silver`, `gold`, `scripts`
3. Secret Manager — 4 GitHub PATs
4. Service accounts + IAM bindings (Dataproc SA, Workflows SA)
5. BigQuery dataset (`github_oss_gold`)
6. Pub/Sub topic + subscription + dead-letter queue
7. Cloud Workflows — `batch_pipeline` + `monthly_pipeline`
8. Cloud Scheduler — daily (03:00 UTC) + monthly (1st, 02:00 UTC)
9. Cloud Monitoring — failure alert policy
10. Dataproc scripts uploaded to GCS

### Step 3 — Download & Upload Data

```bash
# Install dependencies
pip install requests google-cloud-storage

# Download & filter GH Archive files (~2 GB)
python fetch_data.py

# Enrich top 2,000 repos via GitHub API (~300 MB)
python scripts/github_api_enrichment.py --pat <your_github_pat>

# Upload to GCS (triggers pipeline automatically via Pub/Sub)
python scripts/upload_to_gcs.py --bucket <raw_bucket_from_terraform_output>
```

As each file lands in the raw bucket, a Pub/Sub notification is published. The batch pipeline picks these up on its next scheduled run (daily at 03:00 UTC).

### Step 4 — Deploy BigQuery Views

```bash
# Option A: via CLI
bq query --use_legacy_sql=false < scripts/create_bq_views.sql

# Option B: via helper script
bash scripts/apply_bq_views.sh
```

This creates 8 analysis views in the `github_oss_gold` dataset that power the Looker Studio dashboards.

### Step 5 — Connect Looker Studio

1. Open [Looker Studio](https://lookerstudio.google.com/)
2. Create a new report → **Add data** → **BigQuery**
3. Select project → dataset `github_oss_gold` → add all `analysis_*` views
4. Build 8 report pages (one per analytical objective)
5. Refer to `LOOKER_STUDIO_GUIDE.md` for detailed field mappings, chart types, and layout instructions

---

## Pipeline Schedules

| Pipeline | Trigger | Schedule | What It Does |
|----------|---------|----------|--------------|
| **Batch** | Cloud Scheduler | Daily, 03:00 UTC | Pulls Pub/Sub queue → silver → API enrichment → gold → ML score → cleanup |
| **Monthly** | Cloud Scheduler | 1st of month, 02:00 UTC | ML model training (Random Forest + K-Means + Linear Regression) → score all repos → cleanup |

Both pipelines create ephemeral Dataproc clusters and delete them after completion to minimize cost. If no files are queued, the batch pipeline exits early without creating a cluster.

---

## Looker Studio Dashboards

| Page | Dashboard | Key Metrics |
|------|-----------|-------------|
| 1 | Programming Language Adoption | Market share %, total commits, YoY growth |
| 2 | AI/ML Boom (2022--2025) | AI activity share %, stars & forks growth |
| 3 | COVID-19 Impact | Hourly push patterns, weekend vs. weekday activity |
| 4 | Repository Health Scorecard | Health score (0--100), PR merge time, issue close time |
| 5 | Contributor Dynamics | New vs. returning contributors, retention rate |
| 6 | Abandonment Risk (ML) | Risk probability, feature importance breakdown |
| 7 | Language Forecast | 2026--2027 market share predictions, trend labels |
| 8 | Repo Growth Trajectories | Cluster labels: Viral, Established, Stable, Declining, Dormant |

See [`LOOKER_STUDIO_GUIDE.md`](LOOKER_STUDIO_GUIDE.md) for complete setup instructions with field-level detail for every chart.

---

## Estimated GCP Cost

| Service | Estimated Cost |
|---------|----------------|
| Cloud Storage (GCS) | ~$1 |
| Dataproc (~4 hrs total) | ~$10 |
| BigQuery (queries + storage) | ~$1--2 |
| Cloud Workflows | ~$0.01 |
| Pub/Sub | ~$0.01 |
| Secret Manager | ~$0.10 |
| Cloud Monitoring | Free tier |
| **Total** | **~$12--14** |

---

## Manual Pipeline Execution

```bash
# Run the batch pipeline manually (Cloud Console or CLI)
gcloud workflows run batch-pipeline --location=us-central1

# Run the monthly ML pipeline manually
gcloud workflows run monthly-pipeline --location=us-central1

# Or use the helper scripts for specific stages
bash scripts/run_from_api.sh <gcp-project-id>
```

---

## Teardown

```bash
terraform destroy   # removes ALL GCP resources
```
