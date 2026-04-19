# GitHub OSS Ecosystem Analytics — Project Report

**Course:** DAMG 7370 — Designing Advanced Data Architectures for Business Intelligence  
**University:** Northeastern University  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Project Objectives](#2-project-objectives)
3. [Technology Stack](#3-technology-stack)
4. [System Architecture](#4-system-architecture)
5. [Data Sources & Ingestion](#5-data-sources--ingestion)
6. [Data Pipeline — Medallion Architecture](#6-data-pipeline--medallion-architecture)
   - 6.1 [Raw Layer](#61-raw-layer)
   - 6.2 [Silver Layer](#62-silver-layer)
   - 6.3 [Gold Layer — Dimensional Model](#63-gold-layer--dimensional-model)
7. [GitHub API Enrichment](#7-github-api-enrichment)
8. [Machine Learning Models](#8-machine-learning-models)
   - 8.1 [Abandonment Risk Prediction (Random Forest)](#81-abandonment-risk-prediction-random-forest)
   - 8.2 [Repository Trajectory Clustering (K-Means)](#82-repository-trajectory-clustering-k-means)
   - 8.3 [Language Trend Forecasting (Linear Regression)](#83-language-trend-forecasting-linear-regression)
   - 8.4 [ML Scoring Pipeline](#84-ml-scoring-pipeline)
9. [Workflow Orchestration](#9-workflow-orchestration)
   - 9.1 [Daily Batch Pipeline](#91-daily-batch-pipeline)
   - 9.2 [Monthly ML Training Pipeline](#92-monthly-ml-training-pipeline)
10. [BigQuery Analytics Layer](#10-bigquery-analytics-layer)
11. [Power BI Dashboards](#11-power-bi-dashboards)
12. [Infrastructure as Code (Terraform)](#12-infrastructure-as-code-terraform)
13. [Security & Access Control](#13-security--access-control)
14. [Monitoring & Alerting](#14-monitoring--alerting)
15. [Cost Analysis](#15-cost-analysis)
16. [Key Architectural Decisions](#16-key-architectural-decisions)

---

## 1. Executive Summary

This project implements an end-to-end data analytics platform that processes **10 years of GitHub open-source activity data (2016–2025)** to uncover ecosystem trends, quantify the AI/ML boom's impact, analyze COVID-19's effect on developer behavior, and predict which repositories are at risk of abandonment.

The system ingests ~2.5 GB of compressed event data from GH Archive and enriches it with metadata from the GitHub REST API. Data flows through a **Medallion Architecture** (Raw → Silver → Gold) built on **Apache Spark** and **Delta Lake**, running on **Google Cloud Dataproc**. Three machine learning models provide predictive analytics: a Random Forest classifier for abandonment risk, K-Means clustering for repository trajectory patterns, and per-language linear regression for market share forecasting. Results are surfaced through **BigQuery** analysis views and **Power BI** dashboards.

All infrastructure is provisioned via **Terraform**, and pipelines are orchestrated by **Cloud Workflows** with **Cloud Scheduler** triggers.

---

## 2. Project Objectives

| # | Objective | Approach |
|---|-----------|----------|
| 1 | Track programming language adoption over a decade | Yearly market share metrics in `fact_language_yearly` with YoY growth calculations |
| 2 | Quantify the AI/ML boom (2022–2025) | Flag AI/ML repos via topic and name pattern matching; compare activity share over time |
| 3 | Measure COVID-19's impact on developer activity | Temporal analysis of 2019–2021 data segmented by work hours, weekends, and COVID period flags |
| 4 | Assess repository health | Composite health score (0–100) from PR merge time, issue close time, merge rate, and popularity |
| 5 | Predict repository abandonment risk | Random Forest classifier with 28 features, scoring all repositories with risk tiers |
| 6 | Identify repository growth trajectories | K-Means clustering on 10-year activity time series to label repos as Viral, Established, Stable, Declining, Dormant, or Inactive |
| 7 | Forecast language market share for 2026–2027 | Per-language linear regression on historical market share percentages |
| 8 | Analyze contributor dynamics | Contributor growth, retention, and bus factor (concentration of development ownership) |

---

## 3. Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Cloud Platform** | Google Cloud Platform (GCP) | Hosting all services |
| **Data Processing** | Apache Spark 3.x on Cloud Dataproc 2.1 | Distributed ETL and ML |
| **Data Storage** | Google Cloud Storage (GCS) + Delta Lake 2.3 | ACID-compliant lakehouse |
| **Data Warehouse** | BigQuery | SQL analytics and BI serving layer |
| **Orchestration** | Cloud Workflows + Cloud Scheduler | Pipeline scheduling and coordination |
| **Event Streaming** | Cloud Pub/Sub | Event-driven file processing |
| **Secrets** | Secret Manager | GitHub PAT storage |
| **Monitoring** | Cloud Monitoring | Failure alerts |
| **Visualization** | Power BI Service (DirectQuery) | Interactive dashboards |
| **Infrastructure** | Terraform >= 1.6 | Infrastructure as Code |
| **Version Control** | Git / GitHub | Source code management |

---

## 4. System Architecture

```
  DATA SOURCES                       INGESTION
 ┌──────────────┐                 ┌────────────────┐
 │  GH Archive  │── fetch_data   │   GCS Raw      │
 │  (2016-2025) │──  .py ───────▶│   Bucket       │──── GCS Notification ────┐
 │  60 .gz files│                │  gs://raw/     │                          │
 └──────────────┘                └────────────────┘                          │
 ┌──────────────┐                ┌────────────────┐                          ▼
 │  GitHub API  │                │ upload_to_gcs  │                  ┌──────────────┐
 │  REST v3     │                │    .py         │──────────────────│   Pub/Sub    │
 │  (4 PATs)    │                └────────────────┘                  │   Topic      │
 └──────┬───────┘                                                    └──────┬───────┘
        │                                                                   │
        │                                                                   ▼
        │                  ORCHESTRATION                             ┌──────────────┐
        │          ┌─────────────────────────────┐                  │   Pub/Sub    │
        │          │      Cloud Scheduler        │                  │ Subscription │
        │          │  ┌────────────┬───────────┐ │                  └──────┬───────┘
        │          │  │Daily 03:00 │Monthly 1st│ │                         │
        │          │  └─────┬──────┴─────┬─────┘ │                         │
        │          └────────┼────────────┼───────┘                         │
        │                   ▼            ▼                                 │
        │          ┌──────────────────────────────────────────────┐        │
        │          │           Cloud Workflows                    │◀───────┘
        │          │                                              │
        │          │  BATCH (Daily)        MONTHLY (1st)          │
        │          │  ┌─────────────┐      ┌───────────────┐     │
        │          │  │1.Pull PubSub│      │1.ML Train     │     │
        │          │  │2.Silver     │      │2.ML Trajectory│     │
        │          │  │3.API Enrich │      │3.ML Lang Trend│     │
        │          │  │4.Gold       │      │4.ML Score     │     │
        │          │  │5.ML Score   │      └───────────────┘     │
        │          │  └─────────────┘                             │
        │          └─────────────────────┬────────────────────────┘
        │                                │
        │                                ▼
        │                   ┌─────────────────────────┐
        │                   │    Dataproc Cluster      │
        │                   │  (1 master + 2 workers)  │
        │                   │    Apache Spark 3.x      │
        │                   │    Delta Lake 2.3        │
        │                   └─────────────────────────┘
        │                                │
        ▼                                ▼
 ┌─────────────────── MEDALLION ARCHITECTURE (Delta Lake) ───────────────────┐
 │                                                                           │
 │  RAW LAYER              SILVER LAYER              GOLD LAYER              │
 │  gs://raw/              gs://silver/              gs://gold/              │
 │ ┌───────────────┐     ┌───────────────┐        ┌───────────────────┐     │
 │ │ GH Archive    │     │ push_events   │        │ DIMENSIONS        │     │
 │ │  .json.gz     │────▶│ pr_events     │───────▶│  dim_date         │     │
 │ │               │     │ issues_events │        │  dim_repository   │     │
 │ │ GitHub API    │     │ watch_events  │        │  dim_language     │     │
 │ │  enrichment   │────▶│ fork_events   │        │  dim_contributor  │     │
 │ │               │     │ create_events │        │                   │     │
 │ └───────────────┘     │ release_events│        │ FACTS             │     │
 │                       │ repositories  │        │  fact_repo_daily  │     │
 │                       │ contributors  │        │  fact_push_events │     │
 │                       └───────────────┘        │  fact_pr_events   │     │
 │                                                │  fact_issues      │     │
 │                                                │  fact_releases    │     │
 │                                                │  fact_lang_yearly │     │
 │                                                │                   │     │
 │                                                │ ML OUTPUTS        │     │
 │                                                │  ml_model_current │     │
 │                                                │  ml_abandonment   │     │
 │                                                │  ml_trajectories  │     │
 │                                                │  ml_lang_forecast │     │
 │                                                └───────────────────┘     │
 └─────────────────────────────────────────────────────┬─────────────────────┘
                                                       │
                              Spark-BigQuery Connector  │
                                                       ▼
                                            ┌─────────────────────┐
                                            │     BigQuery        │
                                            │  github_oss_gold    │
                                            │  15 Analysis Views  │
                                            └─────────┬───────────┘
                                                      │ DirectQuery
                                                      ▼
                                            ┌─────────────────────┐
                                            │     Power BI        │
                                            │   9 Dashboards      │
                                            └─────────────────────┘
```

---

## 5. Data Sources & Ingestion

### 5.1 GH Archive

[GH Archive](https://www.gharchive.org/) records all public GitHub events as hourly JSON archives. The project downloads a curated subset covering **10 years (2016–2025)**.

**Selection Strategy:**
- **1 mid-week day per year** selected for representative activity
- **6 peak hours per day** (14:00–19:00 UTC) — covers US afternoon and European evening
- **Total: 60 compressed JSON files (~2.5 GB)**

**Event Types Captured (7):**

| Event Type | Description |
|-----------|-------------|
| PushEvent | Code pushes with commit counts |
| PullRequestEvent | PR opens, closes, merges with line-level metrics |
| IssuesEvent | Issue opens, closes with labels and comments |
| WatchEvent | Repository stars (action=started) |
| ForkEvent | Repository forks with fork metadata |
| CreateEvent | Branch/tag/repository creation events |
| ReleaseEvent | Software releases with version tags |

**Download Process (`fetch_data.py`):**
1. Constructs GH Archive URLs for selected date-hour combinations
2. Downloads `.json.gz` files with retry logic (3 attempts, 5-second backoff, 60-second timeout)
3. Filters to keep only the 7 relevant event types with minimal payload fields
4. Stores in `data/gharchive_filtered/` partitioned by year

### 5.2 Upload to GCS

The `upload_to_gcs.py` script uploads filtered files to the raw GCS bucket:

```bash
python scripts/upload_to_gcs.py --bucket ghoss-raw
```

- Idempotent: skips files that already exist in the bucket
- Each upload triggers a **GCS finalize notification** → Pub/Sub → batch pipeline
- Target path: `gs://{bucket}/gharchive/{year}/{filename}.json.gz`

---

## 6. Data Pipeline — Medallion Architecture

The system follows a three-layer **Medallion Architecture** using Delta Lake for ACID transactions, schema enforcement, and time travel capabilities.

### 6.1 Raw Layer

**Storage:** `gs://{prefix}-raw/`

Contains unprocessed GH Archive JSON files and GitHub API enrichment data exactly as ingested. The raw layer serves as an immutable audit trail.

**Lifecycle Policy:**
- Transitions to COLDLINE storage after **90 days**
- Auto-deleted after **365 days**

### 6.2 Silver Layer

**Storage:** `gs://{prefix}-silver/`  
**Script:** `dataproc/silver_layer.py`

The silver layer performs cleaning, validation, schema enforcement, and deduplication on raw data. Each event type produces a dedicated Delta table partitioned by year.

**Processing Steps:**

1. **Schema Inference:** Reads raw JSON with predefined schemas for nested structures (actor, repo, payload)
2. **Field Extraction:** Flattens nested JSON — `actor.id` → `actor_id`, `actor.login` → `actor_login`, etc.
3. **Validation Filters:**
   - `actor_id` IS NOT NULL
   - `repo_id` IS NOT NULL
   - `created_at` IS NOT NULL
   - Date range: 2008-01-01 to current timestamp
4. **Deduplication:** By `event_id` across all files
5. **Null Handling:** Defaults applied per event type (e.g., additions=0, deletions=0 for PRs; comments=0 for issues)
6. **Partitioning:** By `year` column extracted from timestamp
7. **Write Strategy:** Delta MERGE with retry logic (5 retries, exponential backoff for ConcurrentAppendException)

**Silver Tables:**

| Table | Key Columns | Source |
|-------|------------|--------|
| `push_events` | event_id, repo_id, actor_id, commit_count, branch_ref | GH Archive |
| `pr_events` | event_id, repo_id, actor_id, action, state, is_merged, additions, deletions, changed_files | GH Archive |
| `issues_events` | event_id, repo_id, actor_id, action, state, comments_count, label_list | GH Archive |
| `watch_events` | event_id, repo_id, actor_id (stars only, action=started) | GH Archive |
| `fork_events` | event_id, repo_id, actor_id, fork_id, fork_name | GH Archive |
| `create_events` | event_id, repo_id, actor_id, ref_type, description | GH Archive |
| `release_events` | event_id, repo_id, actor_id, tag_name, is_prerelease, is_draft | GH Archive |
| `repositories` | repo_id, repo_name, language, license, stars, forks, topics, is_archived | GitHub API |
| `contributors` | actor_id, login, account_type | Aggregated from events |

**Data Quality Audit:**
- On `full_refresh` mode, a `dq_audit` table records raw_count vs. accepted_count per event type
- Delta VACUUM with 168-hour retention applied to all tables

### 6.3 Gold Layer — Dimensional Model

**Storage:** `gs://{prefix}-gold/`  
**Script:** `dataproc/gold_layer.py`

The gold layer implements a **star schema** dimensional model optimized for analytical queries.

#### Dimension Tables

**dim_date**
| Column | Type | Description |
|--------|------|-------------|
| date_key | int | Surrogate key (format: yyyyMMddHH) |
| full_date | date | Calendar date |
| year | short | Year (2016–2025) |
| quarter | byte | Quarter (1–4) |
| month | byte | Month (1–12) |
| month_name | string | Full month name |
| day_of_week | byte | 1=Sunday through 7=Saturday |
| day_name | string | Full day name |
| hour | byte | Hour (0–23) |
| is_weekend | boolean | Saturday or Sunday |
| is_covid_period | boolean | 2020-03-01 to 2021-06-30 |

**dim_repository**
| Column | Type | Description |
|--------|------|-------------|
| repo_key | long | Surrogate key |
| repo_id | long | GitHub repository ID |
| repo_name | string | Full name (owner/repo) |
| owner_login | string | Extracted from repo_name |
| language | string | Primary language (default: "Unknown") |
| license | string | SPDX license ID |
| topics | string | Comma-separated topic list |
| stars_at_extract | int | Star count at API extraction time |
| forks_at_extract | int | Fork count at API extraction time |
| is_ai_ml_repo | boolean | Pattern match on name/topics |
| is_archived | boolean | Whether repo is archived |
| has_readme | boolean | README file detected |
| has_ci | boolean | CI/CD configuration detected |
| repo_age_days | int | Days since creation |
| created_at | timestamp | Repository creation date |

**AI/ML Detection Pattern:** `ai|ml|llm|gpt|deep.learning|machine.learning|neural|transformer|diffusion|langchain` (case-insensitive match on repo name and topics)

**dim_language**
| Column | Type | Description |
|--------|------|-------------|
| language_key | int | Surrogate key |
| language_name | string | Language name |
| paradigm | string | OOP, functional, multi-paradigm, etc. |
| type_system | string | static, dynamic, unknown |
| first_appeared | int | Year of first release |
| is_emerging | boolean | Emerging language flag |

Hardcoded metadata for 16 major languages (Python, JavaScript, TypeScript, Java, Go, Rust, C++, C, Ruby, PHP, Swift, Kotlin, Scala, Shell, Jupyter Notebook, Unknown) with dynamic discovery from silver/repositories.

**dim_contributor**
| Column | Type | Description |
|--------|------|-------------|
| contributor_key | long | Surrogate key |
| actor_id | long | GitHub user ID |
| login | string | GitHub username |
| is_bot | boolean | Pattern: `[bot]`, `bot$`, `github.actions` |
| account_type | string | "User" (default) |

#### Fact Tables

**fact_push_events** — Individual push event records
- Merge key: `event_id` | Partitioned by: `year`
- Measures: `commit_count`, `is_default_branch`
- Foreign keys: `date_key`, `repo_key`, `contributor_key`, `language_key`

**fact_pr_events** — Pull request lifecycle events
- Merge key: `event_id` | Partitioned by: `year`
- Measures: `additions`, `deletions`, `changed_files`, `review_comments`, `time_to_merge_hours`
- Derived: `pr_size_bucket` (XS ≤10, S ≤50, M ≤250, L ≤1000, XL >1000 lines)

**fact_issues_events** — Issue lifecycle events
- Merge key: `event_id` | Partitioned by: `year`
- Measures: `comments_count`, `time_to_close_hours`, `label_list`

**fact_release_events** — Software release events
- Merge key: `event_id` | Partitioned by: `year`
- Measures: `is_prerelease`, `is_draft`, `tag_name`

**fact_repo_activity_daily** — Aggregated daily metrics per repository
- Merge key: [`date_key`, `repo_key`] | Partitioned by: `year`
- Measures:

| Metric | Description |
|--------|-------------|
| total_pushes | Count of push events |
| total_commits | Sum of commit counts |
| unique_contributors | Count of distinct contributors |
| total_prs_opened | PRs with action=opened |
| total_prs_merged | PRs where is_merged=true |
| avg_pr_merge_hours | Average time to merge |
| total_issues_opened | Issues with action=opened |
| total_issues_closed | Issues with state=closed |
| avg_issue_close_hours | Average time to close |
| total_stars | Watch events (stars) |
| total_forks | Fork events |
| total_releases | Release events |

**fact_language_yearly** — Annual language-level aggregates
- Merge key: [`language_key`, `year`]
- Measures: total_push_events, total_commits, unique_repos, unique_contributors, total_pr_events, total_prs_merged, total_stars, total_forks, yoy_push_growth_pct, market_share_pct

#### BigQuery Sync

All 10 gold tables (4 dimensions + 6 facts) are written to BigQuery via the **Spark-BigQuery connector** on every pipeline run. Each record is tagged with `_pipeline_run_id` (UUID) and `_loaded_at` (timestamp) for lineage tracking.

---

## 7. GitHub API Enrichment

**Script:** `dataproc/github_api_ingestion.py`

The pipeline enriches repository metadata beyond what GH Archive provides by calling the GitHub REST API v3.

### API Endpoints

| Endpoint | Data Collected |
|----------|---------------|
| `GET /repos/{owner}/{repo}` | Stars, forks, language, license, description, archived status, creation date |
| `GET /repos/{owner}/{repo}/topics` | Repository topics (used for AI/ML detection) |
| `GET /rate_limit` | Quota monitoring (checked every 200 repos) |

### Repository Selection

- Aggregates event counts across all silver tables for a given year
- Selects the **top 40%** of repositories by activity
- Capped at **4,000 repositories** per run for cost control
- Incremental mode: only fetches repos not already in the silver `repositories` table

### Rate Limit Management

The system uses **4 GitHub Personal Access Tokens** in round-robin rotation:

| Mechanism | Details |
|-----------|---------|
| Round-robin assignment | `token_idx = i % num_tokens` |
| Per-token hard limit | 4,800 calls max (below GitHub's 5,000/hr limit) |
| Runtime limit | 26,100 seconds (435 min), reserving 45 min for writes |
| Quota monitoring | Every 200 repos, checks remaining quota via `/rate_limit` |
| Minimum remaining | 100 calls per token before rotation |
| Secondary rate limit (403/429) | Checks Retry-After header; rotates tokens; exponential backoff |
| Primary rate limit exhaustion | Waits until reset time + 5 seconds |
| Server errors (5xx) | Exponential backoff: `(2^attempt) * 5` seconds |

**Effective throughput:** ~20,000 API calls/hour (4 tokens × 5,000/hr each).

---

## 8. Machine Learning Models

### 8.1 Abandonment Risk Prediction (Random Forest)

**Script:** `dataproc/ml_train.py`  
**Schedule:** Monthly (1st of each month)

**Objective:** Predict which repositories are at risk of being abandoned based on early activity patterns and metadata.

#### Label Definition

| Label | Condition |
|-------|-----------|
| 1 (Abandoned) | `is_archived = true` OR (`repo_age_days ≥ 730` AND `last_active_year < CURRENT_YEAR - 2`) |
| 0 (Active) | All other repos with `repo_age_days ≥ 730` |
| Excluded | Repos younger than 730 days (2 years) — too young to label |

#### Feature Engineering (`ml_features.py`)

**28 total input features** organized into three categories:

**Numerical Features (23):**

| # | Feature | Description |
|---|---------|-------------|
| 1 | early_commits | Sum of commits in first 90 days |
| 2 | early_pushes | Count of push events in first 90 days |
| 3 | early_stars | Count of star events in first 90 days |
| 4 | early_forks | Count of fork events in first 90 days |
| 5 | early_prs_opened | PRs opened in first 90 days |
| 6 | early_prs_merged | PRs merged in first 90 days |
| 7 | early_issues | Issues opened in first 90 days |
| 8 | avg_daily_contributors | Average unique contributors/day (first 90 days) |
| 9 | active_days_in_90 | Days with any activity (first 90 days) |
| 10 | unique_branches | Distinct branch refs (first 90 days) |
| 11 | default_branch_push_ratio | Proportion of pushes to main/master |
| 12 | avg_pr_additions | Average lines added per PR |
| 13 | avg_review_comments | Average review comments per PR |
| 14 | total_unique_contributors | Distinct non-bot contributors (first 90 days) |
| 15 | pr_merge_rate | merged_prs / (opened_prs + 1) |
| 16 | fork_to_star_ratio | forks / (stars + 1) |
| 17 | commits_per_active_day | commits / (active_days + 1) |
| 18 | repo_age_days | Days since repository creation |
| 19 | created_year | Year of repository creation |
| 20 | commit_momentum | avg_commits_recent / (avg_commits_early + 1.0) |
| 21 | avg_commits_early | Average daily commits (year ≤ 2020) |
| 22 | avg_commits_recent | Average daily commits (year > 2020) |
| 23 | years_observed | Count of distinct years with activity |

**Categorical Features (2):** `language`, `license`

**Binary Features (3):** `is_ai_ml_repo`, `has_readme`, `has_ci`

#### Model Pipeline

```
Imputer (median) → StringIndexer (language, license)
    → OneHotEncoder → VectorAssembler → StandardScaler
    → RandomForestClassifier (weighted)
```

#### Hyperparameter Tuning

| Parameter | Search Space |
|-----------|-------------|
| numTrees | [50, 100, 200] |
| maxDepth | [5, 8, 12] |
| minInstancesPerNode | 5 (fixed) |
| featureSubsetStrategy | "sqrt" (fixed) |

- **Cross-Validation:** 3-fold with `BinaryClassificationEvaluator` (AUC-ROC)
- **Class Imbalance Handling:** Automatic weight ratio `count(active) / count(abandoned)`
- **Temporal Split:** Train on repos created ≤ CURRENT_YEAR-4; Test on repos created CURRENT_YEAR-3 to CURRENT_YEAR-2

#### Evaluation Metrics
- AUC-ROC (primary, used for model selection)
- F1-Score (macro)
- Accuracy

#### Model Versioning
- Saved as: `gs://{prefix}-gold/ml_model_{YYYYMM}`
- Pointer table: `ml_model_current` (model_version, model_path, run_id, trained_at)
- Metadata table: `ml_model_metadata` (metrics, hyperparameters, row counts)
- Feature importance: `ml_feature_importance` (feature_name, importance score, model_version)

### 8.2 Repository Trajectory Clustering (K-Means)

**Script:** `dataproc/ml_trajectory.py`  
**Schedule:** Monthly (parallel with training)

**Objective:** Group repositories by their 10-year activity patterns to identify growth, decline, and revival trajectories.

#### Method

1. **Feature Vector:** Yearly commit counts from 2016–2025 pivoted into a 10-column vector per repo
2. **Filtering:** Repos with ≥ 2 years of observed data
3. **Scaling:** MinMaxScaler (0–1 range)
4. **Optimal K Selection:** Silhouette analysis for k = 2 to min(6, n_repos-1)
5. **Clustering:** K-Means with maxIter=50, seed=42, squaredEuclidean distance

#### Cluster Interpretation

Clusters are ranked by `growth_ratio = avg_commits_recent / (avg_commits_early + 1.0)`:

| Rank | Label | Interpretation |
|------|-------|---------------|
| 1 (highest growth) | Viral | Rapid recent growth |
| 2 | Established | Strong sustained activity |
| 3 | Stable | Consistent steady activity |
| 4 | Declining | Decreasing activity |
| 5 | Dormant | Minimal recent activity |
| 6 | Inactive | Near-zero activity |

#### Output
- Delta table: `gs://{prefix}-gold/ml_repo_trajectories`
- Columns: repo_key, cluster_id, trajectory_label, growth_ratio, avg_commits_early, avg_commits_recent, years_observed, computed_at

### 8.3 Language Trend Forecasting (Linear Regression)

**Script:** `dataproc/ml_language_trends.py`  
**Schedule:** Monthly (parallel with training)

**Objective:** Model the trajectory of each programming language's market share and forecast 2026–2027.

#### Method

- **Input:** `fact_language_yearly` (market_share_pct by year)
- **Filter:** Languages with ≥ 3 years of data
- **Model:** Per-language linear regression (scikit-learn `LinearRegression` via `applyInPandas`)
- **X variable:** Year | **Y variable:** market_share_pct

#### Output Metrics Per Language

| Metric | Description |
|--------|-------------|
| slope_pct_per_year | Annual change in market share (can be negative) |
| r_squared | Goodness of fit (0–1) |
| forecast_2026_share | Predicted 2026 market share (clipped at 0.0) |
| forecast_2027_share | Predicted 2027 market share |
| trend_label | Strongly Growing (>0.5), Growing (>0.1), Stable (±0.1), Declining (>-0.5), Strongly Declining (≤-0.5) |

### 8.4 ML Scoring Pipeline

**Script:** `dataproc/ml_score.py`  
**Schedule:** Every pipeline trigger (daily batch + monthly)

The scoring pipeline applies the latest trained model to **all repositories** in the gold layer.

#### Scoring Process

1. Load model pointer from `ml_model_current`
2. Load saved PipelineModel
3. Build identical features via shared `ml_features.py`
4. Transform all repos through the model pipeline
5. Extract abandonment probability and assign risk tiers

#### Risk Tier Assignment

| Tier | Condition |
|------|-----------|
| High Risk | abandonment_probability ≥ 0.70 |
| Moderate Risk | 0.40 ≤ probability < 0.70 |
| Low Risk | probability < 0.40 and repo_age ≥ 730 days |
| Too Young | repo_age_days < 730 (insufficient history) |

#### Trajectory Labels (from momentum feature)

| Label | Condition |
|-------|-----------|
| Accelerating | commit_momentum ≥ 1.5 |
| Stable | 0.5 ≤ momentum < 1.5 |
| Declining | 0.1 ≤ momentum < 0.5 |
| Stalled | momentum < 0.1 |

**Output:** `ml_abandonment_risk` table (Delta + BigQuery), partitioned by `created_year`, with idempotent upsert on `(repo_key, model_version)`.

**Graceful Degradation:** If no trained model exists yet (e.g., daily batches run before the first monthly training), the scoring step is a no-op.

---

## 9. Workflow Orchestration

### 9.1 Daily Batch Pipeline

**Workflow:** `workflows/batch_pipeline.yaml`  
**Trigger:** Cloud Scheduler — `0 3 * * *` (daily at 03:00 UTC)

```
Cloud Scheduler (03:00 UTC)
    │
    ▼
Cloud Workflows: batch_pipeline
    │
    ├── [1] Pull Pub/Sub messages (max 50)
    │       └── If empty → EXIT (no cluster created, no cost)
    │
    ├── [2] Acknowledge messages (before cluster creation)
    │
    ├── [3] Create Dataproc cluster
    │       └── Poll every 60s, max 15 polls (15-min timeout)
    │
    ├── [4] For each file (sequential):
    │       └── silver_layer.py --triggered_file <path> --run_mode incremental
    │
    ├── [5] github_api_ingestion.py --top_pct 40
    │
    ├── [6] gold_layer.py (star schema build + BigQuery sync)
    │
    ├── [7] ml_score.py (score with latest model)
    │
    ├── [8] Delete cluster (guaranteed via try-except)
    │
    └── Return: {status: complete, files_processed: N}
```

**Key Design:** If the Pub/Sub queue is empty, the workflow exits immediately — no cluster is created, incurring zero compute cost.

### 9.2 Monthly ML Training Pipeline

**Workflow:** `workflows/monthly_pipeline.yaml`  
**Trigger:** Cloud Scheduler — `0 2 1 * *` (1st of month at 02:00 UTC)

```
Cloud Scheduler (02:00 UTC, 1st of month)
    │
    ▼
Cloud Workflows: monthly_pipeline
    │
    ├── [1] Create Dataproc cluster
    │
    ├── [2] Run in PARALLEL:
    │       ├── ml_train.py (Random Forest training)
    │       ├── ml_trajectory.py (K-Means clustering)
    │       └── ml_language_trends.py (Linear trends)
    │
    ├── [3] ml_score.py (with freshly trained model)
    │
    ├── [4] Delete cluster (guaranteed cleanup)
    │
    └── Return: {status: monthly_ml_training_complete}
```

### Dataproc Cluster Specifications

| Parameter | Value |
|-----------|-------|
| Master nodes | 1 × e2-standard-2 |
| Worker nodes | 2 × e2-standard-2 |
| Boot disk | 100 GB per node |
| Image | 2.1-debian11 |
| Idle delete TTL | 1,800s (30 min) |
| Auto delete TTL | 86,400s (24 hours) |
| Init script timeout | 600s |
| Software | Delta Lake 2.3, Spark 3.x with SQL extensions |

### Job Timeouts

| Pipeline | Polling Interval | Max Polls | Total Timeout |
|----------|-----------------|-----------|---------------|
| Batch (daily) | 30 seconds | 960 | 8 hours |
| Monthly | 30 seconds | 720 | 6 hours |
| Cluster creation | 60 seconds | 15 | 15 minutes |

---

## 10. BigQuery Analytics Layer

All gold tables are synced to BigQuery dataset `github_oss_gold`. On top of these, **15 analysis views** are created for Power BI consumption.

### Core Analysis Views

**1. analysis_language_evolution**
- Tracks programming language market share from 2016–2025
- Metrics: push events, commits, unique repos, unique contributors, market_share_pct, yoy_growth_pct
- Source: `fact_language_yearly` JOIN `dim_language`

**2. analysis_ai_boom**
- Compares AI/ML repos vs. non-AI repos year-over-year
- Metrics: unique repos, total pushes, stars, forks, PRs, ai_activity_share_pct
- Source: `fact_repo_activity_daily` JOIN `dim_date` JOIN `dim_repository`

**3. analysis_covid_impact**
- Developer activity during 2019–2021 segmented by work patterns
- Dimensions: year, hour, is_covid_period, is_weekend
- Metrics: pushes, PRs, issues, unique repos

**4. analysis_repo_health**
- Composite health score (0–100) from four equally weighted components (25 points each):
  - **PR merge speed:** Faster merge → higher score (max 168 hours baseline)
  - **Issue close speed:** Faster close → higher score (max 720 hours baseline)
  - **Merge rate:** Higher merged/opened ratio → higher score
  - **Popularity:** log(stars + forks) normalized

**5. analysis_pr_lifecycle**
- PR metrics by year, language, and size bucket
- Metrics: merge rate, average merge hours, average review comments, average lines changed

**6. analysis_bus_factor**
- Contributor concentration analysis per repository
- Metrics: total contributors, top-1 contributor % of pushes, top-3 %, top-5 %
- Filter: repos with ≥ 3 contributors

**7. analysis_ecosystem_growth**
- Year-over-year platform growth metrics
- Metrics: unique repos, total contributors, pushes, PRs, issues, stars, forks, pr_to_push_ratio

**8. analysis_hourly_heatmap**
- Activity patterns by hour and day of week
- Metrics: total pushes, total PRs, active repos
- Purpose: Identify peak development hours globally

### ML-Specific Views

**9. analysis_ml_predictions**
- Full prediction output with confidence tiers: Very High (80%+), High (60–80%), Moderate (40–60%), Low (20–40%), Very Low (<20%)

**10. analysis_ml_by_language**
- Abandonment risk aggregated by language (min 10 repos per language)
- Metrics: total repos, high/moderate/low risk counts, avg_abandonment_prob_pct

**11. analysis_ml_feature_importance**
- Model feature importance with percentage contribution and rank per model version

**12. analysis_abandonment_risk_current**
- Latest model version only — full repo details with risk tier and trajectory label

**13. analysis_abandonment_by_language**
- Current-model risk distribution by programming language

**14. analysis_abandonment_trend**
- Risk tier distribution across model versions (tracks how risk changes over time)

**15. analysis_repo_trajectories**
- K-Means cluster assignments with growth ratios and repo metadata

---

## 11. Power BI Dashboards

**Connection:** Power BI Service → BigQuery (DirectQuery mode via native connector)

| # | Dashboard | Key Insights |
|---|-----------|-------------|
| 1 | Programming Language Evolution | 10-year language adoption trends, market share shifts |
| 2 | AI/ML Boom Impact | Surge in AI-related repos 2022–2025, activity share growth |
| 3 | COVID-19 Developer Impact | Shift to after-hours coding during 2020–2021, weekend activity spike |
| 4 | Repository Health Scorecard | Per-repo health scores, identification of well-maintained projects |
| 5 | Contributor Growth & Retention | New contributor onboarding trends, retention patterns |
| 6 | Abandoned Repository Alerts | High-risk repos with abandonment probability scores |
| 7 | Language Market Forecast | 2026–2027 predicted market share per language |
| 8 | Repository Trajectory Clusters | Visual grouping of repo activity patterns (viral, stable, declining, etc.) |
| 9 | API Enrichment Summary | GitHub API metadata insights: README presence, licenses, topics |

---

## 12. Infrastructure as Code (Terraform)

All cloud resources are provisioned via Terraform, ensuring reproducibility and version control.

### GCP APIs Enabled (9)

| API | Purpose |
|-----|---------|
| dataproc.googleapis.com | Spark cluster management |
| storage.googleapis.com | GCS bucket operations |
| bigquery.googleapis.com | Data warehouse |
| secretmanager.googleapis.com | GitHub PAT storage |
| workflows.googleapis.com | Pipeline orchestration |
| cloudscheduler.googleapis.com | Cron-based triggers |
| cloudresourcemanager.googleapis.com | Project metadata |
| iam.googleapis.com | Service account management |
| pubsub.googleapis.com | Event messaging |

### GCS Buckets (4)

| Bucket | Purpose | Lifecycle |
|--------|---------|-----------|
| `{prefix}-raw` | Raw GH Archive files | COLDLINE at 90d, delete at 365d |
| `{prefix}-silver` | Cleaned Delta tables | Standard (no lifecycle) |
| `{prefix}-gold` | Star schema + ML models | Standard (no lifecycle) |
| `{prefix}-scripts` | PySpark job scripts | Standard (no lifecycle) |

All buckets: uniform bucket-level access, force_destroy=true, labeled.

### Pub/Sub Configuration

| Resource | Configuration |
|----------|--------------|
| Topic: `{prefix}-raw-uploads` | Receives GCS finalize events for `gharchive/` prefix |
| Subscription | Ack deadline: 600s, retention: 7 days, never expires |
| Dead Letter Queue | Max 5 delivery attempts → DLQ topic, 14-day retention |
| DLQ Subscription | For investigating permanently failed messages |

### Terraform Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `project_id` | string | (required) | GCP project ID |
| `region` | string | us-central1 | GCP region |
| `prefix` | string | ghoss | Resource name prefix (max 8 chars, lowercase alphanumeric) |
| `github_pat` through `github_pat_4` | string (sensitive) | (required) | 4 GitHub PATs for API round-robin |
| `bq_dataset` | string | github_oss_gold | BigQuery dataset name |
| `alert_email` | string | "" | Alert recipient (optional) |
| `labels` | map(string) | project=github-oss-analytics, course=damg7370, environment=dev, managed_by=terraform | Resource labels |

### Deployment Order

```
1. Enable GCP APIs
2. Create GCS buckets (raw, silver, gold, scripts)
3. Store GitHub PATs in Secret Manager
4. Create service accounts + IAM bindings
5. Create Pub/Sub topic/subscription + GCS notifications
6. Create BigQuery dataset
7. Upload PySpark scripts to scripts bucket
8. Create Cloud Workflows (batch + monthly)
9. Create Cloud Scheduler jobs
10. Create monitoring alerts
```

---

## 13. Security & Access Control

### Service Accounts (Principle of Least Privilege)

**Dataproc Service Account (`{prefix}-dataproc`):**
| Role | Scope |
|------|-------|
| roles/storage.objectAdmin | Read/write all GCS buckets |
| roles/secretmanager.secretAccessor | Read GitHub PATs |
| roles/bigquery.dataEditor | Write to BigQuery tables |
| roles/bigquery.jobUser | Execute BigQuery jobs |
| roles/dataproc.worker | Internal job execution |

**Workflows Service Account (`{prefix}-workflows`):**
| Role | Scope |
|------|-------|
| roles/dataproc.editor | Create/delete clusters, submit jobs |
| roles/workflows.invoker | Execute workflow definitions |
| roles/pubsub.subscriber | Pull and acknowledge messages |
| roles/iam.serviceAccountUser | Act as dataproc SA for cluster creation |

**GCS Service Account (auto-managed):**
| Role | Scope |
|------|-------|
| roles/pubsub.publisher | Publish GCS finalize events to Pub/Sub |

### Secrets Management

4 GitHub Personal Access Tokens stored in **Secret Manager**, accessed only by the Dataproc SA at runtime. Tokens are never stored in code or Terraform state files (marked as `sensitive`).

---

## 14. Monitoring & Alerting

| Alert | Trigger | Auto-Close |
|-------|---------|------------|
| Workflow Execution Failure | Any Cloud Workflow finishes with status=FAILED | 30 minutes |
| Dataproc Job Failure | Any Dataproc job completes with state=ERROR | 30 minutes |

- **Notification Channel:** Email (configurable via `alert_email` variable)
- **Metric Alignment:** 300-second (5-minute) aggregation windows
- **Condition:** Threshold > 0 failed executions in any 5-minute window
- **Only created** if `alert_email` is non-empty

---

## 15. Cost Analysis

| Service | Monthly Usage | Estimated Cost |
|---------|-------------|----------------|
| GCS Storage (raw/silver/gold) | ~4 GB after lifecycle | ~$1 |
| Dataproc Clusters | ~4 hours total (ephemeral) | ~$10 |
| Cloud Workflows | ~8 executions/month | < $0.10 |
| Cloud Scheduler | 2 jobs (free tier) | $0 |
| BigQuery | ~1 GB scanned (DirectQuery) | ~$0.01 |
| Secret Manager | 4 secrets | ~$0.10 |
| Pub/Sub | Minimal throughput | < $0.01 |
| Cloud Monitoring | 2 alert policies | $0 |
| **Monthly Total** | | **~$11–15** |

**Key cost optimization:** Dataproc clusters are **ephemeral** — created on-demand per pipeline run and deleted immediately after. If the Pub/Sub queue is empty, no cluster is created at all.

---

## 16. Key Architectural Decisions

| Decision | Rationale |
|----------|-----------|
| **Delta Lake over Parquet** | ACID transactions prevent partial writes; schema enforcement catches data drift; time travel enables debugging |
| **Medallion Architecture (Raw → Silver → Gold)** | Clear separation of concerns; enables independent reprocessing of any layer without data loss |
| **Incremental + Full Refresh modes** | Daily batches process individual files incrementally; monthly runs recompute ML models on complete data |
| **Pub/Sub + Cloud Scheduler hybrid** | Pub/Sub queues uploads for event-driven processing; Scheduler ensures periodic execution catches any missed messages |
| **Ephemeral Dataproc clusters** | Zero idle cost — clusters only exist during active processing (~30 min per run) |
| **Service account separation** | Dataproc SA has data access; Workflows SA has orchestration access — neither has permissions beyond its role |
| **4-PAT round-robin for GitHub API** | Achieves ~20K requests/hour (4× single token), enabling richer enrichment within rate limits |
| **Separate monthly ML pipeline** | ML training requires full dataset context; isolating it from daily batches ensures stable model quality |
| **Model versioning with pointer table** | `ml_model_current` enables ml_score.py to always find the latest model without path hardcoding |
| **Shared `ml_features.py`** | Single feature engineering module used by both training and scoring prevents train/serve skew |
| **Graceful degradation** | ml_score.py is a no-op if no model exists; daily pipeline works from day one before first monthly training |
| **Pre-downloaded Delta JARs** | Cluster init script downloads JARs upfront, avoiding transient Maven Central failures during job submission |

---

## Appendix: File Structure

```
github_ecosystem_analytics/
├── report.md                          # This report
├── README.md                          # Deployment guide
├── fetch_data.py                      # Download GH Archive files
├── terraform/                         # Infrastructure as Code
│   ├── main.tf                        # APIs, GCS buckets
│   ├── variables.tf                   # All variables with defaults
│   ├── secrets.tf                     # Secret Manager (4 GitHub PATs)
│   ├── iam.tf                         # Service accounts & IAM roles
│   ├── pubsub.tf                      # Pub/Sub + DLQ + GCS notifications
│   ├── bigquery.tf                    # BigQuery dataset
│   ├── monitoring.tf                  # Alert policies
│   ├── workflows.tf                   # Cloud Workflows + Cloud Scheduler
│   ├── scripts_upload.tf              # Upload PySpark scripts to GCS
│   └── output.tf                      # Terraform outputs
├── workflows/                         # Cloud Workflows YAML
│   ├── batch_pipeline.yaml            # Daily: silver → API → gold → BQ → score
│   └── monthly_pipeline.yaml          # Monthly: train + trajectory + trends → score
├── dataproc/                          # PySpark job scripts
│   ├── silver_layer.py                # Raw JSON → silver Delta tables
│   ├── gold_layer.py                  # Silver → gold star schema + BQ sync
│   ├── github_api_ingestion.py        # GitHub API enrichment
│   ├── ml_features.py                 # Shared feature engineering (28 features)
│   ├── ml_train.py                    # Random Forest abandonment model
│   ├── ml_score.py                    # Score repos with trained model
│   ├── ml_trajectory.py              # K-Means trajectory clustering
│   ├── ml_language_trends.py          # Per-language linear trends
│   └── init_cluster.sh               # Dataproc init (Delta JARs + packages)
├── scripts/                           # Utility scripts
│   ├── upload_to_gcs.py               # Upload to GCS (triggers pipeline)
│   ├── create_bq_views.sql            # 15 BigQuery analysis views
│   └── apply_bq_views.sh             # Deploy views to BigQuery
└── data/                              # Local data (not committed)
    ├── gharchive_raw/                 # Downloaded .gz files
    └── gharchive_filtered/            # Filtered for upload
```
