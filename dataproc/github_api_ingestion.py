# ================================================================
# github_api_ingestion.py  —  Dataproc PySpark job
#
# PURPOSE: Identify the most active repos from silver event tables,
#          call the GitHub REST API to enrich them, and write
#          silver/repositories and silver/contributors.
#
# ARGS:
#   --run_mode    incremental | full_refresh
#   --prefix      GCS bucket prefix
#   --project_id  GCP project ID
#   --top_n       number of top repos to fetch (default 2000)
# ================================================================

import argparse
import sys
import time

from delta.tables import DeltaTable
from google.cloud import secretmanager
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, BooleanType, TimestampType
)
import requests

# ── Parse arguments ──────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--run_mode",   default="incremental")
parser.add_argument("--prefix",     required=True)
parser.add_argument("--project_id", required=True)
parser.add_argument("--top_n",      type=int, default=2000)
args = parser.parse_args()

run_mode   = args.run_mode
prefix     = args.prefix
project_id = args.project_id
TOP_N      = args.top_n

SILVER = f"gs://{prefix}-silver"

# ── Spark session ────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("github_api_ingestion")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print(f"[api_ingest] run_mode   = {run_mode}")
print(f"[api_ingest] project_id = {project_id}")
print(f"[api_ingest] top_n      = {TOP_N}")

# ── GitHub PAT from Secret Manager ───────────────────────────
sm_client  = secretmanager.SecretManagerServiceClient()
secret_name = f"projects/{project_id}/secrets/github-pat/versions/latest"
github_pat  = sm_client.access_secret_version(
    request={"name": secret_name}
).payload.data.decode("UTF-8")
print("[api_ingest] GitHub PAT loaded from Secret Manager")

# ================================================================
# IDENTIFY TOP REPOS
# ================================================================
SILVER_TABLES = [
    "push_events", "pr_events", "issues_events",
    "watch_events", "fork_events", "create_events",
]

event_counts = None
for tbl in SILVER_TABLES:
    path = f"{SILVER}/{tbl}"
    if not DeltaTable.isDeltaTable(spark, path):
        continue
    counts = (spark.read.format("delta").load(path)
                   .groupBy("repo_id")
                   .agg(F.count("event_id").alias("n")))
    event_counts = counts if event_counts is None else (
        event_counts.union(counts).groupBy("repo_id").agg(F.sum("n").alias("n"))
    )

if event_counts is None:
    print("[api_ingest] No silver event tables found — exiting.")
    sys.exit(0)

push_path  = f"{SILVER}/push_events"
repo_names = (
    spark.read.format("delta").load(push_path)
         .select("repo_id", "repo_name")
         .dropDuplicates(["repo_id"])
)

top_repos = (
    event_counts
    .join(repo_names, "repo_id", "inner")
    .orderBy(F.col("n").desc())
    .limit(TOP_N)
    .select("repo_id", "repo_name")
)

# ================================================================
# DECIDE WHICH REPOS TO FETCH
# ================================================================
repo_path = f"{SILVER}/repositories"

if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, repo_path):
    existing_ids = spark.read.format("delta").load(repo_path).select("repo_id")
    to_fetch = (top_repos.join(existing_ids, "repo_id", "left_anti").collect())
    print(f"[api_ingest] Incremental: {len(to_fetch)} new repos to fetch")
else:
    to_fetch = top_repos.collect()
    print(f"[api_ingest] Full refresh: fetching {len(to_fetch)} repos")

# ================================================================
# GITHUB REST API
# ================================================================
if to_fetch:
    session = requests.Session()
    session.headers.update({
        "Authorization":        f"token {github_pat}",
        "Accept":               "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })

    def check_has_readme(name: str) -> bool:
        """Check if the repo has a README via the GitHub contents API."""
        try:
            resp = session.get(
                f"https://api.github.com/repos/{name}/readme", timeout=15)
            return resp.status_code == 200
        except Exception:
            return False

    def fetch_repo(name: str) -> dict | None:
        url = f"https://api.github.com/repos/{name}"
        for attempt in range(5):
            resp = session.get(url, timeout=60)
            if resp.status_code == 404:
                return None
            if resp.status_code in (403, 429):
                reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait  = max(5, reset - time.time()) + 5
                print(f"  [rate_limit] {name} — sleeping {wait:.0f}s")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                time.sleep((2 ** attempt) * 5)
                continue
            resp.raise_for_status()
            d = resp.json()
            return {
                "repo_id":          d["id"],
                "repo_name":        d["full_name"],
                "language":         d.get("language"),
                "license":          (d.get("license") or {}).get("spdx_id"),
                "topics":           ",".join(d.get("topics", [])),
                "stars_at_extract": d["stargazers_count"],
                "forks_at_extract": d["forks_count"],
                "description":      (d.get("description") or "")[:500],
                "is_archived":      d["archived"],
                "has_readme":       check_has_readme(name),
                "has_ci":           ".github/workflows" in d.get("contents_url", ""),
                "created_at":       d["created_at"],
            }
        return None

    results = []
    errors  = 0
    for i, row in enumerate(to_fetch):
        try:
            data = fetch_repo(row["repo_name"])
            if data:
                results.append(data)
        except Exception as e:
            print(f"  [error] {row['repo_name']}: {e}")
            errors += 1
        if (i + 1) % 100 == 0:
            try:
                quota = session.get("https://api.github.com/rate_limit",
                                    timeout=10).json().get("rate", {})
                remaining = quota.get("remaining", "?")
            except Exception:
                remaining = "?"
            print(f"  [{i+1}/{len(to_fetch)}] fetched={len(results)} "
                  f"errors={errors}  api_remaining={remaining}")
            time.sleep(2)

    print(f"\n[api_ingest] API fetch complete: {len(results)} enriched  "
          f"{errors} errors")

    if results:
        repo_schema = StructType([
            StructField("repo_id",          LongType(),    nullable=True),
            StructField("repo_name",        StringType(),  nullable=True),
            StructField("language",         StringType(),  nullable=True),
            StructField("license",          StringType(),  nullable=True),
            StructField("topics",           StringType(),  nullable=True),
            StructField("stars_at_extract", IntegerType(), nullable=True),
            StructField("forks_at_extract", IntegerType(), nullable=True),
            StructField("description",      StringType(),  nullable=True),
            StructField("is_archived",      BooleanType(), nullable=True),
            StructField("has_readme",       BooleanType(), nullable=True),
            StructField("has_ci",           BooleanType(), nullable=True),
            StructField("created_at",       StringType(),  nullable=True),
        ])
        new_repos_df = (
            spark.createDataFrame(results, schema=repo_schema)
                 .withColumn("created_at", F.to_timestamp("created_at"))
                 .dropDuplicates(["repo_id"])
        )
        if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, repo_path):
            (DeltaTable.forPath(spark, repo_path).alias("t")
                       .merge(new_repos_df.alias("s"), "t.repo_id = s.repo_id")
                       .whenMatchedUpdateAll()
                       .whenNotMatchedInsertAll()
                       .execute())
            print(f"[api_ingest] MERGE → silver/repositories ({len(results)} rows)")
        else:
            (new_repos_df.write.format("delta").mode("overwrite")
                         .option("overwriteSchema", "true").save(repo_path))
            print(f"[api_ingest] OVERWRITE → silver/repositories ({len(results)} rows)")
else:
    print("[api_ingest] No new repos to fetch.")

# ================================================================
# CONTRIBUTORS — derived from silver event tables
# ================================================================
actor_frames = []
for tbl in SILVER_TABLES:
    path = f"{SILVER}/{tbl}"
    if not DeltaTable.isDeltaTable(spark, path):
        continue
    df   = spark.read.format("delta").load(path)
    cols = df.columns
    login_col = F.col("actor_login") if "actor_login" in cols \
                else F.lit(None).cast(StringType())
    actor_frames.append(
        df.select(F.col("actor_id").cast(LongType()),
                  login_col.alias("login"))
    )

if not actor_frames:
    print("[api_ingest] No silver event tables — skipping contributors.")
    sys.exit(0)

all_actors = actor_frames[0]
for frame in actor_frames[1:]:
    all_actors = all_actors.union(frame)

contribs_df = (
    all_actors
    .filter(F.col("actor_id").isNotNull())
    .groupBy("actor_id")
    .agg(F.first("login", ignorenulls=True).alias("login"))
    .withColumn("account_type", F.lit("User"))
)

contrib_path = f"{SILVER}/contributors"
if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, contrib_path):
    (DeltaTable.forPath(spark, contrib_path).alias("t")
               .merge(contribs_df.alias("s"), "t.actor_id = s.actor_id")
               .whenNotMatchedInsertAll()
               .execute())
    print("[api_ingest] MERGE → silver/contributors")
else:
    (contribs_df.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").save(contrib_path))
    print(f"[api_ingest] OVERWRITE → silver/contributors ({contribs_df.count():,} rows)")

print("[api_ingest] API ingestion complete")
