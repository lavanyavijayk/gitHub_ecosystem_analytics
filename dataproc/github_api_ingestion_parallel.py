# ================================================================
# github_api_ingestion_parallel.py  —  Dataproc PySpark job
#
# Single-token sharded version of github_api_ingestion.py.
# Designed to run as 4 parallel jobs (one per token) on the same
# cluster.  Each job fetches 1/N of the repo list using one token.
#
# MODES:
#   fetch  — shard the repo list, call GitHub API, write parquet
#   merge  — read all shard parquets, merge into Delta + contributors
#
# ARGS:
#   --mode         fetch | merge
#   --run_mode     incremental | full_refresh
#   --prefix       GCS bucket prefix
#   --project_id   GCP project ID
#   --top_pct      percentage of top repos to fetch (default 40, capped at 4000)
#   --year         only consider events from this year when ranking repos
#   --token_index  which token to use (0-3) — required for fetch mode
#   --num_shards   total parallel jobs (default 4)
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
parser.add_argument("--mode",        default="fetch", choices=["fetch", "merge"])
parser.add_argument("--run_mode",    default="incremental")
parser.add_argument("--prefix",      required=True)
parser.add_argument("--project_id",  required=True)
parser.add_argument("--top_pct",     type=int, default=40)
parser.add_argument("--year",        type=int, required=True)
parser.add_argument("--token_index", type=int, default=-1,
                    help="Token index 0..N-1 (required for fetch mode)")
parser.add_argument("--num_shards",  type=int, default=4)
args = parser.parse_args()

mode        = args.mode
run_mode    = args.run_mode
prefix      = args.prefix
project_id  = args.project_id
TOP_PCT     = args.top_pct
YEAR        = args.year
token_index = args.token_index
num_shards  = args.num_shards

SILVER     = f"gs://{prefix}-silver"
SHARD_BASE = f"{SILVER}/_api_shards/year={YEAR}"

# ── Spark session ────────────────────────────────────────────
spark = (SparkSession.builder
    .appName(f"api_ingest_{'merge' if mode == 'merge' else f'shard{token_index}'}")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ================================================================
# SHARED: Identify top repos (both modes need this for incremental)
# ================================================================
SILVER_TABLES = [
    "push_events", "pr_events", "issues_events",
    "watch_events", "fork_events", "create_events",
    "release_events",
]

event_counts = None
repo_names   = None

for tbl in SILVER_TABLES:
    path = f"{SILVER}/{tbl}"
    if not DeltaTable.isDeltaTable(spark, path):
        continue
    df = (spark.read.format("delta").load(path)
               .filter(F.col("year") == YEAR))
    counts = df.groupBy("repo_id").agg(F.count("event_id").alias("n"))
    event_counts = counts if event_counts is None else (
        event_counts.union(counts).groupBy("repo_id").agg(F.sum("n").alias("n"))
    )
    if "repo_name" in df.columns:
        names = df.select("repo_id", "repo_name")
        repo_names = names if repo_names is None else repo_names.unionByName(names)

if event_counts is None or repo_names is None:
    print(f"[shard] No silver data for year {YEAR} — exiting.")
    sys.exit(0)

repo_names = repo_names.dropDuplicates(["repo_id"])
total_repos = event_counts.count()
TOP_N = max(1, min(int(total_repos * TOP_PCT / 100), 4000))

top_repos = (
    event_counts
    .join(repo_names, "repo_id", "inner")
    .orderBy(F.col("n").desc())
    .limit(TOP_N)
    .select("repo_id", "repo_name")
)

repo_path = f"{SILVER}/repositories"

if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, repo_path):
    existing_ids = spark.read.format("delta").load(repo_path).select("repo_id")
    to_fetch = top_repos.join(existing_ids, "repo_id", "left_anti").collect()
    print(f"[shard] Incremental: {len(to_fetch)} new repos to fetch")
else:
    to_fetch = top_repos.collect()
    print(f"[shard] Full refresh: {len(to_fetch)} repos total")


# ================================================================
# MODE: FETCH — one token, one shard of repos
# ================================================================
if mode == "fetch":
    if token_index < 0:
        print("[shard] ERROR: --token_index required for fetch mode")
        sys.exit(1)

    # ── Load only one token ──────────────────────────────────
    secret_ids = ("github-pat", "github-pat-2", "github-pat-3", "github-pat-4")
    sm_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_ids[token_index]}/versions/latest"
    pat = sm_client.access_secret_version(
        request={"name": secret_name}
    ).payload.data.decode("UTF-8")

    session = requests.Session()
    session.headers.update({
        "Authorization":        f"token {pat}",
        "Accept":               "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })

    print(f"[shard{token_index}] Loaded token {token_index} ({secret_ids[token_index]})")

    # ── Shard the repo list ──────────────────────────────────
    my_repos = to_fetch[token_index::num_shards]
    print(f"[shard{token_index}] Assigned {len(my_repos)} repos "
          f"(of {len(to_fetch)} total, {num_shards} shards)")

    if not my_repos:
        print(f"[shard{token_index}] Nothing to fetch — exiting.")
        sys.exit(0)

    # ── Fetch loop ───────────────────────────────────────────
    api_calls    = 0
    MAX_CALLS    = 4800
    MAX_RUNTIME  = 26100
    START_TIME   = time.time()

    def fetch_repo(name: str) -> dict | None:
        global api_calls
        url = f"https://api.github.com/repos/{name}"
        attempt = 0

        while True:
            api_calls += 1
            resp = session.get(url, timeout=30)

            if resp.status_code == 404:
                return None

            if resp.status_code == 451:
                # Legally restricted — cannot fetch
                return None

            if resp.status_code in (403, 429):
                remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
                body_msg = resp.json().get("message", "") if resp.text else ""
                print(f"  [shard{token_index}] {name} — {resp.status_code} "
                      f"remaining={remaining} msg=\"{body_msg}\"")
                if remaining > 0 and "secondary rate limit" in body_msg.lower():
                    if attempt >= 2:
                        print(f"  [shard{token_index}] {name} — skipping after {attempt+1} rate limit retries")
                        return None
                    # Actual secondary rate limit — sleep and retry
                    retry_after = int(resp.headers.get("Retry-After", 0))
                    wait = retry_after if retry_after > 0 else 60
                    print(f"  [shard{token_index}] {name} — sleeping {wait}s (attempt {attempt+1})")
                    time.sleep(wait)
                    attempt += 1
                    continue
                if remaining > 0:
                    # 403 but not rate limit — repo is blocked/inaccessible
                    print(f"  [shard{token_index}] {name} — skipping (blocked/inaccessible)")
                    return None
                else:
                    # Primary rate limit exhausted — wait for reset
                    reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                    wait = max(5, reset - time.time()) + 5
                    print(f"  [shard{token_index}] {name} — primary limit exhausted, "
                          f"sleeping {wait:.0f}s")
                    time.sleep(wait)
                    attempt += 1
                    continue

            if resp.status_code >= 500:
                wait = min(120, (2 ** attempt) * 5)
                print(f"  [shard{token_index}] {name} — server error {resp.status_code}, "
                      f"sleeping {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                attempt += 1
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
                "has_readme":       None,
                "has_ci":           None,
                "created_at":       d["created_at"],
            }
        return None

    results = []
    errors  = 0

    for i, row in enumerate(my_repos):
        elapsed = time.time() - START_TIME
        if elapsed > MAX_RUNTIME:
            print(f"\n[shard{token_index}] Runtime limit reached after {i} repos")
            break
        if api_calls >= MAX_CALLS:
            print(f"\n[shard{token_index}] API call limit ({MAX_CALLS}) reached after {i} repos")
            break

        try:
            data = fetch_repo(row["repo_name"])
            if data:
                results.append(data)
        except Exception as e:
            print(f"  [shard{token_index}] error {row['repo_name']}: {e}")
            errors += 1

        if (i + 1) % 200 == 0:
            try:
                quota = session.get("https://api.github.com/rate_limit",
                                    timeout=10).json().get("rate", {})
                api_calls += 1
                remaining = quota.get("remaining", "?")
            except Exception:
                remaining = "?"
            print(f"  [shard{token_index}] [{i+1}/{len(my_repos)}] "
                  f"fetched={len(results)} errors={errors} "
                  f"api_calls={api_calls} remaining={remaining} "
                  f"elapsed={int(elapsed)}s")

    print(f"\n[shard{token_index}] Done: {len(results)} fetched, {errors} errors, "
          f"{api_calls} API calls, {int(time.time() - START_TIME)}s elapsed")

    # ── Write results as parquet to shard path ───────────────
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
        shard_path = f"{SHARD_BASE}/shard={token_index}"
        shard_df = spark.createDataFrame(results, schema=repo_schema)
        shard_df.write.mode("overwrite").parquet(shard_path)
        print(f"[shard{token_index}] Wrote {len(results)} rows → {shard_path}")
    else:
        print(f"[shard{token_index}] No results to write.")


# ================================================================
# MODE: MERGE — combine all shards into Delta + build contributors
# ================================================================
elif mode == "merge":
    print(f"[merge] Reading shards from {SHARD_BASE}/shard=*")

    try:
        shard_df = (
            spark.read.parquet(f"{SHARD_BASE}/shard=*")
            .withColumn("created_at", F.to_timestamp("created_at"))
            .dropDuplicates(["repo_id"])
        )
        shard_count = shard_df.count()
    except Exception as e:
        print(f"[merge] No shard data found: {e}")
        shard_count = 0

    if shard_count > 0:
        print(f"[merge] Merging {shard_count} repos into Delta")

        if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, repo_path):
            (DeltaTable.forPath(spark, repo_path).alias("t")
                       .merge(shard_df.alias("s"), "t.repo_id = s.repo_id")
                       .whenMatchedUpdateAll()
                       .whenNotMatchedInsertAll()
                       .execute())
            print(f"[merge] MERGE → silver/repositories ({shard_count} rows)")
        else:
            (shard_df.write.format("delta").mode("overwrite")
                     .option("overwriteSchema", "true").save(repo_path))
            print(f"[merge] OVERWRITE → silver/repositories ({shard_count} rows)")

        # ── Cleanup shard files ──────────────────────────────
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI(f"gs://{prefix}-silver"), hadoop_conf)
        # shard_dir = sc._jvm.org.apache.hadoop.fs.Path(SHARD_BASE)
        # if fs.exists(shard_dir):
        #     fs.delete(shard_dir, True)
        #     print(f"[merge] Cleaned up {SHARD_BASE}")
    else:
        print("[merge] No shard data to merge.")

    # ── Contributors — derived from silver event tables ──────
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
        print("[merge] No silver event tables — skipping contributors.")
        sys.exit(0)

    all_actors = actor_frames[0]
    for frame in actor_frames[1:]:
        all_actors = all_actors.unionByName(frame, allowMissingColumns=True)

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
        print("[merge] MERGE → silver/contributors")
    else:
        (contribs_df.write.format("delta").mode("overwrite")
                    .option("overwriteSchema", "true").save(contrib_path))
        print(f"[merge] OVERWRITE → silver/contributors ({contribs_df.count():,} rows)")

    print("[merge] Complete.")
