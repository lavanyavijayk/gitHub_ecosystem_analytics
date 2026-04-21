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
#   --top_pct     percentage of top repos to fetch (default 40, capped at 4000)
#   --year        only consider events from this year when ranking repos
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
parser.add_argument("--top_pct",    type=int, default=40)
parser.add_argument("--year",       type=int, required=True,
                    help="Only consider events from this year when ranking repos")
args = parser.parse_args()

run_mode   = args.run_mode
prefix     = args.prefix
project_id = args.project_id
TOP_PCT    = args.top_pct
YEAR       = args.year

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
print(f"[api_ingest] top_pct    = {TOP_PCT}%")
print(f"[api_ingest] year       = {YEAR}")

# ── GitHub PATs from Secret Manager (round-robin for throughput) ──
sm_client = secretmanager.SecretManagerServiceClient()

github_pats = []
for secret_id in ("github-pat", "github-pat-2", "github-pat-3", "github-pat-4"):
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    pat = sm_client.access_secret_version(
        request={"name": secret_name}
    ).payload.data.decode("UTF-8")
    github_pats.append(pat)

print(f"[api_ingest] Loaded {len(github_pats)} GitHub PATs from Secret Manager")

# ================================================================
# IDENTIFY TOP REPOS
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

    # event counts
    counts = df.groupBy("repo_id").agg(F.count("event_id").alias("n"))
    event_counts = counts if event_counts is None else (
        event_counts.union(counts).groupBy("repo_id").agg(F.sum("n").alias("n"))
    )

    # repo names
    if "repo_name" in df.columns:
        names = df.select("repo_id", "repo_name")
        repo_names = names if repo_names is None else repo_names.unionByName(names)

if event_counts is None:
    print(f"[api_ingest] No silver event tables with data for year {YEAR} — exiting.")
    sys.exit(0)

if repo_names is None:
    print(f"[api_ingest] No repo_name column found for year {YEAR} — exiting.")
    sys.exit(0)

repo_names = repo_names.dropDuplicates(["repo_id"])

total_repos = event_counts.count()
TOP_N = max(1, min(int(total_repos * TOP_PCT / 100), 4000))
print(f"[api_ingest] total repos = {total_repos}, fetching top {TOP_PCT}% = {TOP_N} (capped at 4000)")

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
    # ── Build one session per PAT for round-robin ────────────
    sessions = []
    for pat in github_pats:
        s = requests.Session()
        s.headers.update({
            "Authorization":        f"token {pat}",
            "Accept":               "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        sessions.append(s)

    num_tokens      = len(sessions)
    per_token_calls = [0] * num_tokens  # track API calls per token
    MAX_PER_TOKEN   = 4800              # hard stop per token (GitHub limit is 5000/hr)

    def fetch_repo(name: str, session_idx: int) -> dict | None:
        """Fetch a single repo, cycling through all tokens on rate limit."""
        url = f"https://api.github.com/repos/{name}"
        tried_tokens = set()

        for attempt in range(3 + num_tokens):
            session = sessions[session_idx]
            per_token_calls[session_idx] += 1
            resp = session.get(url, timeout=30)

            if resp.status_code == 404:
                return None

            if resp.status_code in (403, 429):
                remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
                if remaining > 0:
                    # Secondary rate limit (per-IP, shared across tokens)
                    # Use retry-after header if provided, otherwise backoff
                    retry_after = int(resp.headers.get("Retry-After", 0))
                    if retry_after > 0:
                        print(f"  [secondary_rate_limit] token{session_idx} {name} "
                              f"— retry-after {retry_after}s (remaining={remaining})")
                        time.sleep(retry_after)
                        tried_tokens.clear()
                        continue
                    # No retry-after — try rotating token, then backoff
                    tried_tokens.add(session_idx)
                    next_idx = (session_idx + 1) % num_tokens
                    if next_idx not in tried_tokens:
                        print(f"  [secondary_rate_limit] token{session_idx} {name} "
                              f"— switching to token{next_idx}")
                        session_idx = next_idx
                        time.sleep(2)
                        continue
                    # All tokens hit secondary limit — backoff
                    wait = min(60, 10 * (attempt + 1))
                    print(f"  [secondary_rate_limit] all tokens {name} — sleeping {wait}s "
                          f"(remaining={remaining})")
                    time.sleep(wait)
                    tried_tokens.clear()
                    continue
                else:
                    # Primary rate limit exhausted — cycle to next token
                    tried_tokens.add(session_idx)
                    next_idx = (session_idx + 1) % num_tokens
                    if next_idx not in tried_tokens:
                        print(f"  [rate_limit] token{session_idx} exhausted for {name} "
                              f"— switching to token{next_idx}")
                        session_idx = next_idx
                        continue
                    # All tokens exhausted — wait for earliest reset
                    reset = int(resp.headers.get("X-RateLimit-Reset",
                                                  time.time() + 60))
                    wait = max(5, reset - time.time()) + 5
                    print(f"  [rate_limit] all {num_tokens} tokens exhausted for {name} "
                          f"— sleeping {wait:.0f}s")
                    time.sleep(wait)
                    tried_tokens.clear()   # reset so we retry all tokens after waiting
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
                "has_readme":       None,
                "has_ci":           None,
                "created_at":       d["created_at"],
            }
        return None

    results = []
    errors  = 0
    START_TIME   = time.time()
    MAX_RUNTIME  = 26100         # 435 min — leave 45 min for contributors + Delta writes (workflow timeout is 8hrs)
    MIN_REMAINING = 100          # stop if API quota drops below this

    for i, row in enumerate(to_fetch):
        elapsed = time.time() - START_TIME

        # Guard 1: runtime limit
        if elapsed > MAX_RUNTIME:
            print(f"\n[api_ingest] Runtime limit ({MAX_RUNTIME}s) reached after "
                  f"{i} repos — saving partial results")
            break

        # Guard 2: API call count — stop if ANY token is near its limit
        if any(c >= MAX_PER_TOKEN for c in per_token_calls):
            print(f"\n[api_ingest] API call limit ({MAX_PER_TOKEN}/token) reached "
                  f"(calls={per_token_calls}) after {i} repos — saving partial results")
            break

        # Round-robin: alternate tokens each request
        token_idx = i % num_tokens

        try:
            data = fetch_repo(row["repo_name"], token_idx)
            if data:
                results.append(data)
        except Exception as e:
            print(f"  [error] {row['repo_name']}: {e}")
            errors += 1

        if (i + 1) % 200 == 0:
            # Check quota on all tokens
            token_remaining = []
            for tidx, sess in enumerate(sessions):
                per_token_calls[tidx] += 1   # count the rate_limit check call
                try:
                    quota = sess.get("https://api.github.com/rate_limit",
                                     timeout=10).json().get("rate", {})
                    token_remaining.append(quota.get("remaining", "?"))
                except Exception:
                    token_remaining.append("?")
            print(f"  [{i+1}/{len(to_fetch)}] fetched={len(results)} "
                  f"errors={errors}  calls_per_token={per_token_calls}  "
                  f"api_remaining={token_remaining}  elapsed={int(elapsed)}s")
            # Guard 3: stop if ALL tokens' server-reported quota is low
            low_tokens = [tidx for tidx, rem in enumerate(token_remaining)
                          if isinstance(rem, int) and rem < MIN_REMAINING]
            if len(low_tokens) == num_tokens:
                print(f"\n[api_ingest] All {num_tokens} tokens quota low "
                      f"({token_remaining}) — saving partial results")
                break

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
    print("[api_ingest] MERGE → silver/contributors")
else:
    (contribs_df.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").save(contrib_path))
    print(f"[api_ingest] OVERWRITE → silver/contributors ({contribs_df.count():,} rows)")

print("[api_ingest] API ingestion complete")
