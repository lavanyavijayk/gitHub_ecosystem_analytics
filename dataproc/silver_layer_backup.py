# ================================================================
# silver_layer.py  —  Dataproc PySpark job
#
# PURPOSE: Read raw GH Archive JSON files from GCS raw/ bucket,
#          clean and validate them, then write to silver/ as
#          Delta Lake tables (partitioned by year).
#
# TRIGGERED BY: Cloud Workflows (Eventarc on GCS upload + monthly schedule)
# ARGS:
#   --triggered_file  gs:// path of the new file, or "" for full refresh
#   --run_mode        incremental | full_refresh
#   --prefix          GCS bucket prefix (e.g. "ghoss")
#   --project_id      GCP project ID
# ================================================================

import argparse
import sys
import time as _time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from google.cloud import storage as gcs_client
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, BooleanType, TimestampType,
    ShortType, ArrayType
)

# ── Parse arguments ──────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--triggered_file", default="")
parser.add_argument("--run_mode",       default="incremental")
parser.add_argument("--prefix",         required=True)
parser.add_argument("--project_id",     required=True)
args = parser.parse_args()

triggered_file = args.triggered_file
run_mode       = args.run_mode
prefix         = args.prefix
project_id     = args.project_id

RAW    = f"gs://{prefix}-raw"
SILVER = f"gs://{prefix}-silver"

# ── Spark session — Delta Lake configs set in cluster properties ──
spark = (SparkSession.builder
    .appName("silver_layer")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.sources.partitionOverwriteMode", "static")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

print(f"[silver] run_mode       = {run_mode}")
print(f"[silver] triggered_file = {triggered_file}")
print(f"[silver] RAW            = {RAW}")

# ================================================================
# DETERMINE FILES TO PROCESS
# ================================================================
if run_mode == "full_refresh" or not triggered_file:
    # List all .gz files under gs://{prefix}-raw/gharchive/
    client = gcs_client.Client(project=project_id)
    bucket = client.bucket(f"{prefix}-raw")
    blobs  = bucket.list_blobs(prefix="gharchive/")
    all_files = [
        f"gs://{prefix}-raw/{b.name}"
        for b in blobs
        if b.name.endswith(".gz")
    ]
    print(f"[silver] Full refresh: {len(all_files)} files")
else:
    all_files = [triggered_file]
    print("[silver] Incremental: 1 file")

if not all_files:
    print("[silver] No files found — exiting.")
    sys.exit(0)

# ================================================================
# RAW SCHEMA
# ================================================================
base_schema = StructType([
    StructField("id",         StringType(),    nullable=True),
    StructField("type",       StringType(),    nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
    StructField("actor", StructType([
        StructField("id",    LongType(),   nullable=True),
        StructField("login", StringType(), nullable=True),
    ]), nullable=True),
    StructField("repo", StructType([
        StructField("id",   LongType(),   nullable=True),
        StructField("name", StringType(), nullable=True),
    ]), nullable=True),
    StructField("payload", StringType(), nullable=True),
])

EVENT_TYPES = [
    "PushEvent", "PullRequestEvent", "IssuesEvent",
    "WatchEvent", "ForkEvent", "CreateEvent", "ReleaseEvent"
]

_pipeline_start = _time.time()

# ================================================================
# READ + FILTER + FLATTEN
# ================================================================
_step_start = _time.time()
raw_df = (
    spark.read
         .schema(base_schema)
         .json(all_files)
         .filter(F.col("type").isin(EVENT_TYPES))
         .filter(F.col("actor.id").isNotNull())
         .filter(F.col("repo.id").isNotNull())
         .filter(F.col("created_at").isNotNull())
         .filter(F.col("created_at") >= F.lit("2008-01-01").cast(TimestampType()))
         .filter(F.col("created_at") <= F.current_timestamp())
         .withColumn("event_id",    F.col("id"))
         .withColumn("actor_id",    F.col("actor.id"))
         .withColumn("actor_login", F.col("actor.login"))
         .withColumn("repo_id",     F.col("repo.id"))
         .withColumn("repo_name",   F.col("repo.name"))
         .withColumn("year",        F.year("created_at").cast(ShortType()))
         .withColumn("source_file", F.input_file_name())
         .dropDuplicates(["event_id"])
         .drop("id", "actor", "repo")
)

total_events = raw_df.cache().count()
print(f"[silver] Total events after filter + dedup: {total_events:,}  "
      f"({_time.time() - _step_start:.0f}s)")

# ================================================================
# WRITE HELPER
# ================================================================
def write_delta(df, table: str, merge_key: str = "event_id",
                max_retries: int = 5):
    import time, random
    _write_start = time.time()
    path = f"{SILVER}/{table}"
    # Enable schema auto-merge so new columns (e.g. repo_name, actor_login)
    # are added automatically during MERGE without failing
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, path):
        row_count = df.cache().count()
        if row_count == 0:
            print(f"  [silver] SKIP → {table} (0 rows) ({time.time() - _write_start:.0f}s)")
            df.unpersist()
            return
        for attempt in range(1, max_retries + 1):
            try:
                delta_tbl = DeltaTable.forPath(spark, path)
                (delta_tbl.alias("existing")
                          .merge(df.alias("incoming"),
                                 f"existing.{merge_key} = incoming.{merge_key}")
                          .whenNotMatchedInsertAll()
                          .execute())
                print(f"  [silver] MERGE → {table} ({row_count} rows) "
                      f"({time.time() - _write_start:.0f}s)")
                break
            except Exception as e:
                if "ConcurrentAppendException" in str(e) and attempt < max_retries:
                    wait = 2 ** attempt + random.uniform(1, 5)
                    print(f"  [silver] Concurrent write conflict on {table}, "
                          f"retry {attempt}/{max_retries} in {wait:.0f}s")
                    time.sleep(wait)
                else:
                    raise
        df.unpersist()
    else:
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .partitionBy("year")
           .save(path))
        print(f"  [silver] OVERWRITE → {table} ({time.time() - _write_start:.0f}s)")

# ================================================================
# PUSH EVENTS
# ================================================================
push_payload_schema = StructType([
    StructField("ref",  StringType(),  nullable=True),
    StructField("size", IntegerType(), nullable=True),
])
push_df = (
    raw_df.filter(F.col("type") == "PushEvent")
          .withColumn("p", F.from_json("payload", push_payload_schema))
          .withColumn("commit_count",
              F.coalesce(F.col("p.size"), F.lit(0)).cast(IntegerType()))
          .withColumn("branch_ref",
              F.coalesce(F.col("p.ref"), F.lit("unknown")))
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "commit_count", "branch_ref", "source_file")
)
write_delta(push_df, "push_events")

# ================================================================
# PULL REQUEST EVENTS
# ================================================================
pr_payload_schema = StructType([
    StructField("action", StringType(), nullable=True),
    StructField("pull_request", StructType([
        StructField("id",              LongType(),      nullable=True),
        StructField("title",           StringType(),    nullable=True),
        StructField("state",           StringType(),    nullable=True),
        StructField("merged",          BooleanType(),   nullable=True),
        StructField("additions",       IntegerType(),   nullable=True),
        StructField("deletions",       IntegerType(),   nullable=True),
        StructField("changed_files",   IntegerType(),   nullable=True),
        StructField("review_comments", IntegerType(),   nullable=True),
        StructField("created_at",      TimestampType(), nullable=True),
        StructField("merged_at",       TimestampType(), nullable=True),
        StructField("closed_at",       TimestampType(), nullable=True),
    ]), nullable=True),
])
pr_df = (
    raw_df.filter(F.col("type") == "PullRequestEvent")
          .withColumn("p", F.from_json("payload", pr_payload_schema))
          .withColumn("action",          F.col("p.action"))
          .withColumn("pr_id",           F.col("p.pull_request.id"))
          .withColumn("title",           F.col("p.pull_request.title"))
          .withColumn("state",           F.col("p.pull_request.state"))
          .withColumn("is_merged",
              F.coalesce(F.col("p.pull_request.merged"), F.lit(False)))
          .withColumn("additions",
              F.coalesce(F.col("p.pull_request.additions"),   F.lit(0)))
          .withColumn("deletions",
              F.coalesce(F.col("p.pull_request.deletions"),   F.lit(0)))
          .withColumn("changed_files",
              F.coalesce(F.col("p.pull_request.changed_files"),   F.lit(0)))
          .withColumn("review_comments",
              F.coalesce(F.col("p.pull_request.review_comments"), F.lit(0)))
          .withColumn("pr_created_at", F.col("p.pull_request.created_at"))
          .withColumn("merged_at",     F.col("p.pull_request.merged_at"))
          .withColumn("closed_at",     F.col("p.pull_request.closed_at"))
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "action", "pr_id", "title", "state", "is_merged",
                  "additions", "deletions", "changed_files",
                  "review_comments", "pr_created_at", "merged_at", "closed_at",
                  "source_file")
)
write_delta(pr_df, "pr_events")

# ================================================================
# ISSUES EVENTS
# ================================================================
issue_payload_schema = StructType([
    StructField("action", StringType(), nullable=True),
    StructField("issue", StructType([
        StructField("id",         LongType(),      nullable=True),
        StructField("title",      StringType(),    nullable=True),
        StructField("state",      StringType(),    nullable=True),
        StructField("comments",   IntegerType(),   nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("closed_at",  TimestampType(), nullable=True),
        StructField("labels",     ArrayType(StructType([
            StructField("name", StringType(), nullable=True)
        ])), nullable=True),
    ]), nullable=True),
])
issues_df = (
    raw_df.filter(F.col("type") == "IssuesEvent")
          .withColumn("p", F.from_json("payload", issue_payload_schema))
          .withColumn("action",           F.col("p.action"))
          .withColumn("issue_id",         F.col("p.issue.id"))
          .withColumn("title",            F.col("p.issue.title"))
          .withColumn("state",            F.col("p.issue.state"))
          .withColumn("comments_count",
              F.coalesce(F.col("p.issue.comments"), F.lit(0)))
          .withColumn("issue_created_at", F.col("p.issue.created_at"))
          .withColumn("closed_at",        F.col("p.issue.closed_at"))
          .withColumn("label_list",
              F.concat_ws(",",
                  F.transform(F.col("p.issue.labels"), lambda lbl: lbl["name"])))
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "action", "issue_id", "title", "state",
                  "comments_count", "label_list", "issue_created_at", "closed_at",
                  "source_file")
)
write_delta(issues_df, "issues_events")

# ================================================================
# WATCH EVENTS
# ================================================================
watch_payload_schema = StructType([StructField("action", StringType(), nullable=True)])
watch_df = (
    raw_df.filter(F.col("type") == "WatchEvent")
          .withColumn("p", F.from_json("payload", watch_payload_schema))
          .filter(F.col("p.action") == "started")
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "source_file")
)
write_delta(watch_df, "watch_events")

# ================================================================
# FORK EVENTS
# ================================================================
fork_payload_schema = StructType([
    StructField("forkee", StructType([
        StructField("id",        LongType(),   nullable=True),
        StructField("full_name", StringType(), nullable=True),
    ]), nullable=True),
])
fork_df = (
    raw_df.filter(F.col("type") == "ForkEvent")
          .withColumn("p", F.from_json("payload", fork_payload_schema))
          .withColumn("fork_id",   F.col("p.forkee.id"))
          .withColumn("fork_name", F.col("p.forkee.full_name"))
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "fork_id", "fork_name", "source_file")
)
write_delta(fork_df, "fork_events")

# ================================================================
# CREATE EVENTS
# ================================================================
create_payload_schema = StructType([
    StructField("ref_type",    StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
])
create_df = (
    raw_df.filter(F.col("type") == "CreateEvent")
          .withColumn("p", F.from_json("payload", create_payload_schema))
          .withColumn("ref_type",    F.col("p.ref_type"))
          .withColumn("description", F.col("p.description"))
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "ref_type", "description", "source_file")
)
write_delta(create_df, "create_events")

# ================================================================
# RELEASE EVENTS
# ================================================================
release_payload_schema = StructType([
    StructField("action", StringType(), nullable=True),
    StructField("release", StructType([
        StructField("id",              LongType(),      nullable=True),
        StructField("tag_name",        StringType(),    nullable=True),
        StructField("name",            StringType(),    nullable=True),
        StructField("prerelease",      BooleanType(),   nullable=True),
        StructField("draft",           BooleanType(),   nullable=True),
        StructField("created_at",      TimestampType(), nullable=True),
        StructField("published_at",    TimestampType(), nullable=True),
    ]), nullable=True),
])
release_df = (
    raw_df.filter(F.col("type") == "ReleaseEvent")
          .withColumn("p", F.from_json("payload", release_payload_schema))
          .withColumn("action",            F.col("p.action"))
          .withColumn("release_id",        F.col("p.release.id"))
          .withColumn("tag_name",          F.col("p.release.tag_name"))
          .withColumn("release_name",      F.col("p.release.name"))
          .withColumn("is_prerelease",
              F.coalesce(F.col("p.release.prerelease"), F.lit(False)))
          .withColumn("is_draft",
              F.coalesce(F.col("p.release.draft"), F.lit(False)))
          .withColumn("release_created_at", F.col("p.release.created_at"))
          .withColumn("published_at",       F.col("p.release.published_at"))
          .select("event_id", "repo_id", "repo_name", "actor_id",
                  "actor_login", "created_at", "year",
                  "action", "release_id", "tag_name", "release_name",
                  "is_prerelease", "is_draft",
                  "release_created_at", "published_at", "source_file")
)
write_delta(release_df, "release_events")

# ================================================================
# DATA QUALITY AUDIT
# ================================================================
_step_start = _time.time()
_audit_rows = []
for _tbl, _etype in [
    ("push_events",    "PushEvent"),
    ("pr_events",      "PullRequestEvent"),
    ("issues_events",  "IssuesEvent"),
    ("watch_events",   "WatchEvent"),
    ("fork_events",    "ForkEvent"),
    ("create_events",  "CreateEvent"),
    ("release_events", "ReleaseEvent"),
]:
    _path = f"{SILVER}/{_tbl}"
    try:
        _accepted = spark.read.format("delta").load(_path).count() \
            if DeltaTable.isDeltaTable(spark, _path) else 0
    except Exception:
        _accepted = 0
    _raw_count = raw_df.filter(F.col("type") == _etype).count()
    _audit_rows.append((
        _etype, int(_raw_count), int(_accepted),
        run_mode, datetime.now(timezone.utc).isoformat(),
    ))

_audit_schema = ["event_type", "raw_count", "accepted_count", "run_mode", "audited_at"]
_audit_df     = spark.createDataFrame(_audit_rows, _audit_schema)
_audit_path   = f"{SILVER}/dq_audit"

(_audit_df.write
          .format("delta")
          .mode("append" if DeltaTable.isDeltaTable(spark, _audit_path) else "overwrite")
          .option("overwriteSchema", "true")
          .save(_audit_path))
print(f"[silver] DQ audit written ({_time.time() - _step_start:.0f}s):")
_audit_df.show(truncate=False)

# ================================================================
# VACUUM  (7-day retention — safe minimum for OSS Delta Lake)
# ================================================================
_step_start = _time.time()
for tbl in ["push_events", "pr_events", "issues_events",
            "watch_events", "fork_events", "create_events",
            "release_events"]:
    path = f"{SILVER}/{tbl}"
    if DeltaTable.isDeltaTable(spark, path):
        _vac_start = _time.time()
        DeltaTable.forPath(spark, path).vacuum(retentionHours=168)
        print(f"  [silver] VACUUM → {tbl} ({_time.time() - _vac_start:.0f}s)")
print(f"[silver] VACUUM total ({_time.time() - _step_start:.0f}s)")

print(f"[silver] silver layer complete — total pipeline time: "
      f"{_time.time() - _pipeline_start:.0f}s")
