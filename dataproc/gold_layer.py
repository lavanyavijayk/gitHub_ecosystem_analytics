# ================================================================
# gold_layer.py  —  Dataproc PySpark job
#
# PURPOSE: Read silver Delta tables, build the star schema
#          dimensional model, write to gold/ as Delta tables,
#          and write all tables to BigQuery (every run).
#
# ARGS:
#   --run_mode    incremental | full_refresh
#   --prefix      GCS bucket prefix
#   --project_id  GCP project ID
# ================================================================

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ── Parse arguments ──────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--run_mode",   default="incremental")
parser.add_argument("--prefix",     required=True)
parser.add_argument("--project_id", required=True)
parser.add_argument("--bq_dataset", default="github_oss_gold")
args = parser.parse_args()

run_mode   = args.run_mode
prefix     = args.prefix
project_id = args.project_id
bq_dataset = args.bq_dataset

SILVER = f"gs://{prefix}-silver"
GOLD   = f"gs://{prefix}-gold"

# ── Spark session ────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("gold_layer")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print(f"[gold] run_mode  = {run_mode}")
print(f"[gold] SILVER    = {SILVER}")
print(f"[gold] GOLD      = {GOLD}")

# ================================================================
# WRITE HELPER
# ================================================================
def write_gold(df, table: str, merge_key, partition_col: str = None):
    path = f"{GOLD}/{table}"
    if isinstance(merge_key, str):
        merge_cond = f"t.{merge_key} = s.{merge_key}"
    else:
        merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)

    if run_mode == "incremental" and DeltaTable.isDeltaTable(spark, path):
        delta_tbl = DeltaTable.forPath(spark, path)
        (delta_tbl.alias("t")
                  .merge(df.alias("s"), merge_cond)
                  .whenMatchedUpdateAll()
                  .whenNotMatchedInsertAll()
                  .execute())
        print(f"  [gold] MERGE → {table}")
    else:
        writer = (df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .option("mergeSchema",     "true"))
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(path)
        print(f"  [gold] OVERWRITE → {table}")

# ================================================================
# LOAD SILVER TABLES
# ================================================================
push    = spark.read.format("delta").load(f"{SILVER}/push_events")
pr      = spark.read.format("delta").load(f"{SILVER}/pr_events")
issues  = spark.read.format("delta").load(f"{SILVER}/issues_events")
watches = spark.read.format("delta").load(f"{SILVER}/watch_events")
forks   = spark.read.format("delta").load(f"{SILVER}/fork_events")
repos   = spark.read.format("delta").load(f"{SILVER}/repositories")
contribs= spark.read.format("delta").load(f"{SILVER}/contributors")

# ================================================================
# dim_date
# ================================================================
all_ts = (push.select("created_at")
          .union(pr.select("created_at"))
          .union(issues.select("created_at"))
          .union(watches.select("created_at"))
          .union(forks.select("created_at"))
          .distinct())

dim_date = (
    all_ts
    .withColumn("date_key",
        F.date_format("created_at", "yyyyMMddHH").cast("int"))
    .withColumn("full_date",       F.to_date("created_at"))
    .withColumn("year",            F.year("created_at").cast("short"))
    .withColumn("quarter",         F.quarter("created_at").cast("byte"))
    .withColumn("month",           F.month("created_at").cast("byte"))
    .withColumn("month_name",      F.date_format("created_at", "MMMM"))
    .withColumn("day_of_week",     F.dayofweek("created_at").cast("byte"))
    .withColumn("day_name",        F.date_format("created_at", "EEEE"))
    .withColumn("hour",            F.hour("created_at").cast("byte"))
    .withColumn("is_weekend",
        F.dayofweek("created_at").isin(1, 7).cast("boolean"))
    .withColumn("is_covid_period",
        ((F.to_date("created_at") >= "2020-03-01") &
         (F.to_date("created_at") <= "2021-06-30")).cast("boolean"))
    .drop("created_at")
    .distinct()
)
write_gold(dim_date, "dim_date", merge_key="date_key", partition_col="year")

# ================================================================
# dim_repository
# ================================================================
AI_PATTERN = "ai|ml|llm|gpt|deep.learning|machine.learning|neural|transformer|diffusion|langchain"

dim_repo = (
    repos
    .dropDuplicates(["repo_id"])
    .withColumn("repo_key",     F.col("repo_id").cast("long"))
    .withColumn("language",     F.coalesce("language", F.lit("Unknown")))
    .withColumn("topics",       F.coalesce("topics",   F.lit("")))
    .withColumn("is_ai_ml_repo",
        (F.lower(F.col("topics")).rlike(AI_PATTERN) |
         F.lower(F.col("repo_name")).rlike(AI_PATTERN)).cast("boolean"))
    .withColumn("created_year", F.year("created_at").cast("short"))
    .withColumn("owner_login",  F.split("repo_name", "/")[0])
    .withColumn("repo_age_days",
        F.datediff(F.current_date(), F.to_date("created_at")))
    .select("repo_key", "repo_id", "repo_name", "owner_login",
            "language", "license", "topics", "stars_at_extract",
            "forks_at_extract", "is_ai_ml_repo", "is_archived",
            "created_year", "description", "has_readme",
            "has_ci", "repo_age_days", "created_at")
)
write_gold(dim_repo, "dim_repository", merge_key="repo_id")

# ================================================================
# dim_contributor
# ================================================================
dim_contrib = (
    contribs
    .withColumn("contributor_key", F.col("actor_id").cast("long"))
    .withColumn("is_bot",
        F.coalesce(
            F.lower(F.col("login")).rlike(r"\[bot\]|bot$|github.actions"),
            F.lit(False)
        ).cast("boolean"))
    .select("contributor_key", "actor_id", "login",
            "is_bot", "account_type")
)
write_gold(dim_contrib, "dim_contributor", merge_key="actor_id")

# ================================================================
# dim_language (dynamic — derives from silver/repositories + defaults)
# ================================================================
# Hardcoded defaults for well-known languages (paradigm, type_system, etc.)
_known_langs = {
    "Python":     ("multi-paradigm", "dynamic", 1991, False),
    "JavaScript": ("multi-paradigm", "dynamic", 1995, False),
    "TypeScript": ("multi-paradigm", "static",  2012, False),
    "Java":       ("OOP",            "static",  1995, False),
    "Go":         ("procedural",     "static",  2009, False),
    "Rust":       ("systems",        "static",  2015, True),
    "C++":        ("multi-paradigm", "static",  1985, False),
    "C":          ("procedural",     "static",  1972, False),
    "Ruby":       ("OOP",            "dynamic", 1995, False),
    "PHP":        ("multi-paradigm", "dynamic", 1994, False),
    "Swift":      ("multi-paradigm", "static",  2014, False),
    "Kotlin":     ("multi-paradigm", "static",  2011, False),
    "Scala":      ("functional",     "static",  2004, False),
    "Shell":      ("scripting",      "dynamic", 1989, False),
    "Jupyter Notebook": ("scripting", "dynamic", 2014, False),
    "Unknown":    ("unknown",        "unknown", None, False),
}

# Discover languages from silver/repositories (dynamic)
repo_langs = (
    repos.select(F.coalesce("language", F.lit("Unknown")).alias("language_name"))
    .distinct()
    .collect()
)
discovered = {row["language_name"] for row in repo_langs}
# Merge with known defaults
all_lang_names = sorted(discovered | set(_known_langs.keys()))

lang_rows = []
for idx, name in enumerate(all_lang_names, start=1):
    if name in _known_langs:
        paradigm, type_sys, first_appeared, is_emerging = _known_langs[name]
    else:
        paradigm, type_sys, first_appeared, is_emerging = ("unknown", "unknown", None, False)
    lang_rows.append((idx, name, paradigm, type_sys, first_appeared, is_emerging))

schema = ["language_key", "language_name", "paradigm",
          "type_system", "first_appeared", "is_emerging"]
dim_lang = spark.createDataFrame(lang_rows, schema)
write_gold(dim_lang, "dim_language", merge_key="language_key")

# ================================================================
# SURROGATE KEY MAPS
# ================================================================
repo_map    = spark.read.format("delta").load(f"{GOLD}/dim_repository")\
                   .select("repo_id", "repo_key", "language")
contrib_map = spark.read.format("delta").load(f"{GOLD}/dim_contributor")\
                   .select("actor_id", "contributor_key")
lang_map    = spark.read.format("delta").load(f"{GOLD}/dim_language")\
                   .select(F.lower("language_name").alias("lang_lower"), "language_key")
date_map    = spark.read.format("delta").load(f"{GOLD}/dim_date")\
                   .select("date_key")

def attach_keys(df, ts_col="created_at"):
    keyed = df.withColumn("date_key",
        F.date_format(F.col(ts_col), "yyyyMMddHH").cast("int"))
    return (
        keyed
        .join(date_map,    "date_key",                                   "left")
        .join(repo_map,    keyed["repo_id"]  == repo_map["repo_id"],     "left")
        .join(contrib_map, keyed["actor_id"] == contrib_map["actor_id"], "left")
        .join(lang_map,
              F.lower(F.col("language")) == lang_map["lang_lower"], "left")
        .withColumn("language_key",
            F.coalesce(F.col("language_key"), F.lit(16)))
    )

# ================================================================
# fact_push_events
# ================================================================
push_fact = (
    attach_keys(push)
    .withColumn("is_default_branch",
        F.col("branch_ref").rlike(r"refs/heads/(main|master)$").cast("boolean"))
    .select("event_id", "date_key", "repo_key", "contributor_key",
            "language_key", "year", "commit_count",
            "branch_ref", "is_default_branch")
)
write_gold(push_fact, "fact_push_events",
           merge_key="event_id", partition_col="year")

# ================================================================
# fact_pr_events
# ================================================================
SIZE_BUCKET = (
    F.when(F.col("total_lines") <=  10, "XS")
     .when(F.col("total_lines") <=  50, "S")
     .when(F.col("total_lines") <= 250, "M")
     .when(F.col("total_lines") <= 1000, "L")
     .otherwise("XL")
)
pr_fact = (
    attach_keys(pr)
    .withColumn("total_lines", F.col("additions") + F.col("deletions"))
    .withColumn("pr_size_bucket", SIZE_BUCKET)
    .withColumn("time_to_merge_hours",
        F.when(F.col("is_merged"),
               F.when(F.col("merged_at") > F.col("pr_created_at"),
                      F.round((F.unix_timestamp("merged_at") -
                               F.unix_timestamp("pr_created_at")) / 3600, 2))))
    .select("event_id", "date_key", "repo_key", "contributor_key",
            "language_key", "year", "action", "pr_id", "is_merged",
            "additions", "deletions", "changed_files", "review_comments",
            "time_to_merge_hours", "pr_size_bucket")
)
write_gold(pr_fact, "fact_pr_events",
           merge_key="event_id", partition_col="year")

# ================================================================
# fact_issues_events
# ================================================================
issues_fact = (
    attach_keys(issues)
    .withColumn("time_to_close_hours",
        F.when(F.col("state") == "closed",
               F.when(F.col("closed_at") > F.col("issue_created_at"),
                      F.round((F.unix_timestamp("closed_at") -
                               F.unix_timestamp("issue_created_at")) / 3600, 2))))
    .select("event_id", "date_key", "repo_key", "contributor_key",
            "year", "action", "issue_id", "label_list",
            "comments_count", "time_to_close_hours")
)
write_gold(issues_fact, "fact_issues_events",
           merge_key="event_id", partition_col="year")

# ================================================================
# fact_repo_activity_daily
# ================================================================
watch_keyed = attach_keys(watches)
fork_keyed  = attach_keys(forks)

push_agg = (
    push_fact
    .withColumn("obs_date",
        F.to_date(F.col("date_key").cast("string"), "yyyyMMddHH"))
    .groupBy("date_key", "obs_date", "repo_key", "language_key", "year")
    .agg(F.count("event_id").alias("total_pushes"),
         F.sum("commit_count").alias("total_commits"),
         F.countDistinct("contributor_key").alias("unique_contributors"))
)
pr_agg = (
    pr_fact.groupBy("date_key", "repo_key")
           .agg(F.sum((F.col("action") == "opened").cast("int"))
                  .alias("total_prs_opened"),
                F.sum(F.col("is_merged").cast("int"))
                  .alias("total_prs_merged"),
                F.avg("time_to_merge_hours").alias("avg_pr_merge_hours"))
)
iss_agg = (
    issues_fact.groupBy("date_key", "repo_key")
               .agg(F.sum((F.col("action") == "opened").cast("int"))
                      .alias("total_issues_opened"),
                    F.sum((F.col("action") == "closed").cast("int"))
                      .alias("total_issues_closed"),
                    F.avg("time_to_close_hours")
                      .alias("avg_issue_close_hours"))
)
star_agg = (
    watch_keyed.groupBy("date_key", "repo_key")
               .agg(F.count("event_id").alias("total_stars"))
)
fork_agg = (
    fork_keyed.groupBy("date_key", "repo_key")
              .agg(F.count("event_id").alias("total_forks"))
)

_DAILY_METRICS = [
    "total_pushes", "total_commits", "unique_contributors",
    "total_prs_opened", "total_prs_merged",
    "total_issues_opened", "total_issues_closed",
    "total_stars", "total_forks",
]
daily = (
    push_agg
    .join(pr_agg,   ["date_key", "repo_key"], "left")
    .join(iss_agg,  ["date_key", "repo_key"], "left")
    .join(star_agg, ["date_key", "repo_key"], "left")
    .join(fork_agg, ["date_key", "repo_key"], "left")
    .fillna(0, subset=_DAILY_METRICS)
)
write_gold(daily, "fact_repo_activity_daily",
           merge_key=["date_key", "repo_key"], partition_col="year")

# ================================================================
# fact_language_yearly
# ================================================================
w_year = Window.partitionBy("year")
w_lang = Window.partitionBy("language_key").orderBy("year")

lang_yearly = (
    push_fact
    .groupBy("language_key", "year")
    .agg(F.count("event_id").alias("total_push_events"),
         F.sum("commit_count").alias("total_commits"),
         F.countDistinct("repo_key").alias("unique_repos"),
         F.countDistinct("contributor_key").alias("unique_contributors"))
    .withColumn("prev_push",
        F.lag("total_push_events", 1).over(w_lang))
    .withColumn("yoy_push_growth_pct",
        F.round(((F.col("total_push_events") / F.col("prev_push")) - 1) * 100, 2))
    .withColumn("total_year_pushes",
        F.sum("total_push_events").over(w_year))
    .withColumn("market_share_pct",
        F.round(F.col("total_push_events") /
                F.col("total_year_pushes") * 100, 3))
    .drop("prev_push", "total_year_pushes")
)
write_gold(lang_yearly, "fact_language_yearly",
           merge_key=["language_key", "year"])

# ================================================================
# OPTIMIZE gold tables (OSS Delta — no ZORDER)
# ================================================================
gold_tables = [
    "dim_date", "dim_repository", "dim_contributor", "dim_language",
    "fact_push_events", "fact_pr_events", "fact_issues_events",
    "fact_repo_activity_daily", "fact_language_yearly",
]
for tbl in gold_tables:
    path = f"{GOLD}/{tbl}"
    if DeltaTable.isDeltaTable(spark, path):
        spark.sql(f"OPTIMIZE delta.`{path}`")
        DeltaTable.forPath(spark, path).vacuum(retentionHours=168)

# ================================================================
# BIGQUERY WRITE (every run)
# Uses the Spark-BigQuery connector pre-installed on Dataproc 2.1.
# Power BI connects to these BigQuery tables directly.
#
# Dimension tables use overwrite (small, idempotent).
# Fact tables use overwrite as well since Delta gold is the source
# of truth — BQ is a read-only mirror for Power BI.
# Each write is tagged with pipeline_run_id for auditability.
# ================================================================
import uuid as _uuid
_pipeline_run_id = str(_uuid.uuid4())

print(f"[gold] Writing gold tables to BigQuery dataset {project_id}.{bq_dataset}")
print(f"[gold] pipeline_run_id = {_pipeline_run_id}")

bq_tables = [
    "dim_date", "dim_repository", "dim_contributor", "dim_language",
    "fact_push_events", "fact_pr_events", "fact_issues_events",
    "fact_repo_activity_daily", "fact_language_yearly",
]
for tbl in bq_tables:
    df = (spark.read.format("delta").load(f"{GOLD}/{tbl}")
          .withColumn("_pipeline_run_id", F.lit(_pipeline_run_id))
          .withColumn("_loaded_at", F.current_timestamp()))
    (df.write
       .format("bigquery")
       .option("table",              f"{project_id}.{bq_dataset}.{tbl}")
       .option("temporaryGcsBucket", f"{prefix}-gold")
       .mode("overwrite")
       .save())
    print(f"  [gold→BQ] {bq_dataset}.{tbl}")

print("[gold] gold layer complete")
