# ================================================================
# ml_trajectory.py  —  Dataproc PySpark job
#
# PURPOSE: K-Means clustering of repo activity trajectories (2016-2025).
# ARGS:
#   --prefix      GCS bucket prefix
#   --project_id  GCP project ID
# RUNS: Monthly only.
# ================================================================

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline

# ── Parse arguments ──────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--prefix",     required=True)
parser.add_argument("--project_id", required=True)
parser.add_argument("--bq_dataset", default="github_oss_gold")
args = parser.parse_args()

GOLD       = f"gs://{args.prefix}-gold"
prefix     = args.prefix
project_id = args.project_id
bq_dataset = args.bq_dataset

# ── Spark session ────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("ml_trajectory")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"[trajectory] GOLD = {GOLD}")

# ================================================================
# LOAD + BUILD YEARLY ACTIVITY VECTOR
# ================================================================
fact_daily = spark.read.format("delta").load(f"{GOLD}/fact_repo_activity_daily")

YEARS = list(range(2016, 2026))

yearly_commits = (
    fact_daily
    .groupBy("repo_key", "year")
    .agg(F.sum("total_commits").alias("commits"))
)

pivoted = (
    yearly_commits
    .groupBy("repo_key")
    .pivot("year", YEARS)
    .agg(F.first("commits"))
    .fillna(0)
)

year_cols = [str(y) for y in YEARS]

years_observed = (
    yearly_commits
    .groupBy("repo_key")
    .agg(F.countDistinct("year").alias("years_observed"))
)

pivoted = pivoted.join(years_observed, "repo_key").filter(F.col("years_observed") >= 2)
n_repos = pivoted.count()
print(f"[trajectory] Repos with >=2 years of data: {n_repos:,}")

if n_repos < 4:
    print(f"[trajectory] Not enough repos ({n_repos}) for trajectory clustering. "
          "Need at least 4. Skipping.")
    spark.stop()
    sys.exit(0)

# ================================================================
# ML PIPELINE: assemble → min-max scale → K-Means
# Use silhouette analysis to find optimal k (test k=2..max_k)
# ================================================================
assembler = VectorAssembler(inputCols=year_cols, outputCol="features_raw",
                            handleInvalid="keep")
scaler    = MinMaxScaler(inputCol="features_raw", outputCol="features")

# Pre-compute scaled features for silhouette evaluation
prep_pipeline = Pipeline(stages=[assembler, scaler])
prep_model    = prep_pipeline.fit(pivoted)
scaled_data   = prep_model.transform(pivoted)

# Cap max k at n_repos - 1 (KMeans needs k < n)
max_k = min(6, n_repos - 1)

evaluator = ClusteringEvaluator(
    featuresCol="features", predictionCol="cluster_id",
    metricName="silhouette", distanceMeasure="squaredEuclidean")

best_k = min(4, max_k)
best_score = -1.0
print(f"[trajectory] Silhouette analysis (k=2..{max_k}):")
for k in range(2, max_k + 1):
    km = KMeans(k=k, seed=42, featuresCol="features",
                predictionCol="cluster_id", maxIter=50)
    km_model = km.fit(scaled_data)
    preds = km_model.transform(scaled_data)
    score = evaluator.evaluate(preds)
    print(f"  k={k}  silhouette={score:.4f}")
    if score > best_score:
        best_score = score
        best_k = k

print(f"[trajectory] Best k={best_k} (silhouette={best_score:.4f})")

kmeans = KMeans(k=best_k, seed=42, featuresCol="features",
                predictionCol="cluster_id", maxIter=50)

traj_pipeline = Pipeline(stages=[assembler, scaler, kmeans])
traj_model    = traj_pipeline.fit(pivoted)
clustered     = traj_model.transform(pivoted)

# ================================================================
# LABEL CLUSTERS BY GROWTH RATIO
# ================================================================
early_year_cols  = [str(y) for y in range(2016, 2021)]
recent_year_cols = [str(y) for y in range(2021, 2026)]

clustered = (
    clustered
    .withColumn("avg_commits_early",
        (sum([F.col(c) for c in early_year_cols], F.lit(0))) / len(early_year_cols))
    .withColumn("avg_commits_recent",
        (sum([F.col(c) for c in recent_year_cols], F.lit(0))) / len(recent_year_cols))
    .withColumn("growth_ratio",
        F.col("avg_commits_recent") / (F.col("avg_commits_early") + 1.0))
)

cluster_stats = (
    clustered
    .groupBy("cluster_id")
    .agg(
        F.avg("growth_ratio").alias("mean_growth_ratio"),
        F.count("repo_key").alias("cluster_size"),
    )
    .orderBy("mean_growth_ratio", ascending=False)
)
print("[trajectory] Cluster summary:")
cluster_stats.show()

stats_rows = cluster_stats.collect()
label_map  = {}
# Assign labels based on rank order (highest growth first).
# Expand or contract labels to match actual k.
all_labels = ["Viral", "Established", "Stable", "Declining", "Dormant", "Inactive"]
labels     = all_labels[:len(stats_rows)]
for rank, row in enumerate(stats_rows):
    label_map[row["cluster_id"]] = labels[rank]
    print(f"  cluster {row['cluster_id']} → {labels[rank]}  "
          f"(growth={row['mean_growth_ratio']:.2f}, n={row['cluster_size']})")

label_map_expr = F.create_map(
    *[item for pair in
      [(F.lit(k), F.lit(v)) for k, v in label_map.items()]
      for item in pair]
)

result_df = (
    clustered
    .withColumn("trajectory_label", label_map_expr[F.col("cluster_id")])
    .select(
        "repo_key", "cluster_id", "trajectory_label",
        F.round("growth_ratio",       3).alias("growth_ratio"),
        F.round("avg_commits_early",  1).alias("avg_commits_early"),
        F.round("avg_commits_recent", 1).alias("avg_commits_recent"),
        "years_observed",
        F.current_timestamp().alias("computed_at"),
    )
)

# ================================================================
# WRITE TO GOLD
# ================================================================
out_path = f"{GOLD}/ml_repo_trajectories"
(result_df.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").save(out_path))

n_written = result_df.count()
print(f"[trajectory] Written {n_written:,} rows → gold/ml_repo_trajectories")
result_df.groupBy("trajectory_label").count().orderBy("trajectory_label").show()
# ================================================================
# BIGQUERY WRITE — ml_repo_trajectories for Power BI
# ================================================================
print(f"[trajectory] Writing ml_repo_trajectories to BigQuery {project_id}.{bq_dataset}")
(result_df.write
          .format("bigquery")
          .option("table",              f"{project_id}.{bq_dataset}.ml_repo_trajectories")
          .option("temporaryGcsBucket", f"{prefix}-gold")
          .mode("overwrite")
          .save())
print("[trajectory] ml_repo_trajectories written to BigQuery")

print("[trajectory] trajectory clustering complete")
