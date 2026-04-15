# ================================================================
# ml_score.py  —  Dataproc PySpark job
#
# PURPOSE: Load the saved abandonment-risk model and score
#          ALL repos in the gold layer.
#          Runs on EVERY pipeline trigger (file upload + monthly).
#          Completes in ~2 min — no retraining.
#
# ARGS:
#   --prefix      GCS bucket prefix
#   --project_id  GCP project ID
# ================================================================

import argparse
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

# ml_features.py is uploaded alongside this script to GCS scripts bucket

# ── Parse arguments ──────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--prefix",     required=True)
parser.add_argument("--project_id", required=True)
parser.add_argument("--bq_dataset", default="github_oss_gold")
args = parser.parse_args()

prefix     = args.prefix
project_id = args.project_id
bq_dataset = args.bq_dataset

GOLD         = f"gs://{prefix}-gold"
CURRENT_YEAR = datetime.now(timezone.utc).year

# ── Spark session ────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("ml_score")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile(f"gs://{prefix}-scripts/ml_features.py")
from ml_features import build_features, NUMERICAL_COLS

# ================================================================
# LOAD MODEL POINTER
# ================================================================
_pointer_path = f"{GOLD}/ml_model_current"
try:
    _ptr          = spark.read.format("delta").load(_pointer_path).collect()[0]
    MODEL_URI     = _ptr["model_path"]
    MODEL_VERSION = _ptr["model_version"]
except Exception:
    MODEL_URI = MODEL_VERSION = None

print(f"[score] model version = {MODEL_VERSION}")
print(f"[score] model path    = {MODEL_URI}")

if not MODEL_URI:
    print("[score] No saved model found — ml_train has not run yet. Skipping.")
    sys.exit(0)

# ── Verify the model path exists in GCS ─────────────────────
from google.cloud import storage as gcs_client
_bucket_name = MODEL_URI.replace("gs://", "").split("/")[0]
_blob_prefix = "/".join(MODEL_URI.replace("gs://", "").split("/")[1:]) + "/"
_bucket      = gcs_client.Client(project=project_id).bucket(_bucket_name)
if not any(True for _ in _bucket.list_blobs(prefix=_blob_prefix, max_results=1)):
    print(f"[score] Model path {MODEL_URI} not found. Run ml_train first.")
    sys.exit(0)

# ================================================================
# LOAD SAVED MODEL
# ================================================================
print("[score] Loading saved model...")
model = PipelineModel.load(MODEL_URI)
print("[score] Model loaded.")

# ================================================================
# LOAD GOLD TABLES + FEATURE ENGINEERING
# (shared with ml_train.py via ml_features.py)
# ================================================================
dim_repo = spark.read.format("delta").load(f"{GOLD}/dim_repository")

TRAJECTORY_BOUNDARY = CURRENT_YEAR - 4

repo_meta = (
    dim_repo
    .select("repo_key", "language", "license", "is_ai_ml_repo",
            "has_readme", "has_ci", "created_year", "repo_age_days",
            "stars_at_extract", "is_archived",
            F.to_date("created_at").alias("repo_created_date"))
    .filter(F.col("repo_created_date").isNotNull())
)

score_features = (
    build_features(
        spark, GOLD,
        repo_meta.drop("is_archived", "stars_at_extract"),
        trajectory_boundary=TRAJECTORY_BOUNDARY,
    )
    .filter(F.col("repo_age_days").isNotNull())
)

total_repos = score_features.count()
print(f"[score] Repos to score: {total_repos:,}")

# ================================================================
# APPLY MODEL + BUILD OUTPUT
# ================================================================
predictions = model.transform(score_features)

display_cols = dim_repo.select("repo_key", "is_archived")

pred_df = (
    predictions
    .select(
        "repo_key",
        F.lit(MODEL_VERSION).alias("model_version"),
        F.round(vector_to_array(F.col("probability"))[1], 4).alias("abandonment_probability"),
        F.when(vector_to_array(F.col("probability"))[1] >= 0.70, "High Risk")
         .when(vector_to_array(F.col("probability"))[1] >= 0.40, "Moderate Risk")
         .when(F.col("repo_age_days") < 730,    "Too Young")
         .otherwise("Low Risk")
         .alias("risk_tier"),
        "created_year",
        F.current_timestamp().alias("scored_at"),
    )
    .join(display_cols, "repo_key", "left")
)

momentum_labels = score_features.select(
    "repo_key",
    F.when(F.col("commit_momentum") >= 1.5, "Accelerating")
     .when(F.col("commit_momentum") >= 0.5, "Stable")
     .when(F.col("commit_momentum") >= 0.1, "Declining")
     .otherwise("Stalled")
     .alias("trajectory_label"),
)
pred_df = pred_df.join(momentum_labels, "repo_key", "left")

# ================================================================
# DELTA MERGE — idempotent upsert
# ================================================================
out_path = f"{GOLD}/ml_abandonment_risk"

if DeltaTable.isDeltaTable(spark, out_path):
    (DeltaTable.forPath(spark, out_path).alias("t")
               .merge(pred_df.alias("s"),
                      "t.repo_key = s.repo_key AND t.model_version = s.model_version")
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())
    print(f"[score] MERGE complete — {total_repos:,} repos scored")
else:
    (pred_df.write.format("delta")
            .partitionBy("created_year")
            .mode("overwrite")
            .save(out_path))
    print(f"[score] Initial write — {total_repos:,} repos scored")

# ================================================================
# BIGQUERY WRITE — ml_abandonment_risk for Power BI
# ================================================================
print(f"[score] Writing ml_abandonment_risk to BigQuery {project_id}.{bq_dataset}")
score_bq = spark.read.format("delta").load(out_path)
(score_bq.write
         .format("bigquery")
         .option("table",              f"{project_id}.{bq_dataset}.ml_abandonment_risk")
         .option("temporaryGcsBucket", f"{prefix}-gold")
         .mode("overwrite")
         .save())
print("[score] ml_abandonment_risk written to BigQuery")

print("[score] scoring complete")
