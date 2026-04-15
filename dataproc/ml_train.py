# ================================================================
# ml_train.py  —  Dataproc PySpark job
#
# PURPOSE: Train a Random Forest classifier to predict repository
#          abandonment. Saves versioned model to GCS gold bucket.
#
# ARGS:
#   --prefix      GCS bucket prefix
#   --project_id  GCP project ID
#
# RUNS: Monthly only (called by monthly Cloud Workflow).
# ================================================================

import argparse
import sys
import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder,
    StandardScaler, Imputer
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
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

GOLD          = f"gs://{prefix}-gold"
MODEL_VERSION = datetime.now(timezone.utc).strftime("%Y%m")
MODEL_URI     = f"{GOLD}/ml_model_{MODEL_VERSION}"

# ── Spark session ────────────────────────────────────────────
spark = (SparkSession.builder
    .appName("ml_train")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

spark.sparkContext.addPyFile(f"gs://{prefix}-scripts/ml_features.py")
from ml_features import build_features, NUMERICAL_COLS, BINARY_COLS, IMPUTED_COLS

print(f"[train] prefix        = {prefix}")
print(f"[train] model_version = {MODEL_VERSION}")
print(f"[train] model_uri     = {MODEL_URI}")

# ================================================================
# TEMPORAL SPLIT
# ================================================================
CURRENT_YEAR = datetime.now(timezone.utc).year
TRAIN_CUTOFF = CURRENT_YEAR - 4
TEST_MAX     = CURRENT_YEAR - 2

print(f"[train] train: created_year<={TRAIN_CUTOFF}  "
      f"test: {TRAIN_CUTOFF+1}–{TEST_MAX}")

# ================================================================
# LOAD GOLD TABLES + LABEL COMPUTATION
# ================================================================
dim_repo   = spark.read.format("delta").load(f"{GOLD}/dim_repository")
fact_daily = spark.read.format("delta").load(f"{GOLD}/fact_repo_activity_daily")

last_active = (
    fact_daily
    .groupBy("repo_key")
    .agg(
        F.max("year").alias("last_active_year"),
    )
)

repo_base = (
    dim_repo
    .select("repo_key", "language", "license", "is_ai_ml_repo",
            "has_readme", "has_ci", "created_year", "repo_age_days",
            "is_archived",
            F.to_date("created_at").alias("repo_created_date"))
    .filter(F.col("repo_created_date").isNotNull())
    .filter(F.col("repo_age_days").isNotNull())
    .join(last_active, "repo_key", "left")
    .withColumn("label",
        F.when(F.col("is_archived") == True, F.lit(1))
         .when(
             (F.col("repo_age_days") >= 730) &
             (F.col("last_active_year") < (CURRENT_YEAR - 2)),
             F.lit(1))
         .when(F.col("repo_age_days") < 730, F.lit(None).cast("int"))
         .otherwise(F.lit(0)))
    .drop("is_archived", "last_active_year")
    .filter(F.col("label").isNotNull())
)

print(f"[train] Labelled repos: {repo_base.count():,}")
repo_base.groupBy("label").count().show()

# ================================================================
# FEATURE ENGINEERING (shared with ml_score.py via ml_features.py)
# ================================================================
features_df = build_features(
    spark, GOLD, repo_base, trajectory_boundary=2020
).cache()

total_rows = features_df.count()
print(f"[train] Feature rows: {total_rows:,}")

pos_count    = features_df.filter(F.col("label") == 1).count()
neg_count    = features_df.filter(F.col("label") == 0).count()
weight_ratio = neg_count / max(pos_count, 1)
print(f"[train] Abandoned: {pos_count:,}  Active: {neg_count:,}  "
      f"Weight ratio: {weight_ratio:.2f}")

features_df = features_df.withColumn(
    "class_weight",
    F.when(F.col("label") == 1, weight_ratio).otherwise(F.lit(1.0))
)

# ================================================================
# ML PIPELINE
# ================================================================
features_df = features_df.fillna(0, subset=NUMERICAL_COLS)

imputer   = Imputer(inputCols=NUMERICAL_COLS, outputCols=IMPUTED_COLS,
                    strategy="median")
lang_idx  = StringIndexer(inputCol="language", outputCol="lang_idx",
                           handleInvalid="keep")
lic_idx   = StringIndexer(inputCol="license",  outputCol="lic_idx",
                           handleInvalid="keep")
lang_ohe  = OneHotEncoder(inputCol="lang_idx", outputCol="lang_vec",
                           handleInvalid="keep")
lic_ohe   = OneHotEncoder(inputCol="lic_idx",  outputCol="lic_vec",
                           handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=IMPUTED_COLS + BINARY_COLS + ["lang_vec", "lic_vec"],
    outputCol="features_raw", handleInvalid="keep")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features",
                           withMean=True, withStd=True)
rf        = RandomForestClassifier(
    featuresCol="features", labelCol="label",
    weightCol="class_weight",
    numTrees=100, maxDepth=8, minInstancesPerNode=5,
    featureSubsetStrategy="sqrt", seed=42)

pipeline = Pipeline(stages=[
    imputer, lang_idx, lic_idx, lang_ohe, lic_ohe,
    assembler, scaler, rf
])

# ================================================================
# TEMPORAL SPLIT + CROSS VALIDATION
# ================================================================
train_df = features_df.filter(F.col("created_year") <= TRAIN_CUTOFF)
test_df  = features_df.filter(
    (F.col("created_year") > TRAIN_CUTOFF) &
    (F.col("created_year") <= TEST_MAX))

print(f"[train] Train rows: {train_df.count():,}  "
      f"Test rows: {test_df.count():,}")

param_grid = (ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 200])
    .addGrid(rf.maxDepth, [5, 8, 12])
    .build())

auc_eval = BinaryClassificationEvaluator(
    labelCol="label", metricName="areaUnderROC")
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=param_grid,
                    evaluator=auc_eval, numFolds=3, seed=42)

# ================================================================
# TRAIN → EVALUATE → SAVE
# ================================================================
trained_at_str = datetime.now(timezone.utc).isoformat()
print("[train] Training...")
cv_model   = cv.fit(train_df)
best_model = cv_model.bestModel
preds      = best_model.transform(test_df)

auc = auc_eval.evaluate(preds)
f1  = MulticlassClassificationEvaluator(labelCol="label", metricName="f1").evaluate(preds)
acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy").evaluate(preds)
print(f"[train] AUC={auc:.4f}  F1={f1:.4f}  Acc={acc:.4f}")

best_rf   = best_model.stages[-1]
num_trees = best_rf.getNumTrees
max_depth = best_rf.getMaxDepth()

best_model.write().overwrite().save(MODEL_URI)
print(f"[train] Model saved → {MODEL_URI}")

run_id = str(uuid.uuid4())

pointer_df = spark.createDataFrame([{
    "model_version": MODEL_VERSION,
    "model_path":    MODEL_URI,
    "model_type":    "abandonment_risk",
    "run_id":        run_id,
    "trained_at":    trained_at_str,
}])
(pointer_df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true")
           .save(f"{GOLD}/ml_model_current"))

test_preds_df = preds.select(
    "repo_key",
    F.lit(MODEL_VERSION).alias("model_version"),
    "label",
    F.col("prediction").cast("int").alias("predicted_label"),
    F.round(vector_to_array(F.col("probability"))[1], 4).alias("abandonment_probability"),
    F.when((F.col("label")==1) & (F.col("prediction")==1), "True Positive")
     .when((F.col("label")==0) & (F.col("prediction")==0), "True Negative")
     .when((F.col("label")==0) & (F.col("prediction")==1), "False Positive")
     .when((F.col("label")==1) & (F.col("prediction")==0), "False Negative")
     .alias("outcome"),
    F.lit(trained_at_str).alias("trained_at"),
)
(test_preds_df.write.format("delta").mode("append")
              .save(f"{GOLD}/ml_test_predictions"))

meta_df = spark.createDataFrame([{
    "trained_at": trained_at_str, "model_version": MODEL_VERSION,
    "model_type": "abandonment_risk", "run_id": run_id,
    "auc_roc": float(auc), "f1": float(f1), "accuracy": float(acc),
    "num_trees": int(num_trees), "max_depth": int(max_depth),
    "train_cutoff": int(TRAIN_CUTOFF), "test_max": int(TEST_MAX),
    "train_rows": int(train_df.count()), "test_rows": int(test_df.count()),
    "model_path": MODEL_URI,
}])
(meta_df.write.format("delta").mode("append")
        .save(f"{GOLD}/ml_model_metadata"))

fi_pairs = list(zip(
    IMPUTED_COLS + BINARY_COLS,
    best_rf.featureImportances.toArray()[:len(IMPUTED_COLS + BINARY_COLS)]
))
fi_df = spark.createDataFrame(
    [(n, float(s), MODEL_VERSION)
     for n, s in sorted(fi_pairs, key=lambda x: -x[1])],
    ["feature_name", "importance", "model_version"]
)
(fi_df.write.format("delta").mode("append")
      .save(f"{GOLD}/ml_feature_importance"))

# ================================================================
# BIGQUERY WRITE — ml_feature_importance + ml_model_metadata
# (ml_score writes ml_abandonment_risk; train writes these two)
# ================================================================
print(f"[train] Writing ml_feature_importance to BigQuery {project_id}.{bq_dataset}")
fi_bq = spark.read.format("delta").load(f"{GOLD}/ml_feature_importance")
(fi_bq.write
      .format("bigquery")
      .option("table",              f"{project_id}.{bq_dataset}.ml_feature_importance")
      .option("temporaryGcsBucket", f"{prefix}-gold")
      .mode("overwrite")
      .save())
print("[train] ml_feature_importance written to BigQuery")

print(f"[train] Writing ml_model_metadata to BigQuery {project_id}.{bq_dataset}")
meta_bq = spark.read.format("delta").load(f"{GOLD}/ml_model_metadata")
(meta_bq.write
        .format("bigquery")
        .option("table",              f"{project_id}.{bq_dataset}.ml_model_metadata")
        .option("temporaryGcsBucket", f"{prefix}-gold")
        .mode("overwrite")
        .save())
print("[train] ml_model_metadata written to BigQuery")

print("[train] abandonment risk training complete")
