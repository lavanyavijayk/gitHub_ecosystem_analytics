# ================================================================
# ml_language_trends.py  —  Dataproc PySpark job
#
# PURPOSE: Per-language linear trend model on 10 years of
#          market-share data and forecast 2026-2027.
# ARGS:
#   --prefix      GCS bucket prefix
#   --project_id  GCP project ID
# RUNS: Monthly only.
# ================================================================

import argparse
import sys

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType
)

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
    .appName("ml_language_trends")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"[lang_trends] GOLD = {GOLD}")

# ================================================================
# LOAD DATA
# ================================================================
lang_yearly = spark.read.format("delta").load(f"{GOLD}/fact_language_yearly")
dim_lang    = spark.read.format("delta").load(f"{GOLD}/dim_language")

# Require at least 3 years of observations per language
lang_counts = lang_yearly.groupBy("language_key").count()
lang_yearly = lang_yearly.join(
    lang_counts.filter(F.col("count") >= 3).select("language_key"),
    "language_key"
)
print(f"[lang_trends] Rows for modelling: {lang_yearly.count():,}")

# ================================================================
# PER-LANGUAGE LINEAR REGRESSION via applyInPandas
# scikit-learn is pre-installed on Dataproc 2.1
# ================================================================
output_schema = StructType([
    StructField("language_key",        IntegerType(), False),
    StructField("slope_pct_per_year",  DoubleType(),  True),
    StructField("r_squared",           DoubleType(),  True),
    StructField("forecast_2026_share", DoubleType(),  True),
    StructField("forecast_2027_share", DoubleType(),  True),
    StructField("trend_label",         StringType(),  True),
])


def fit_language_trend(pdf: pd.DataFrame) -> pd.DataFrame:
    from sklearn.linear_model import LinearRegression
    import numpy as np

    lang_key = int(pdf["language_key"].iloc[0])
    pdf      = pdf.sort_values("year")

    X = pdf[["year"]].values.astype(float)
    y = pdf["market_share_pct"].values.astype(float)

    if len(pdf) < 3 or y.std() == 0:
        return pd.DataFrame()

    model = LinearRegression().fit(X, y)
    slope = float(model.coef_[0])
    r2    = float(model.score(X, y))
    f2026 = max(0.0, float(model.predict([[2026]])[0]))
    f2027 = max(0.0, float(model.predict([[2027]])[0]))

    if   slope >  0.5: label = "Strongly Growing"
    elif slope >  0.1: label = "Growing"
    elif slope > -0.1: label = "Stable"
    elif slope > -0.5: label = "Declining"
    else:              label = "Strongly Declining"

    return pd.DataFrame([{
        "language_key":        lang_key,
        "slope_pct_per_year":  round(slope, 4),
        "r_squared":           round(r2,    4),
        "forecast_2026_share": round(f2026, 4),
        "forecast_2027_share": round(f2027, 4),
        "trend_label":         label,
    }])


trend_df = (
    lang_yearly
    .groupBy("language_key")
    .applyInPandas(fit_language_trend, schema=output_schema)
)

result_df = (
    trend_df
    .join(dim_lang.select("language_key", "language_name"), "language_key", "left")
    .withColumn("computed_at", F.current_timestamp())
    .orderBy(F.col("slope_pct_per_year").desc())
)

# ================================================================
# WRITE TO GOLD
# ================================================================
out_path = f"{GOLD}/ml_language_trend_forecasts"
(result_df.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").save(out_path))

n_written = result_df.count()
print(f"[lang_trends] Written {n_written} language forecasts")
result_df.select(
    "language_name", "slope_pct_per_year", "r_squared",
    "forecast_2026_share", "trend_label"
).show(20, truncate=False)

# ================================================================
# BIGQUERY WRITE — ml_language_trend_forecasts for Power BI
# ================================================================
print(f"[lang_trends] Writing ml_language_trend_forecasts to BigQuery {project_id}.{bq_dataset}")
(result_df.write
          .format("bigquery")
          .option("table",              f"{project_id}.{bq_dataset}.ml_language_trend_forecasts")
          .option("temporaryGcsBucket", f"{prefix}-gold")
          .mode("overwrite")
          .save())
print("[lang_trends] ml_language_trend_forecasts written to BigQuery")

print("[lang_trends] language trend forecasting complete")
