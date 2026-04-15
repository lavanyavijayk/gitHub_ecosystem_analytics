# ================================================================
# ml_features.py  —  Shared feature engineering for ML pipeline
#
# PURPOSE: Single source of truth for feature engineering used by
#          both ml_train.py and ml_score.py. Prevents train/score
#          divergence bugs.
#
# USAGE:
#   spark.sparkContext.addPyFile(f"gs://{prefix}-scripts/ml_features.py")
#   from ml_features import build_features, NUMERICAL_COLS, ...
# ================================================================

from pyspark.sql import DataFrame, SparkSession, functions as F

# ── Column lists (must match ML pipeline expectations) ───────────
NUMERICAL_COLS = [
    "early_commits", "early_pushes", "early_stars", "early_forks",
    "early_prs_opened", "early_prs_merged", "early_issues",
    "avg_daily_contributors", "active_days_in_90",
    "unique_branches", "default_branch_push_ratio",
    "avg_pr_additions", "avg_review_comments",
    "total_unique_contributors", "pr_merge_rate",
    "fork_to_star_ratio", "commits_per_active_day",
    "repo_age_days", "created_year",
    "commit_momentum", "avg_commits_early", "avg_commits_recent",
    "years_observed", "total_active_days",
]
CATEGORICAL_COLS = ["language", "license"]
BINARY_COLS = ["is_ai_ml_repo", "has_readme", "has_ci"]
IMPUTED_COLS = [f"{c}_imp" for c in NUMERICAL_COLS]


def build_features(
    spark: SparkSession,
    gold_path: str,
    repo_df: DataFrame,
    trajectory_boundary: int = 2020,
) -> DataFrame:
    """Build the feature DataFrame for abandonment-risk prediction.

    Args:
        spark:                Active SparkSession.
        gold_path:            GCS path to gold bucket (e.g. "gs://ghoss-gold").
        repo_df:              DataFrame with at least: repo_key, repo_created_date,
                              language, license, is_ai_ml_repo, has_readme, has_ci,
                              created_year, repo_age_days.  May also include 'label'.
        trajectory_boundary:  Year that splits "early era" vs "recent era" for
                              commit momentum.  ml_train uses 2020; ml_score uses
                              CURRENT_YEAR - 4.

    Returns:
        DataFrame with all feature columns attached to repo_df (minus
        repo_created_date).
    """
    # ── Load gold fact tables ────────────────────────────────────
    dim_contrib = spark.read.format("delta").load(f"{gold_path}/dim_contributor")
    fact_daily  = spark.read.format("delta").load(f"{gold_path}/fact_repo_activity_daily")
    fact_push   = spark.read.format("delta").load(f"{gold_path}/fact_push_events")
    fact_pr     = spark.read.format("delta").load(f"{gold_path}/fact_pr_events")

    # ── Add obs_date for temporal filtering ──────────────────────
    fact_daily_dated = fact_daily.withColumn(
        "obs_date",
        F.coalesce(
            F.col("obs_date"),
            F.to_date(F.col("date_key").cast("string"), "yyyyMMddHH"),
        ),
    )
    fact_push_dated = fact_push.withColumn(
        "obs_date", F.to_date(F.col("date_key").cast("string"), "yyyyMMddHH")
    )
    fact_pr_dated = fact_pr.withColumn(
        "obs_date", F.to_date(F.col("date_key").cast("string"), "yyyyMMddHH")
    )

    repo_dates = repo_df.select("repo_key", "repo_created_date")

    # ── Early activity (first 90 days) ───────────────────────────
    early_activity = (
        fact_daily_dated.join(repo_dates, "repo_key")
        .filter(F.col("obs_date") <= F.date_add("repo_created_date", 90))
        .groupBy("repo_key")
        .agg(
            F.sum("total_commits").alias("early_commits"),
            F.sum("total_pushes").alias("early_pushes"),
            F.sum("total_stars").alias("early_stars"),
            F.sum("total_forks").alias("early_forks"),
            F.sum("total_prs_opened").alias("early_prs_opened"),
            F.sum("total_prs_merged").alias("early_prs_merged"),
            F.sum("total_issues_opened").alias("early_issues"),
            F.avg("unique_contributors").alias("avg_daily_contributors"),
            F.count("date_key").alias("active_days_in_90"),
        )
    )

    # ── Push features (first 90 days) ────────────────────────────
    push_features = (
        fact_push_dated.join(repo_dates, "repo_key")
        .filter(F.col("obs_date") <= F.date_add("repo_created_date", 90))
        .groupBy("repo_key")
        .agg(
            F.countDistinct("branch_ref").alias("unique_branches"),
            F.avg(F.col("is_default_branch").cast("int")).alias(
                "default_branch_push_ratio"
            ),
        )
    )

    # ── PR features (first 90 days) ──────────────────────────────
    pr_features = (
        fact_pr_dated.join(repo_dates, "repo_key")
        .filter(F.col("obs_date") <= F.date_add("repo_created_date", 90))
        .groupBy("repo_key")
        .agg(
            F.avg("additions").alias("avg_pr_additions"),
            F.avg("review_comments").alias("avg_review_comments"),
            F.sum(F.col("is_merged").cast("int")).alias("early_merged_prs"),
        )
    )

    # ── Contributor quality (first 90 days, non-bots) ────────────
    contrib_quality = (
        fact_push_dated.join(repo_dates, "repo_key")
        .filter(F.col("obs_date") <= F.date_add("repo_created_date", 90))
        .join(
            dim_contrib.select("contributor_key", "is_bot"),
            "contributor_key",
            "left",
        )
        .filter(F.col("is_bot") == False)
        .groupBy("repo_key")
        .agg(
            F.countDistinct("contributor_key").alias(
                "total_unique_contributors"
            ),
        )
    )

    # ── Trajectory / commit momentum ─────────────────────────────
    early_era = fact_daily_dated.filter(F.col("year") <= trajectory_boundary)
    recent_era = fact_daily_dated.filter(F.col("year") > trajectory_boundary)

    trajectory = (
        early_era.groupBy("repo_key")
        .agg(F.avg("total_commits").alias("avg_commits_early"))
        .join(
            recent_era.groupBy("repo_key").agg(
                F.avg("total_commits").alias("avg_commits_recent")
            ),
            "repo_key",
            "full",
        )
        .withColumn(
            "commit_momentum",
            F.coalesce(F.col("avg_commits_recent"), F.lit(0.0))
            / (F.coalesce(F.col("avg_commits_early"), F.lit(0.0)) + 1.0),
        )
        .select(
            "repo_key", "commit_momentum",
            "avg_commits_early", "avg_commits_recent",
        )
    )

    # ── Lifetime stats ───────────────────────────────────────────
    lifetime = fact_daily_dated.groupBy("repo_key").agg(
        F.countDistinct("date_key").alias("total_active_days"),
        F.countDistinct("year").alias("years_observed"),
    )

    # ── Assemble all features ────────────────────────────────────
    features_df = (
        repo_df.drop("repo_created_date")
        .join(early_activity, "repo_key", "left")
        .join(push_features, "repo_key", "left")
        .join(pr_features, "repo_key", "left")
        .join(contrib_quality, "repo_key", "left")
        .join(trajectory, "repo_key", "left")
        .join(lifetime, "repo_key", "left")
        .withColumn(
            "pr_merge_rate",
            F.coalesce(F.col("early_merged_prs"), F.lit(0))
            / (F.coalesce(F.col("early_prs_opened"), F.lit(0)) + 1),
        )
        .withColumn(
            "fork_to_star_ratio",
            F.coalesce(F.col("early_forks"), F.lit(0))
            / (F.coalesce(F.col("early_stars"), F.lit(0)) + 1),
        )
        .withColumn(
            "commits_per_active_day",
            F.coalesce(F.col("early_commits"), F.lit(0))
            / (F.coalesce(F.col("active_days_in_90"), F.lit(0)) + 1),
        )
    )

    return features_df
