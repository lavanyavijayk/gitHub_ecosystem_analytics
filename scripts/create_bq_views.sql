-- ================================================================
-- create_bq_views.sql
--
-- Creates all 8 analysis views in BigQuery for Power BI.
-- Run in BigQuery console or via:
--   bq query --use_legacy_sql=false < scripts/create_bq_views.sql
--
-- Replace PROJECT_ID below with your actual GCP project ID.
-- Dataset: github_oss_gold (must exist — created by terraform apply)
-- ================================================================

-- ----------------------------------------------------------------
-- Dashboard 1: Programming Language Evolution
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_language_evolution` AS
SELECT
    l.language_name,
    f.year,
    f.total_push_events,
    f.total_commits,
    f.unique_repos,
    f.unique_contributors,
    f.market_share_pct,
    f.yoy_push_growth_pct
FROM `PROJECT_ID.github_oss_gold.fact_language_yearly`  f
JOIN `PROJECT_ID.github_oss_gold.dim_language`          l ON f.language_key = l.language_key
WHERE l.language_name != 'Unknown'
  AND f.year BETWEEN 2016 AND 2025;

-- ----------------------------------------------------------------
-- Dashboard 2: AI/ML Boom Effect
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_ai_boom` AS
SELECT
    d.year,
    r.is_ai_ml_repo,
    COUNT(DISTINCT f.repo_key)   AS unique_repos,
    SUM(f.total_pushes)          AS total_pushes,
    SUM(f.total_stars)           AS total_stars,
    SUM(f.total_forks)           AS total_forks,
    SUM(f.total_prs_opened)      AS total_prs,
    ROUND(
        100.0 * SUM(f.total_pushes) /
        SUM(SUM(f.total_pushes)) OVER (PARTITION BY d.year), 2
    ) AS ai_activity_share_pct
FROM `PROJECT_ID.github_oss_gold.fact_repo_activity_daily` f
JOIN `PROJECT_ID.github_oss_gold.dim_date`                 d ON f.date_key = d.date_key
JOIN `PROJECT_ID.github_oss_gold.dim_repository`           r ON f.repo_key  = r.repo_key
GROUP BY d.year, r.is_ai_ml_repo;

-- ----------------------------------------------------------------
-- Dashboard 3: COVID-19 Impact on Developer Activity
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_covid_impact` AS
SELECT
    d.year,
    d.hour,
    d.is_covid_period,
    d.is_weekend,
    SUM(f.total_pushes)        AS total_pushes,
    SUM(f.total_prs_opened)    AS total_prs,
    SUM(f.total_issues_opened) AS total_issues,
    COUNT(DISTINCT f.repo_key) AS unique_repos
FROM `PROJECT_ID.github_oss_gold.fact_repo_activity_daily` f
JOIN `PROJECT_ID.github_oss_gold.dim_date`                 d ON f.date_key = d.date_key
WHERE d.year IN (2019, 2020, 2021)
GROUP BY d.year, d.hour, d.is_covid_period, d.is_weekend;

-- ----------------------------------------------------------------
-- Dashboard 4: Repository Health Scorecard
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_repo_health` AS
WITH metrics AS (
    SELECT
        f.repo_key,
        AVG(f.avg_pr_merge_hours)    AS avg_pr_merge_hrs,
        AVG(f.avg_issue_close_hours) AS avg_issue_close_hrs,
        SUM(f.total_stars)           AS total_stars,
        SUM(f.total_forks)           AS total_forks,
        SAFE_DIVIDE(
            SUM(f.total_prs_merged),
            SUM(f.total_prs_opened)
        ) AS merge_rate
    FROM `PROJECT_ID.github_oss_gold.fact_repo_activity_daily` f
    GROUP BY f.repo_key
)
SELECT
    r.repo_name,
    r.language,
    r.is_ai_ml_repo,
    m.avg_pr_merge_hrs,
    m.avg_issue_close_hrs,
    m.total_stars,
    m.total_forks,
    m.merge_rate,
    ROUND(LEAST(100, GREATEST(0,
        25 * (1 - LEAST(1, COALESCE(m.avg_pr_merge_hrs,   168) / 168.0)) +
        25 * (1 - LEAST(1, COALESCE(m.avg_issue_close_hrs,720) / 720.0)) +
        25 * COALESCE(m.merge_rate, 0) +
        25 * LEAST(1, LOG(1 + m.total_stars + m.total_forks) / 15.0)
    )), 2) AS health_score
FROM metrics m
JOIN `PROJECT_ID.github_oss_gold.dim_repository` r ON m.repo_key = r.repo_key;

-- ----------------------------------------------------------------
-- Dashboard 5: Pull Request Lifecycle
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_pr_lifecycle` AS
SELECT
    d.year,
    l.language_name,
    f.pr_size_bucket,
    COUNT(f.event_id)                                           AS total_pr_events,
    SUM(CAST(f.is_merged AS INT64))                             AS merged_count,
    ROUND(100.0 * SUM(CAST(f.is_merged AS INT64))
        / NULLIF(COUNT(f.event_id), 0), 2)                     AS merge_rate_pct,
    ROUND(AVG(f.time_to_merge_hours), 1)                       AS avg_merge_hrs,
    ROUND(AVG(CAST(f.review_comments AS FLOAT64)), 2)          AS avg_review_comments,
    ROUND(AVG(CAST(f.additions + f.deletions AS FLOAT64)), 0)  AS avg_lines_changed
FROM `PROJECT_ID.github_oss_gold.fact_pr_events`  f
JOIN `PROJECT_ID.github_oss_gold.dim_date`         d ON f.date_key     = d.date_key
JOIN `PROJECT_ID.github_oss_gold.dim_language`     l ON f.language_key = l.language_key
WHERE f.action = 'closed'
GROUP BY d.year, l.language_name, f.pr_size_bucket;

-- ----------------------------------------------------------------
-- Dashboard 6: Bus Factor / Contributor Concentration
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_bus_factor` AS
WITH ranked AS (
    SELECT
        fp.repo_key,
        fp.contributor_key,
        COUNT(fp.event_id)  AS push_count,
        RANK() OVER (PARTITION BY fp.repo_key
                     ORDER BY COUNT(fp.event_id) DESC)         AS rnk,
        SUM(COUNT(fp.event_id)) OVER (PARTITION BY fp.repo_key) AS repo_total
    FROM `PROJECT_ID.github_oss_gold.fact_push_events` fp
    GROUP BY fp.repo_key, fp.contributor_key
)
SELECT
    r.repo_name,
    r.language,
    COUNT(DISTINCT rk.contributor_key)                          AS total_contributors,
    ROUND(100.0 * SUM(CASE WHEN rk.rnk = 1
        THEN rk.push_count ELSE 0 END)
        / NULLIF(MAX(rk.repo_total), 0), 2)                    AS top1_pct,
    ROUND(100.0 * SUM(CASE WHEN rk.rnk <= 3
        THEN rk.push_count ELSE 0 END)
        / NULLIF(MAX(rk.repo_total), 0), 2)                    AS top3_pct,
    ROUND(100.0 * SUM(CASE WHEN rk.rnk <= 5
        THEN rk.push_count ELSE 0 END)
        / NULLIF(MAX(rk.repo_total), 0), 2)                    AS top5_pct
FROM ranked rk
JOIN `PROJECT_ID.github_oss_gold.dim_repository` r ON rk.repo_key = r.repo_key
GROUP BY r.repo_name, r.language
HAVING COUNT(DISTINCT rk.contributor_key) >= 3;

-- ----------------------------------------------------------------
-- Dashboard 7: GitHub Ecosystem Growth YOY
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_ecosystem_growth` AS
SELECT
    d.year,
    COUNT(DISTINCT f.repo_key)              AS unique_repos,
    SUM(f.unique_contributors)              AS total_contributors,
    SUM(f.total_pushes)                     AS total_pushes,
    SUM(f.total_prs_opened)                 AS total_prs,
    SUM(f.total_issues_opened)              AS total_issues,
    SUM(f.total_stars)                      AS total_stars,
    SUM(f.total_forks)                      AS total_forks,
    ROUND(SAFE_DIVIDE(
        SUM(f.total_prs_opened),
        SUM(f.total_pushes)
    ), 4) AS pr_to_push_ratio
FROM `PROJECT_ID.github_oss_gold.fact_repo_activity_daily` f
JOIN `PROJECT_ID.github_oss_gold.dim_date`                 d ON f.date_key = d.date_key
GROUP BY d.year;

-- ----------------------------------------------------------------
-- Dashboard 8: Peak Activity Heatmap (hour x day-of-week)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_hourly_heatmap` AS
SELECT
    d.year,
    d.hour,
    d.day_of_week,
    d.day_name,
    SUM(f.total_pushes)        AS total_pushes,
    SUM(f.total_prs_opened)    AS total_prs,
    COUNT(DISTINCT f.repo_key) AS active_repos
FROM `PROJECT_ID.github_oss_gold.fact_repo_activity_daily` f
JOIN `PROJECT_ID.github_oss_gold.dim_date`                 d ON f.date_key = d.date_key
GROUP BY d.year, d.hour, d.day_of_week, d.day_name;

-- ----------------------------------------------------------------
-- Dashboard 9: ML Repo Success Predictions
-- (source: ml_abandonment_risk written by ml_score.py on every run)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_ml_predictions` AS
SELECT
    r.repo_name,
    r.language,
    r.is_ai_ml_repo,
    r.created_year,
    p.abandonment_probability,
    p.risk_tier,
    p.trajectory_label,
    p.model_version,
    p.scored_at,
    p.is_archived,
    CASE
        WHEN p.abandonment_probability >= 0.80 THEN 'Very High Risk (80%+)'
        WHEN p.abandonment_probability >= 0.60 THEN 'High Risk (60-80%)'
        WHEN p.abandonment_probability >= 0.40 THEN 'Moderate Risk (40-60%)'
        WHEN p.abandonment_probability >= 0.20 THEN 'Low Risk (20-40%)'
        ELSE 'Very Low Risk (<20%)'
    END AS confidence_tier
FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk`  p
JOIN `PROJECT_ID.github_oss_gold.dim_repository`       r ON p.repo_key = r.repo_key;

-- ----------------------------------------------------------------
-- Dashboard 10: ML Predictions by Language
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_ml_by_language` AS
SELECT
    r.language,
    COUNT(*)                                                          AS total_repos,
    SUM(CASE WHEN p.risk_tier = 'High Risk'     THEN 1 ELSE 0 END)  AS high_risk_count,
    SUM(CASE WHEN p.risk_tier = 'Moderate Risk' THEN 1 ELSE 0 END)  AS moderate_risk_count,
    SUM(CASE WHEN p.risk_tier = 'Low Risk'      THEN 1 ELSE 0 END)  AS low_risk_count,
    ROUND(AVG(p.abandonment_probability) * 100, 2)                   AS avg_abandonment_prob_pct,
    ROUND(100.0 * SUM(CASE WHEN p.risk_tier = 'High Risk' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1)                                    AS high_risk_pct
FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk`  p
JOIN `PROJECT_ID.github_oss_gold.dim_repository`       r ON p.repo_key = r.repo_key
WHERE r.language IS NOT NULL
GROUP BY r.language
HAVING COUNT(*) >= 10;

-- ----------------------------------------------------------------
-- Dashboard 11: ML Feature Importance
-- (source: ml_feature_importance written by ml_train.py monthly)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_ml_feature_importance` AS
SELECT
    feature_name,
    importance,
    model_version,
    ROUND(100.0 * importance / SUM(importance) OVER (PARTITION BY model_version), 2) AS importance_pct,
    RANK() OVER (PARTITION BY model_version ORDER BY importance DESC)                 AS importance_rank
FROM `PROJECT_ID.github_oss_gold.ml_feature_importance`;

-- ----------------------------------------------------------------
-- Dashboard 12: Abandonment Risk — Current Snapshot
-- (latest model version, one row per repo)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_abandonment_risk_current` AS
WITH latest_version AS (
    SELECT MAX(model_version) AS current_version
    FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk`
)
SELECT
    r.repo_name,
    r.owner_login,
    r.language,
    r.license,
    r.is_ai_ml_repo,
    r.has_readme,
    r.has_ci,
    r.created_year,
    r.repo_age_days,
    p.abandonment_probability,
    p.risk_tier,
    p.trajectory_label,
    p.is_archived,
    p.model_version,
    p.scored_at
FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk`  p
JOIN latest_version lv                                  ON p.model_version = lv.current_version
JOIN `PROJECT_ID.github_oss_gold.dim_repository`       r ON p.repo_key = r.repo_key;

-- ----------------------------------------------------------------
-- Dashboard 13: Abandonment Risk by Language (bar chart)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_abandonment_by_language` AS
WITH latest_version AS (
    SELECT MAX(model_version) AS current_version
    FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk`
),
current_scores AS (
    SELECT p.*
    FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk` p
    JOIN latest_version lv ON p.model_version = lv.current_version
)
SELECT
    r.language,
    COUNT(*)                                                                AS total_repos,
    SUM(CASE WHEN s.risk_tier = 'High Risk'     THEN 1 ELSE 0 END)        AS high_risk_count,
    SUM(CASE WHEN s.risk_tier = 'Moderate Risk' THEN 1 ELSE 0 END)        AS moderate_risk_count,
    SUM(CASE WHEN s.risk_tier = 'Low Risk'      THEN 1 ELSE 0 END)        AS low_risk_count,
    ROUND(AVG(s.abandonment_probability), 4)                                AS avg_abandonment_probability,
    ROUND(100.0 * SUM(CASE WHEN s.risk_tier = 'High Risk' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1)                                          AS high_risk_pct
FROM current_scores                                                        s
JOIN `PROJECT_ID.github_oss_gold.dim_repository`                           r ON s.repo_key = r.repo_key
WHERE r.language IS NOT NULL
GROUP BY r.language;

-- ----------------------------------------------------------------
-- Dashboard 14: Abandonment Risk Trend over Model Versions
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_abandonment_trend` AS
SELECT
    model_version,
    risk_tier,
    COUNT(*)                                                                    AS repo_count,
    ROUND(AVG(abandonment_probability), 4)                                      AS avg_probability,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY model_version), 1
    )                                                                           AS pct_of_scored_repos
FROM `PROJECT_ID.github_oss_gold.ml_abandonment_risk`
GROUP BY model_version, risk_tier;

-- ----------------------------------------------------------------
-- Dashboard 15: Repository Trajectory Clustering (K-Means)
-- (source: ml_repo_trajectories written by ml_trajectory.py monthly)
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW `PROJECT_ID.github_oss_gold.analysis_repo_trajectories` AS
SELECT
    r.repo_name,
    r.language,
    r.is_ai_ml_repo,
    r.created_year,
    t.trajectory_label,
    t.growth_ratio,
    t.avg_commits_early,
    t.avg_commits_recent,
    t.years_observed,
    t.computed_at
FROM `PROJECT_ID.github_oss_gold.ml_repo_trajectories` t
JOIN `PROJECT_ID.github_oss_gold.dim_repository`       r ON t.repo_key = r.repo_key;
