# GitHub Ecosystem Analytics — Looker Studio Dashboard Guide

**Project:** `damg-7370-492706`
**Dataset:** `github_oss_gold`
**Total Views:** 17 (15 existing + 2 new)

---

## Prerequisites

### 1. Deploy the 2 New Views to BigQuery

Run this in the BigQuery console (or via CLI). These are the **new views** added for Objectives 7 and 8:

```sql
-- View 16: Language Market Share Forecast (2026-2027)
CREATE OR REPLACE VIEW `damg-7370-492706.github_oss_gold.analysis_language_forecast` AS
SELECT
    f.language_name,
    f.slope_pct_per_year,
    f.r_squared,
    f.forecast_2026_share,
    f.forecast_2027_share,
    f.trend_label,
    f.computed_at,
    l.paradigm,
    l.type_system,
    l.first_appeared
FROM `damg-7370-492706.github_oss_gold.ml_language_trend_forecasts` f
LEFT JOIN `damg-7370-492706.github_oss_gold.dim_language`           l ON f.language_key = l.language_key
WHERE f.language_name IS NOT NULL;

-- View 17: Contributor Dynamics (Growth, Retention, New vs Returning)
CREATE OR REPLACE VIEW `damg-7370-492706.github_oss_gold.analysis_contributor_dynamics` AS
WITH yearly_contributors AS (
    SELECT
        d.year,
        fp.contributor_key,
        COUNT(fp.event_id) AS push_count
    FROM `damg-7370-492706.github_oss_gold.fact_push_events`  fp
    JOIN `damg-7370-492706.github_oss_gold.dim_date`          d  ON fp.date_key = d.date_key
    GROUP BY d.year, fp.contributor_key
),
first_year AS (
    SELECT
        contributor_key,
        MIN(year) AS first_active_year
    FROM yearly_contributors
    GROUP BY contributor_key
),
classified AS (
    SELECT
        yc.year,
        yc.contributor_key,
        yc.push_count,
        fy.first_active_year,
        CASE WHEN yc.year = fy.first_active_year THEN 'New' ELSE 'Returning' END AS contributor_type
    FROM yearly_contributors yc
    JOIN first_year fy ON yc.contributor_key = fy.contributor_key
),
retention AS (
    SELECT
        a.year AS current_year,
        COUNT(DISTINCT a.contributor_key) AS active_this_year,
        COUNT(DISTINCT b.contributor_key) AS retained_from_prior,
        ROUND(100.0 * COUNT(DISTINCT b.contributor_key)
            / NULLIF(COUNT(DISTINCT a.contributor_key), 0), 2) AS retention_rate_pct
    FROM yearly_contributors a
    LEFT JOIN yearly_contributors b
        ON a.contributor_key = b.contributor_key AND b.year = a.year - 1
    GROUP BY a.year
)
SELECT
    c.year,
    COUNT(DISTINCT c.contributor_key)                                           AS total_contributors,
    COUNT(DISTINCT CASE WHEN c.contributor_type = 'New'       THEN c.contributor_key END) AS new_contributors,
    COUNT(DISTINCT CASE WHEN c.contributor_type = 'Returning' THEN c.contributor_key END) AS returning_contributors,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN c.contributor_type = 'New' THEN c.contributor_key END)
        / NULLIF(COUNT(DISTINCT c.contributor_key), 0), 2)                     AS new_contributor_pct,
    r.retained_from_prior,
    r.retention_rate_pct,
    SUM(c.push_count)                                                           AS total_pushes
FROM classified c
JOIN retention r ON c.year = r.current_year
GROUP BY c.year, r.retained_from_prior, r.retention_rate_pct
ORDER BY c.year;
```

Or run the full file:
```bash
bq query --use_legacy_sql=false < scripts/create_bq_views_ready.sql
```

### 2. Connect Looker Studio to BigQuery

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Click **Create** > **Report**
3. Click **Add data** > **BigQuery**
4. Select: **Project** `damg-7370-492706` > **Dataset** `github_oss_gold`
5. You will add specific views per page (instructions below for each)
6. Click **Add** > **Connect**

---

## Dashboard Structure: 8 Pages, One Per Objective

Each page is a separate Looker Studio page. For each page, you add the relevant view as a data source.

---

## PAGE 1: Programming Language Adoption Over a Decade

### Data Source
**View:** `analysis_language_evolution`

**Add data source:** BigQuery > `damg-7370-492706` > `github_oss_gold` > `analysis_language_evolution`

### Available Fields
| Field | Type | Description |
|-------|------|-------------|
| `language_name` | Text | Programming language (Python, JavaScript, etc.) |
| `year` | Number | Calendar year (2016-2025) |
| `total_push_events` | Number | Total push events for that language/year |
| `total_commits` | Number | Total commits for that language/year |
| `unique_repos` | Number | Distinct repos using this language that year |
| `unique_contributors` | Number | Distinct developers pushing in this language |
| `market_share_pct` | Number | % of all push events attributable to this language |
| `yoy_push_growth_pct` | Number | Year-over-year growth in push events (%) |

### Visuals to Build

#### 1A. Line Chart — "Language Market Share Over Time (2016-2025)"
- **Chart type:** Time series (line chart)
- **Dimension:** `year`
- **Breakdown dimension:** `language_name`
- **Metric:** `market_share_pct` (SUM)
- **Sort:** `year` ascending
- **Description:** Shows how each language's share of total GitHub push activity has changed over 10 years. Reveals the rise of Python/TypeScript and the relative decline of legacy languages like Ruby and PHP.
- **Why relevant:** Tracks the long-term shift in developer preferences, helping identify which languages are gaining or losing traction in the open-source ecosystem.

#### 1B. Stacked Bar Chart — "Total Commits by Language Per Year"
- **Chart type:** Stacked bar chart
- **Dimension:** `year`
- **Breakdown dimension:** `language_name`
- **Metric:** `total_commits` (SUM)
- **Description:** Shows absolute commit volume per language stacked by year. Unlike market share (relative), this shows whether total activity is growing even if share is flat.
- **Why relevant:** A language can lose market share while still growing in absolute terms — this chart surfaces that nuance.

#### 1C. Scorecard Row — "Top Language Metrics (Latest Year)"
- Add a **filter** control: `year` = 2025
- **Scorecards** (one per top language): filter by `language_name`
  - Metric: `market_share_pct`
  - Comparison metric: `yoy_push_growth_pct`
- **Description:** Quick KPI view of each top language's current market share and growth rate.

#### 1D. Table — "Language Detail Table"
- **Chart type:** Table
- **Dimension:** `language_name`, `year`
- **Metrics:** `market_share_pct`, `yoy_push_growth_pct`, `unique_repos`, `unique_contributors`
- **Sort:** `year` desc, `market_share_pct` desc
- **Description:** Detailed drill-down table for exact numbers behind the charts.

---

## PAGE 2: The AI/ML Boom (2022-2025)

### Data Source
**View:** `analysis_ai_boom`

**Add data source:** BigQuery > `damg-7370-492706` > `github_oss_gold` > `analysis_ai_boom`

### Available Fields
| Field | Type | Description |
|-------|------|-------------|
| `year` | Number | Calendar year |
| `is_ai_ml_repo` | Boolean | `true` = AI/ML repo (flagged by topic/name matching) |
| `unique_repos` | Number | Distinct repositories |
| `total_pushes` | Number | Total push events |
| `total_stars` | Number | Total star events |
| `total_forks` | Number | Total fork events |
| `total_prs` | Number | Total pull request events |
| `ai_activity_share_pct` | Number | % of that year's total pushes from AI/ML repos |

### Visuals to Build

#### 2A. Dual-Axis Line Chart — "AI/ML vs Non-AI Repository Activity"
- **Chart type:** Combo chart (bars + line)
- **Dimension:** `year`
- **Bar metric:** `total_pushes` (SUM), breakdown by `is_ai_ml_repo`
- **Line metric:** `ai_activity_share_pct` (SUM, filter `is_ai_ml_repo` = true)
- **Description:** Bars show absolute push volume for AI/ML repos vs all others; the line shows AI/ML's growing share of total activity. The inflection point around 2022-2023 corresponds to the ChatGPT/LLM explosion.
- **Why relevant:** Quantifies the AI/ML boom — not just that it happened, but exactly how much of GitHub's activity shifted toward AI projects.

#### 2B. Bar Chart — "AI/ML Repos: Stars & Forks Growth"
- **Chart type:** Grouped bar chart
- **Filter:** `is_ai_ml_repo` = true
- **Dimension:** `year`
- **Metrics:** `total_stars` (SUM), `total_forks` (SUM)
- **Description:** Shows the explosive growth in community engagement (stars) and derivative work (forks) for AI/ML repositories.
- **Why relevant:** Stars and forks are proxy measures for community interest and practical adoption — a spike here means developers are not just building AI tools but actively using and extending them.

#### 2C. Scorecard — "AI/ML Share of GitHub (Latest Year)"
- **Filter:** `year` = 2025, `is_ai_ml_repo` = true
- **Metrics:** `ai_activity_share_pct`, `unique_repos`, `total_pushes`
- **Description:** At-a-glance KPIs showing how dominant AI/ML has become.

#### 2D. Table — "AI vs Non-AI Year-over-Year Comparison"
- **Chart type:** Table with heatmap conditional formatting
- **Dimensions:** `year`, `is_ai_ml_repo`
- **Metrics:** `unique_repos`, `total_pushes`, `total_stars`, `total_prs`, `ai_activity_share_pct`
- **Description:** Side-by-side numbers to compare AI/ML and non-AI/ML ecosystems across all metrics.

---

## PAGE 3: COVID-19 Impact on Developer Activity

### Data Source
**View:** `analysis_covid_impact`

**Add data source:** BigQuery > `damg-7370-492706` > `github_oss_gold` > `analysis_covid_impact`

### Available Fields
| Field | Type | Description |
|-------|------|-------------|
| `year` | Number | 2019, 2020, or 2021 |
| `hour` | Number | Hour of day (0-23, UTC) |
| `is_covid_period` | Boolean | `true` = 2020-03-01 to 2021-06-30 |
| `is_weekend` | Boolean | `true` = Saturday/Sunday |
| `total_pushes` | Number | Push events in this time bucket |
| `total_prs` | Number | PR events |
| `total_issues` | Number | Issue events |
| `unique_repos` | Number | Distinct active repos |

### Visuals to Build

#### 3A. Line Chart — "Hourly Push Activity: Pre-COVID vs During COVID"
- **Chart type:** Line chart
- **Dimension:** `hour`
- **Breakdown dimension:** `is_covid_period`
- **Metric:** `total_pushes` (SUM)
- **Description:** Compares the hourly distribution of pushes before COVID (2019) vs during COVID (Mar 2020 - Jun 2021). Expect to see activity spreading more evenly across the day during COVID as developers worked from home on flexible schedules.
- **Why relevant:** Reveals how remote work changed developer work patterns — shifted from a 9-5 spike to a flatter, more distributed curve.

#### 3B. Grouped Bar Chart — "Weekday vs Weekend Activity by Period"
- **Chart type:** Grouped bar chart
- **Dimension:** `is_covid_period` (label as "Pre-COVID" / "During COVID")
- **Breakdown dimension:** `is_weekend`
- **Metric:** `total_pushes` (SUM)
- **Description:** Compares weekday vs weekend coding volume before and during COVID. Weekend activity typically increases during lockdowns.
- **Why relevant:** Tests the hypothesis that developers coded more on weekends during COVID lockdowns.

#### 3C. Stacked Bar — "Activity Type Breakdown by Year"
- **Chart type:** Stacked bar chart
- **Dimension:** `year`
- **Metrics:** `total_pushes` (SUM), `total_prs` (SUM), `total_issues` (SUM)
- **Description:** Shows total volume of pushes, PRs, and issues for 2019, 2020, 2021. Expect to see a surge in 2020 as developers pivoted to open-source during lockdowns.
- **Why relevant:** Measures the aggregate impact of COVID on developer productivity and open-source contribution.

#### 3D. Heatmap Table — "Hour-by-Year Activity Heatmap"
- **Chart type:** Pivot table with heatmap
- **Row dimension:** `hour`
- **Column dimension:** `year`
- **Metric:** `total_pushes` (SUM, apply heatmap coloring)
- **Description:** Shows where push activity concentrates by hour for each year. During COVID, the "hotspot" should shift and spread.

---

## PAGE 4: Repository Health Assessment

### Data Source
**View:** `analysis_repo_health`

**Add data source:** BigQuery > `damg-7370-492706` > `github_oss_gold` > `analysis_repo_health`

### Available Fields
| Field | Type | Description |
|-------|------|-------------|
| `repo_name` | Text | Full repo name (owner/repo) |
| `language` | Text | Primary programming language |
| `is_ai_ml_repo` | Boolean | Whether the repo is flagged as AI/ML |
| `avg_pr_merge_hrs` | Number | Average hours to merge a PR |
| `avg_issue_close_hrs` | Number | Average hours to close an issue |
| `total_stars` | Number | Cumulative star count |
| `total_forks` | Number | Cumulative fork count |
| `merge_rate` | Number | Fraction of opened PRs that got merged (0-1) |
| `health_score` | Number | Composite score (0-100) |

### Health Score Formula
```
health_score = 25 * (1 - min(1, avg_pr_merge_hrs / 168))     -- PR speed (168h = 1 week)
             + 25 * (1 - min(1, avg_issue_close_hrs / 720))  -- Issue speed (720h = 30 days)
             + 25 * merge_rate                                 -- PR merge rate
             + 25 * min(1, log(1 + stars + forks) / 15)       -- Popularity
```

### Visuals to Build

#### 4A. Histogram / Distribution Chart — "Health Score Distribution"
- **Chart type:** Bar chart (bucket `health_score` into ranges: 0-20, 21-40, 41-60, 61-80, 81-100)
- **Create calculated field:** `health_score_bucket`
  - Formula: `CASE WHEN health_score >= 80 THEN "Excellent (80-100)" WHEN health_score >= 60 THEN "Good (60-79)" WHEN health_score >= 40 THEN "Fair (40-59)" WHEN health_score >= 20 THEN "Poor (20-39)" ELSE "Critical (0-19)" END`
- **Dimension:** `health_score_bucket`
- **Metric:** Record Count
- **Description:** Shows how many repositories fall into each health tier. Highlights the overall health of the ecosystem — a healthy ecosystem has most repos in the Good/Excellent range.
- **Why relevant:** Quickly identifies what proportion of the ecosystem is well-maintained vs at risk of deterioration.

#### 4B. Scatter Plot — "PR Merge Time vs Health Score"
- **Chart type:** Scatter chart
- **X-axis:** `avg_pr_merge_hrs`
- **Y-axis:** `health_score`
- **Color/size dimension:** `language`
- **Tooltip:** `repo_name`
- **Description:** Reveals the relationship between PR responsiveness and overall health. Healthy repos cluster in the bottom-right (low merge time, high score).
- **Why relevant:** Identifies which metric most strongly drives health — useful for repo maintainers wanting to improve.

#### 4C. Bar Chart — "Average Health Score by Language"
- **Chart type:** Horizontal bar chart
- **Dimension:** `language`
- **Metric:** `health_score` (AVG)
- **Sort:** `health_score` descending
- **Description:** Compares average repository health across languages. Languages with strong community tooling (CI, linters) tend to score higher.
- **Why relevant:** Helps identify which language ecosystems have the healthiest maintenance practices.

#### 4D. Table — "Top/Bottom Repos by Health Score"
- **Chart type:** Table with conditional formatting
- **Dimensions:** `repo_name`, `language`, `is_ai_ml_repo`
- **Metrics:** `health_score`, `avg_pr_merge_hrs`, `avg_issue_close_hrs`, `merge_rate`, `total_stars`
- **Sort:** `health_score` descending (or ascending to see worst)
- Add **filter control** for `language` and `is_ai_ml_repo`
- **Description:** Drill-down table to identify specific top-performing and at-risk repositories.

---

## PAGE 5: Repository Abandonment Risk Prediction

### Data Sources (add all 4)
1. **View:** `analysis_abandonment_risk_current` — per-repo current risk snapshot
2. **View:** `analysis_abandonment_by_language` — aggregated by language
3. **View:** `analysis_ml_feature_importance` — model feature importance
4. **View:** `analysis_abandonment_trend` — risk trend over model versions

### Fields — `analysis_abandonment_risk_current`
| Field | Type | Description |
|-------|------|-------------|
| `repo_name` | Text | Full repo name |
| `owner_login` | Text | Repository owner |
| `language` | Text | Primary language |
| `license` | Text | License type |
| `is_ai_ml_repo` | Boolean | AI/ML flag |
| `has_readme` | Boolean | Has README file |
| `has_ci` | Boolean | Has CI configuration |
| `created_year` | Number | Year the repo was created |
| `repo_age_days` | Number | Age in days |
| `abandonment_probability` | Number | ML-predicted probability (0.0-1.0) |
| `risk_tier` | Text | "High Risk", "Moderate Risk", "Low Risk" |
| `trajectory_label` | Text | Growth trajectory cluster label |
| `is_archived` | Boolean | Whether the repo is archived |
| `model_version` | Text | Model version (YYYYMM) |
| `scored_at` | Timestamp | When the score was computed |

### Fields — `analysis_abandonment_by_language`
| Field | Type | Description |
|-------|------|-------------|
| `language` | Text | Programming language |
| `total_repos` | Number | Repos scored in this language |
| `high_risk_count` | Number | Repos with High Risk tier |
| `moderate_risk_count` | Number | Repos with Moderate Risk |
| `low_risk_count` | Number | Repos with Low Risk |
| `avg_abandonment_probability` | Number | Average probability (0-1) |
| `high_risk_pct` | Number | % of repos at High Risk |

### Fields — `analysis_ml_feature_importance`
| Field | Type | Description |
|-------|------|-------------|
| `feature_name` | Text | Name of the ML feature |
| `importance` | Number | Raw importance value |
| `model_version` | Text | Model version |
| `importance_pct` | Number | Importance as % of total |
| `importance_rank` | Number | Rank (1 = most important) |

### Visuals to Build

#### 5A. Donut Chart — "Overall Risk Distribution"
- **Data source:** `analysis_abandonment_risk_current`
- **Chart type:** Donut chart
- **Dimension:** `risk_tier`
- **Metric:** Record Count
- **Color:** High Risk = red, Moderate Risk = yellow, Low Risk = green
- **Description:** Shows the proportion of repositories in each risk tier. A large red slice indicates systemic ecosystem health issues.
- **Why relevant:** Provides an instant read on what fraction of the ecosystem is at risk of being abandoned.

#### 5B. Stacked Bar Chart — "Abandonment Risk by Language"
- **Data source:** `analysis_abandonment_by_language`
- **Chart type:** Stacked horizontal bar chart
- **Dimension:** `language`
- **Metrics:** `high_risk_count`, `moderate_risk_count`, `low_risk_count`
- **Sort:** `high_risk_pct` descending
- **Description:** Shows which languages have the highest proportion of at-risk repos. Languages with small communities or declining popularity tend to have higher risk.
- **Why relevant:** Identifies language ecosystems where project sustainability is a concern.

#### 5C. Horizontal Bar Chart — "Top 15 Predictive Features"
- **Data source:** `analysis_ml_feature_importance`
- **Chart type:** Horizontal bar chart
- **Filter:** `importance_rank` <= 15
- **Dimension:** `feature_name`
- **Metric:** `importance_pct` (SUM)
- **Sort:** `importance_pct` descending
- **Description:** Shows which features the Random Forest model uses most to predict abandonment. Typically: recent commit activity, contributor count, and repo age dominate.
- **Why relevant:** Explains the model — essential for trust and actionability. If "has_ci" is important, it suggests CI adoption helps prevent abandonment.

#### 5D. Table — "At-Risk Repository Detail"
- **Data source:** `analysis_abandonment_risk_current`
- **Chart type:** Table with conditional formatting on `abandonment_probability`
- **Dimensions:** `repo_name`, `language`, `risk_tier`, `trajectory_label`
- **Metrics:** `abandonment_probability`, `repo_age_days`, `created_year`
- **Filters:** Add dropdown controls for `risk_tier`, `language`, `is_ai_ml_repo`
- **Sort:** `abandonment_probability` descending
- **Description:** Drill-down list of every scored repository with its risk assessment and trajectory. Users can filter to find specific repos or language ecosystems.

---

## PAGE 6: Repository Growth Trajectories (K-Means Clustering)

### Data Source
**View:** `analysis_repo_trajectories`

**Add data source:** BigQuery > `damg-7370-492706` > `github_oss_gold` > `analysis_repo_trajectories`

### Available Fields
| Field | Type | Description |
|-------|------|-------------|
| `repo_name` | Text | Full repo name |
| `language` | Text | Primary language |
| `is_ai_ml_repo` | Boolean | AI/ML flag |
| `created_year` | Number | Year created |
| `trajectory_label` | Text | Cluster label: Viral, Established, Stable, Declining, Dormant, Inactive |
| `growth_ratio` | Number | recent_activity / early_activity ratio |
| `avg_commits_early` | Number | Avg commits in early era (2016-2020) |
| `avg_commits_recent` | Number | Avg commits in recent era (2021-2025) |
| `years_observed` | Number | How many years the repo has data |
| `computed_at` | Timestamp | When clustering was computed |

### Visuals to Build

#### 6A. Donut Chart — "Trajectory Distribution"
- **Chart type:** Donut chart
- **Dimension:** `trajectory_label`
- **Metric:** Record Count
- **Colors:** Viral = dark green, Established = blue, Stable = gray, Declining = orange, Dormant = red, Inactive = dark red
- **Description:** Shows what proportion of repos fall into each growth trajectory. A healthy ecosystem has a mix of Viral + Established and relatively few Dormant/Inactive.
- **Why relevant:** Gives a snapshot of ecosystem vitality — are repos growing, stagnating, or dying?

#### 6B. Scatter Plot — "Early vs Recent Activity by Trajectory"
- **Chart type:** Scatter chart
- **X-axis:** `avg_commits_early`
- **Y-axis:** `avg_commits_recent`
- **Color:** `trajectory_label`
- **Size:** `growth_ratio`
- **Tooltip:** `repo_name`
- **Description:** Each dot is a repository. Repos above the diagonal line (recent > early) are growing; below are declining. Color-coded by cluster assignment.
- **Why relevant:** Visually validates the K-Means clustering — you should see clear color separation along the diagonal.

#### 6C. Stacked Bar — "Trajectory by Language"
- **Chart type:** Stacked bar chart (100%)
- **Dimension:** `language`
- **Breakdown dimension:** `trajectory_label`
- **Metric:** Record Count
- **Sort by:** Record Count descending
- **Description:** Shows each language's distribution across growth trajectories. A language dominated by "Declining" clusters may be losing relevance.
- **Why relevant:** Connects growth trajectory analysis back to the language-level story.

#### 6D. Table — "Repository Trajectory Detail"
- **Chart type:** Table
- **Dimensions:** `repo_name`, `language`, `trajectory_label`, `is_ai_ml_repo`
- **Metrics:** `growth_ratio`, `avg_commits_early`, `avg_commits_recent`, `years_observed`
- **Sort:** `growth_ratio` descending
- **Filters:** Add dropdown controls for `trajectory_label`, `language`
- **Description:** Full drill-down into individual repo trajectories.

---

## PAGE 7: Language Market Share Forecast (2026-2027)

### Data Sources (add both)
1. **View:** `analysis_language_forecast` (NEW — View 16)
2. **View:** `analysis_language_evolution` (for historical context)

### Fields — `analysis_language_forecast`
| Field | Type | Description |
|-------|------|-------------|
| `language_name` | Text | Programming language |
| `slope_pct_per_year` | Number | Annual market share change (pp/year from linear regression) |
| `r_squared` | Number | Model fit (0-1, higher = better fit) |
| `forecast_2026_share` | Number | Predicted market share % for 2026 |
| `forecast_2027_share` | Number | Predicted market share % for 2027 |
| `trend_label` | Text | "Strongly Growing", "Growing", "Stable", "Declining", "Strongly Declining" |
| `computed_at` | Timestamp | When the forecast was generated |
| `paradigm` | Text | Language paradigm (OOP, functional, etc.) |
| `type_system` | Text | Static or dynamic typing |
| `first_appeared` | Number | Year the language was created |

### Visuals to Build

#### 7A. Grouped Bar Chart — "Forecast: 2026 vs 2027 Market Share"
- **Data source:** `analysis_language_forecast`
- **Chart type:** Grouped bar chart
- **Dimension:** `language_name`
- **Metrics:** `forecast_2026_share` (SUM), `forecast_2027_share` (SUM)
- **Sort:** `forecast_2026_share` descending
- **Description:** Side-by-side bars showing predicted market share for each language in 2026 and 2027. Languages with growing bars are gaining traction; shrinking bars are losing ground.
- **Why relevant:** Provides forward-looking intelligence for technology investment decisions and hiring strategies.

#### 7B. Horizontal Bar Chart — "Growth Slope (pp/year)"
- **Data source:** `analysis_language_forecast`
- **Chart type:** Horizontal bar chart with positive/negative coloring
- **Dimension:** `language_name`
- **Metric:** `slope_pct_per_year` (SUM)
- **Sort:** `slope_pct_per_year` descending
- **Conditional formatting:** Green for positive, red for negative
- **Description:** Shows the annual rate of market share change per language. A slope of +0.5 means the language gains 0.5 percentage points of market share each year.
- **Why relevant:** Separates "growing fast" from "growing slowly" — a language with 2% share but +0.5 slope is more dynamic than one with 15% share and +0.1 slope.

#### 7C. Combo Chart — "Historical + Forecasted Market Share"
- **Data source:** Blend `analysis_language_evolution` (historical) with `analysis_language_forecast` (forecast)
- **How to blend:**
  1. Click **Resource** > **Manage added data sources** > **Add a data source** (add both views)
  2. Click **Resource** > **Manage blends** > **Add a blend**
  3. **Left source:** `analysis_language_evolution` — fields: `language_name`, `year`, `market_share_pct`
  4. **Right source:** `analysis_language_forecast` — fields: `language_name`, `forecast_2026_share`, `forecast_2027_share`
  5. **Join condition:** `language_name` = `language_name`
  6. **Join type:** Full outer join
- **Chart type:** Line chart
- **Dimension:** `year` (for historical), add 2026/2027 as calculated fields
- **Metric:** `market_share_pct` (solid line), forecast values (dashed line)
- **Description:** Extends the 2016-2025 historical trend lines into 2026-2027 forecasts. Dashed lines represent predictions.
- **Why relevant:** Provides the complete picture — where languages have been and where they're headed.

**Alternative (simpler) approach without blending:**
- Use just `analysis_language_evolution` in a line chart
- Enable **Trend Line** in chart settings (right-click chart > Style > Trend line > Linear)
- Looker Studio will auto-project the regression line forward
- This is simpler but less precise than the custom ML forecast

#### 7D. Table — "Forecast Detail with Model Confidence"
- **Data source:** `analysis_language_forecast`
- **Chart type:** Table
- **Dimensions:** `language_name`, `trend_label`, `paradigm`, `type_system`
- **Metrics:** `forecast_2026_share`, `forecast_2027_share`, `slope_pct_per_year`, `r_squared`
- **Conditional formatting:** `r_squared` >= 0.7 green, < 0.3 red
- **Sort:** `forecast_2026_share` descending
- **Description:** Detailed table showing every language's forecast with model confidence (R-squared). High R-squared means the linear trend is a reliable predictor; low R-squared means the language's trajectory is erratic.
- **Why relevant:** R-squared acts as a confidence indicator — readers should trust forecasts with high R-squared more than those with low R-squared.

---

## PAGE 8: Contributor Dynamics

### Data Sources (add both)
1. **View:** `analysis_contributor_dynamics` (NEW — View 17)
2. **View:** `analysis_bus_factor` (existing — View 6)

### Fields — `analysis_contributor_dynamics`
| Field | Type | Description |
|-------|------|-------------|
| `year` | Number | Calendar year |
| `total_contributors` | Number | Total unique contributors active that year |
| `new_contributors` | Number | Contributors whose first push was that year |
| `returning_contributors` | Number | Contributors who also pushed in a prior year |
| `new_contributor_pct` | Number | % of total that are new |
| `retained_from_prior` | Number | Count of contributors active both this year and previous year |
| `retention_rate_pct` | Number | % of this year's contributors who were also active last year |
| `total_pushes` | Number | Total pushes from all contributors |

### Fields — `analysis_bus_factor`
| Field | Type | Description |
|-------|------|-------------|
| `repo_name` | Text | Full repo name |
| `language` | Text | Primary language |
| `total_contributors` | Number | Total unique contributors to this repo |
| `top1_pct` | Number | % of pushes from the #1 contributor |
| `top3_pct` | Number | % of pushes from top 3 contributors |
| `top5_pct` | Number | % of pushes from top 5 contributors |

### Visuals to Build

#### 8A. Stacked Bar Chart — "New vs Returning Contributors Per Year"
- **Data source:** `analysis_contributor_dynamics`
- **Chart type:** Stacked bar chart
- **Dimension:** `year`
- **Metrics:** `new_contributors` (SUM), `returning_contributors` (SUM)
- **Colors:** New = light blue, Returning = dark blue
- **Description:** Shows the composition of the contributor base each year. A growing "Returning" segment indicates a maturing ecosystem with strong retention; a dominant "New" segment suggests high churn.
- **Why relevant:** Answers "Is the GitHub OSS community growing because of new blood or because existing developers keep contributing?"

#### 8B. Line Chart — "Contributor Retention Rate Over Time"
- **Data source:** `analysis_contributor_dynamics`
- **Chart type:** Line chart
- **Dimension:** `year`
- **Metric:** `retention_rate_pct` (SUM)
- **Reference line:** Add a constant reference line at 50% as a benchmark
- **Description:** Tracks what percentage of each year's active contributors were also active the previous year. Rising retention means the ecosystem is "stickier."
- **Why relevant:** Retention is a leading indicator of ecosystem health — low retention means high onboarding cost and knowledge loss.

#### 8C. Histogram — "Bus Factor Distribution"
- **Data source:** `analysis_bus_factor`
- **Create calculated field:** `bus_factor_tier`
  - Formula: `CASE WHEN top1_pct >= 80 THEN "Critical (1 person >80%)" WHEN top1_pct >= 50 THEN "At Risk (1 person >50%)" WHEN top3_pct >= 80 THEN "Concentrated (top 3 >80%)" ELSE "Healthy (distributed)" END`
- **Chart type:** Donut or bar chart
- **Dimension:** `bus_factor_tier`
- **Metric:** Record Count
- **Description:** Shows how many repos have dangerous contributor concentration. "Critical" means one person does >80% of all pushes — if they leave, the project likely dies.
- **Why relevant:** Bus factor is the single biggest risk to open-source sustainability. This chart quantifies how widespread the problem is.

#### 8D. Scatter Plot — "Contributor Count vs Top-1 Concentration"
- **Data source:** `analysis_bus_factor`
- **Chart type:** Scatter chart
- **X-axis:** `total_contributors`
- **Y-axis:** `top1_pct`
- **Color:** `language`
- **Tooltip:** `repo_name`
- **Description:** Plots each repo by contributor count vs. top contributor's share. Healthy repos cluster in the bottom-right (many contributors, no single dominant one). At-risk repos are in the top-left.
- **Why relevant:** Identifies repos that have many contributors but are still dominated by one person, as well as repos that have naturally distributed ownership.

#### 8E. Table — "Bus Factor Detail"
- **Data source:** `analysis_bus_factor`
- **Chart type:** Table
- **Dimensions:** `repo_name`, `language`
- **Metrics:** `total_contributors`, `top1_pct`, `top3_pct`, `top5_pct`
- **Sort:** `top1_pct` descending
- **Conditional formatting:** `top1_pct` >= 80 red, >= 50 yellow, < 50 green
- **Description:** Repository-level breakdown of contributor concentration.

---

## Adding Global Controls (All Pages)

Add these interactive controls to the top of each page:

### Filter Bar (Top of Report)
1. **Drop-down list control:** `language` or `language_name` — lets users filter all charts to a specific language
2. **Drop-down list control:** `year` — filter to a specific year
3. **Checkbox control:** `is_ai_ml_repo` — toggle AI/ML repos only
4. **Date range control:** (on pages that have date fields)

### How to Add a Filter Control:
1. Click **Add a control** in the toolbar
2. Choose **Drop-down list**
3. Set **Data source** to the page's primary view
4. Set **Control field** to the field (e.g., `language_name`)
5. Check **Allow multiple selections** if desired

---

## Quick Reference: View → Objective Mapping

| Objective | Page | Primary View(s) | New? |
|-----------|------|-----------------|------|
| 1. Language adoption | Page 1 | `analysis_language_evolution` | No |
| 2. AI/ML boom | Page 2 | `analysis_ai_boom` | No |
| 3. COVID impact | Page 3 | `analysis_covid_impact` | No |
| 4. Repo health | Page 4 | `analysis_repo_health` | No |
| 5. Abandonment risk | Page 5 | `analysis_abandonment_risk_current`, `analysis_abandonment_by_language`, `analysis_ml_feature_importance`, `analysis_abandonment_trend` | No |
| 6. Growth trajectories | Page 6 | `analysis_repo_trajectories` | No |
| 7. Language forecast | Page 7 | `analysis_language_forecast`, `analysis_language_evolution` | **YES (View 16)** |
| 8. Contributor dynamics | Page 8 | `analysis_contributor_dynamics`, `analysis_bus_factor` | **YES (View 17)** |

---

## Styling Recommendations

- **Theme:** Use Looker Studio's "Dark" or "Simple Dark" theme for data-heavy dashboards
- **Color palette:** Consistent language colors across all pages (e.g., Python = blue, JavaScript = yellow, TypeScript = cornflower blue, Rust = orange, Go = cyan)
- **Font:** Use "Roboto" or "Google Sans" for clean readability
- **Page size:** Set to 1200 x 900 px for widescreen presentation
- **Navigation:** Add a left-side or top navigation bar with page links
