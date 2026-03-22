-- ============================================================
-- UPI Ecosystem Dashboard — Analysis Queries
-- db/queries.sql
-- ============================================================


-- ── QUERY 1: UPI Growth Timeline ─────────────────────────────
-- Shows month-by-month volume and value with growth rates
-- Powers: Line chart in Power BI

SELECT
    d.report_month,
    d.month_name,
    d.calendar_year,
    d.financial_year,
    f.volume_mn,
    f.value_cr,
    f.banks_live,
    f.volume_mom_pct,
    f.value_mom_pct,
    f.avg_txn_value_rs,
    f.volume_yoy_pct,
    -- Running total volume within each financial year
    SUM(f.volume_mn) OVER (
        PARTITION BY d.financial_year
        ORDER BY d.report_month
    ) AS fy_cumulative_volume_mn,
    -- Rank each month by volume within its FY
    RANK() OVER (
        PARTITION BY d.financial_year
        ORDER BY f.volume_mn DESC
    ) AS volume_rank_in_fy
FROM fact_upi_monthly f
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY d.report_month;


-- ── QUERY 2: Payment Ecosystem Market Share ───────────────────
-- UPI vs NEFT vs IMPS vs RTGS by volume and value
-- Powers: Stacked bar + pie chart in Power BI

SELECT
    d.report_month,
    d.month_name,
    d.calendar_year,
    ps.system_name,
    ps.system_category,
    f.volume_mn,
    f.value_cr,
    f.share_of_volume_pct,
    f.share_of_value_pct,
    -- Rank systems by volume each month
    RANK() OVER (
        PARTITION BY d.report_month
        ORDER BY f.volume_mn DESC
    ) AS volume_rank
FROM fact_payment_systems f
JOIN dim_date d        ON f.date_id   = d.date_id
JOIN dim_payment_system ps ON f.system_id = ps.system_id
ORDER BY d.report_month, f.volume_mn DESC;


-- ── QUERY 3: Month-over-Month Anomaly Detection Prep ─────────
-- Flags months where growth deviated significantly
-- Powers: Anomaly highlight in Power BI + Python ML input

WITH stats AS (
    SELECT
        AVG(volume_mom_pct)    AS avg_growth,
        STDDEV(volume_mom_pct) AS stddev_growth
    FROM fact_upi_monthly
    WHERE volume_mom_pct IS NOT NULL
)
SELECT
    d.report_month,
    d.month_name,
    d.calendar_year,
    f.volume_mn,
    f.volume_mom_pct,
    s.avg_growth,
    s.stddev_growth,
    -- Z-score: how many standard deviations from mean
    ROUND(
        (f.volume_mom_pct - s.avg_growth) / NULLIF(s.stddev_growth, 0),
        2
    ) AS z_score,
    -- Flag if beyond 1.5 standard deviations
    CASE
        WHEN ABS((f.volume_mom_pct - s.avg_growth) /
             NULLIF(s.stddev_growth, 0)) > 1.5
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS anomaly_flag
FROM fact_upi_monthly f
JOIN dim_date d ON f.date_id = d.date_id
CROSS JOIN stats s
WHERE f.volume_mom_pct IS NOT NULL
ORDER BY d.report_month;


-- ── QUERY 4: Financial Year Summary ──────────────────────────
-- Aggregated KPIs per financial year
-- Powers: KPI cards + FY comparison bar chart

SELECT
    d.financial_year,
    COUNT(*)                        AS months_count,
    SUM(f.volume_mn)                AS total_volume_mn,
    SUM(f.value_cr)                 AS total_value_cr,
    AVG(f.volume_mn)                AS avg_monthly_volume,
    MAX(f.volume_mn)                AS peak_volume_mn,
    MIN(f.volume_mn)                AS trough_volume_mn,
    MAX(f.volume_mn) - MIN(f.volume_mn) AS volume_range_mn,
    AVG(f.avg_txn_value_rs)         AS avg_txn_value_rs,
    MAX(f.banks_live)               AS max_banks_live,
    -- FY-over-FY volume growth %
    ROUND(
        (SUM(f.volume_mn) - LAG(SUM(f.volume_mn)) OVER (ORDER BY d.financial_year))
        / NULLIF(LAG(SUM(f.volume_mn)) OVER (ORDER BY d.financial_year), 0) * 100,
        2
    ) AS fyoy_volume_growth_pct
FROM fact_upi_monthly f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.financial_year
ORDER BY d.financial_year;


-- ── QUERY 5: UPI Dominance Trend ─────────────────────────────
-- How UPI's share of total payment volume has grown over time
-- Powers: Area chart showing UPI dominance rising

SELECT
    d.report_month,
    d.month_name,
    d.calendar_year,
    -- UPI metrics
    MAX(CASE WHEN ps.system_name = 'UPI'
        THEN f.volume_mn END)              AS upi_volume_mn,
    MAX(CASE WHEN ps.system_name = 'UPI'
        THEN f.share_of_volume_pct END)    AS upi_volume_share_pct,
    MAX(CASE WHEN ps.system_name = 'UPI'
        THEN f.share_of_value_pct END)     AS upi_value_share_pct,
    -- Competing systems for context
    MAX(CASE WHEN ps.system_name = 'NEFT'
        THEN f.volume_mn END)              AS neft_volume_mn,
    MAX(CASE WHEN ps.system_name = 'IMPS'
        THEN f.volume_mn END)              AS imps_volume_mn,
    MAX(CASE WHEN ps.system_name = 'RTGS'
        THEN f.value_cr END)               AS rtgs_value_cr,
    -- Total retail volume (UPI + IMPS + NEFT + Cards)
    SUM(CASE WHEN ps.system_category = 'Retail'
        THEN f.volume_mn ELSE 0 END)       AS total_retail_volume_mn
FROM fact_payment_systems f
JOIN dim_date d           ON f.date_id   = d.date_id
JOIN dim_payment_system ps ON f.system_id = ps.system_id
GROUP BY d.report_month, d.month_name, d.calendar_year
ORDER BY d.report_month;


-- ── QUERY 6: Avg Transaction Value Decline ────────────────────
-- UPI avg txn value has been FALLING — more small payments
-- This is a key insight: UPI democratising micro-payments
-- Powers: Line chart with annotation

SELECT
    d.report_month,
    d.financial_year,
    d.month_name,
    f.avg_txn_value_rs,
    f.volume_mn,
    -- Rolling 3-month average to smooth noise
    ROUND(AVG(f.avg_txn_value_rs) OVER (
        ORDER BY d.report_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_3m_avg_txn_value,
    -- % change in avg txn value from first month
    ROUND(
        (f.avg_txn_value_rs - FIRST_VALUE(f.avg_txn_value_rs) OVER (
            ORDER BY d.report_month
        )) / NULLIF(FIRST_VALUE(f.avg_txn_value_rs) OVER (
            ORDER BY d.report_month
        ), 0) * 100,
        2
    ) AS pct_change_from_start
FROM fact_upi_monthly f
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY d.report_month;