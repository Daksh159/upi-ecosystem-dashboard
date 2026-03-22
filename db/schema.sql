-- ============================================================
-- UPI Ecosystem Dashboard — Database Schema
-- PostgreSQL 15+
-- ============================================================

-- Drop tables in correct order (facts first, then dims)
-- Safe to re-run — won't error if tables don't exist yet
DROP TABLE IF EXISTS fact_news_sentiment   CASCADE;
DROP TABLE IF EXISTS fact_payment_systems  CASCADE;
DROP TABLE IF EXISTS fact_upi_monthly      CASCADE;
DROP TABLE IF EXISTS dim_payment_system    CASCADE;
DROP TABLE IF EXISTS dim_date              CASCADE;


-- ============================================================
-- DIMENSION: dim_date
-- One row per month in our dataset
-- Power BI will use this as the master date table
-- ============================================================
CREATE TABLE dim_date (
    date_id        SERIAL PRIMARY KEY,
    report_month   DATE        NOT NULL UNIQUE,
    month_num      SMALLINT    NOT NULL,  -- 1-12
    month_name     VARCHAR(10) NOT NULL,  -- 'April'
    quarter        SMALLINT    NOT NULL,  -- 1-4
    calendar_year  SMALLINT    NOT NULL,  -- 2024
    financial_year VARCHAR(10) NOT NULL,  -- 'FY2024-25'
    fy_month_num   SMALLINT    NOT NULL   -- position within FY (1=April, 12=March)
);

COMMENT ON TABLE dim_date IS
    'Master date dimension. All fact tables join to this via date_id.';


-- ============================================================
-- DIMENSION: dim_payment_system
-- One row per payment system (UPI, NEFT, IMPS, RTGS, etc.)
-- ============================================================
CREATE TABLE dim_payment_system (
    system_id       SERIAL PRIMARY KEY,
    system_name     VARCHAR(30)  NOT NULL UNIQUE,  -- 'UPI'
    system_category VARCHAR(30)  NOT NULL,          -- 'Retail' / 'Large Value'
    full_name       VARCHAR(100) NOT NULL,
    description     TEXT
);

COMMENT ON TABLE dim_payment_system IS
    'Reference table for payment systems tracked in the ecosystem.';


-- ============================================================
-- FACT: fact_upi_monthly
-- Core UPI metrics — one row per month
-- Source: NPCI Monthly Product Statistics
-- ============================================================
CREATE TABLE fact_upi_monthly (
    id               SERIAL PRIMARY KEY,
    date_id          INT          NOT NULL REFERENCES dim_date(date_id),
    banks_live       INT,                   -- banks live on UPI that month
    volume_mn        NUMERIC(12,2),         -- transactions in millions
    value_cr         NUMERIC(15,2),         -- transaction value in crores
    volume_mom_pct   NUMERIC(8,2),          -- month-over-month volume growth %
    value_mom_pct    NUMERIC(8,2),          -- month-over-month value growth %
    avg_txn_value_rs NUMERIC(10,2),         -- average transaction value in Rs
    volume_yoy_pct   NUMERIC(8,2),          -- year-over-year volume growth %
    created_at       TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE fact_upi_monthly IS
    'Monthly UPI transaction metrics from NPCI. One row per month.';

-- Index for fast date lookups (Power BI queries heavily by date)
CREATE INDEX idx_upi_monthly_date ON fact_upi_monthly(date_id);


-- ============================================================
-- FACT: fact_payment_systems
-- Payment ecosystem comparison — one row per system per month
-- Source: RBI Payment System Indicators
-- ============================================================
CREATE TABLE fact_payment_systems (
    id                   SERIAL PRIMARY KEY,
    date_id              INT         NOT NULL REFERENCES dim_date(date_id),
    system_id            INT         NOT NULL REFERENCES dim_payment_system(system_id),
    volume_mn            NUMERIC(12,2),    -- transaction volume in millions
    value_cr             NUMERIC(15,2),    -- transaction value in crores
    share_of_volume_pct  NUMERIC(6,2),     -- % of total volume that month
    share_of_value_pct   NUMERIC(6,2),     -- % of total value that month
    created_at           TIMESTAMP DEFAULT NOW(),

    -- Prevent duplicate month + system combinations
    UNIQUE (date_id, system_id)
);

COMMENT ON TABLE fact_payment_systems IS
    'Monthly metrics per payment system from RBI PSI. One row per system per month.';

CREATE INDEX idx_payment_systems_date   ON fact_payment_systems(date_id);
CREATE INDEX idx_payment_systems_system ON fact_payment_systems(system_id);


-- ============================================================
-- FACT: fact_news_sentiment
-- Fraud news headlines with auto-tagged categories
-- Source: Inc42 scraper (news_scraper.py)
-- ============================================================
CREATE TABLE fact_news_sentiment (
    id          SERIAL PRIMARY KEY,
    date_id     INT          REFERENCES dim_date(date_id),  -- nullable: not all articles map cleanly to a month
    headline    TEXT         NOT NULL,
    fraud_type  VARCHAR(50)  NOT NULL DEFAULT 'not_fraud',
    source      VARCHAR(50),
    article_url TEXT,
    scraped_at  TIMESTAMP,
    created_at  TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE fact_news_sentiment IS
    'Scraped fintech/UPI news headlines tagged by fraud category.';

CREATE INDEX idx_news_date       ON fact_news_sentiment(date_id);
CREATE INDEX idx_news_fraud_type ON fact_news_sentiment(fraud_type);