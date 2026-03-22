"""
db_loader.py
------------
Loads cleaned CSVs into PostgreSQL.
Populates tables in correct order:
  1. dim_date
  2. dim_payment_system
  3. fact_upi_monthly
  4. fact_payment_systems
  5. fact_news_sentiment (if news CSV exists)

Run:
  python scripts/db_loader.py
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/db_loading.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Paths and config ───────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
# REPLACE WITH THIS:
load_dotenv(BASE_DIR / ".env", override=True)


# ── Database connection ────────────────────────────────────────────────────────
def get_engine():
    load_dotenv(BASE_DIR / ".env", override=True)

    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host     = os.getenv("DB_HOST")
    port     = os.getenv("DB_PORT")
    dbname   = os.getenv("DB_NAME")

    from sqlalchemy import create_engine
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host":     host,
            "port":     int(port),
            "dbname":   dbname,
            "user":     user,
            "password": password,
        },
        echo=False
    )

    logger.info(f"Engine created: {user}@{host}:{port}/{dbname}")
    return engine


# ── LOADER 1: dim_date ─────────────────────────────────────────────────────────
def load_dim_date(engine, upi_df: pd.DataFrame, psi_df: pd.DataFrame) -> dict:
    """
    Builds dim_date from all unique months across both datasets.
    Returns a dict: { '2024-04-01': date_id } for FK lookups later.
    """
    # Combine all unique months from both sources
    all_months = pd.concat([
        upi_df[["report_month", "financial_year"]],
        psi_df[["report_month"]].assign(financial_year=None)
    ]).drop_duplicates(subset=["report_month"])

    all_months["report_month"] = pd.to_datetime(all_months["report_month"])
    all_months = all_months.sort_values("report_month").reset_index(drop=True)

    def get_fy(dt):
        if dt.month >= 4:
            return f"FY{dt.year}-{str(dt.year + 1)[2:]}"
        return f"FY{dt.year - 1}-{str(dt.year)[2:]}"

    def get_fy_month_num(dt):
        # April = 1, May = 2, ..., March = 12
        return ((dt.month - 4) % 12) + 1

    def get_quarter(dt):
        # Indian FY quarters: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
        fy_month = get_fy_month_num(dt)
        return (fy_month - 1) // 3 + 1

    dim_date_rows = []
    for _, row in all_months.iterrows():
        dt = row["report_month"]
        dim_date_rows.append({
            "report_month":  dt.date(),
            "month_num":     dt.month,
            "month_name":    dt.strftime("%B"),
            "quarter":       get_quarter(dt),
            "calendar_year": dt.year,
            "financial_year": get_fy(dt),
            "fy_month_num":  get_fy_month_num(dt),
        })

    dim_date_df = pd.DataFrame(dim_date_rows)

    # Load with upsert — safe to re-run without duplicating
    with engine.begin() as conn:
        # Clear existing and reload cleanly
        conn.execute(text("DELETE FROM dim_date"))
        dim_date_df.to_sql(
            "dim_date", conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    logger.info(f"dim_date loaded: {len(dim_date_df)} rows")

    # Build lookup dict for FK mapping
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT date_id, report_month FROM dim_date")
        )
        return {
            str(row.report_month): row.date_id
            for row in result
        }


# ── LOADER 2: dim_payment_system ───────────────────────────────────────────────
def load_dim_payment_system(engine) -> dict:
    """
    Inserts the 6 payment systems with descriptive metadata.
    Returns a dict: { 'UPI': system_id }
    """
    systems = [
        {
            "system_name":     "UPI",
            "system_category": "Retail",
            "full_name":       "Unified Payments Interface",
            "description":     "Real-time mobile payment system by NPCI. "
                               "P2P and P2M transactions."
        },
        {
            "system_name":     "IMPS",
            "system_category": "Retail",
            "full_name":       "Immediate Payment Service",
            "description":     "24x7 interbank electronic fund transfer "
                               "via mobile, internet, ATM."
        },
        {
            "system_name":     "NEFT",
            "system_category": "Retail",
            "full_name":       "National Electronic Funds Transfer",
            "description":     "Batch-based fund transfer system. "
                               "Operates in half-hourly batches."
        },
        {
            "system_name":     "RTGS",
            "system_category": "Large Value",
            "full_name":       "Real Time Gross Settlement",
            "description":     "Real-time settlement for large value "
                               "transactions. Minimum Rs 2 lakh."
        },
        {
            "system_name":     "Credit_Card",
            "system_category": "Card",
            "full_name":       "Credit Card (PoS)",
            "description":     "Point-of-sale credit card transactions "
                               "at physical and online merchants."
        },
        {
            "system_name":     "Debit_Card",
            "system_category": "Card",
            "full_name":       "Debit Card (PoS)",
            "description":     "Point-of-sale debit card transactions "
                               "at physical and online merchants."
        },
    ]

    dim_ps_df = pd.DataFrame(systems)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dim_payment_system"))
        dim_ps_df.to_sql(
            "dim_payment_system", conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    logger.info(f"dim_payment_system loaded: {len(dim_ps_df)} rows")

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT system_id, system_name FROM dim_payment_system")
        )
        return {row.system_name: row.system_id for row in result}


# ── LOADER 3: fact_upi_monthly ─────────────────────────────────────────────────
def load_fact_upi_monthly(
    engine, upi_df: pd.DataFrame, date_lookup: dict
) -> None:
    """
    Loads 24 months of UPI metrics into fact_upi_monthly.
    Maps report_month -> date_id using the lookup dict.
    """
    upi_df = upi_df.copy()
    upi_df["report_month"] = pd.to_datetime(
        upi_df["report_month"]
    ).dt.strftime("%Y-%m-%d")

    upi_df["date_id"] = upi_df["report_month"].map(date_lookup)

    # Select only columns that match the fact table
    fact_cols = [
        "date_id", "banks_live", "volume_mn", "value_cr",
        "volume_mom_pct", "value_mom_pct", "avg_txn_value_rs",
        "volume_yoy_pct"
    ]

    fact_df = upi_df[fact_cols].copy()

    # Replace NaN with None — PostgreSQL expects NULL, not NaN
    fact_df = fact_df.where(pd.notna(fact_df), other=None)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_upi_monthly"))
        fact_df.to_sql(
            "fact_upi_monthly", conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    logger.info(f"fact_upi_monthly loaded: {len(fact_df)} rows")


# ── LOADER 4: fact_payment_systems ─────────────────────────────────────────────
def load_fact_payment_systems(
    engine, psi_df: pd.DataFrame,
    date_lookup: dict, system_lookup: dict
) -> None:
    """
    Loads payment ecosystem comparison data.
    Maps report_month -> date_id and payment_system -> system_id.
    """
    psi_df = psi_df.copy()
    psi_df["report_month"] = pd.to_datetime(
        psi_df["report_month"]
    ).dt.strftime("%Y-%m-%d")

    psi_df["date_id"]   = psi_df["report_month"].map(date_lookup)
    psi_df["system_id"] = psi_df["payment_system"].map(system_lookup)

    fact_cols = [
        "date_id", "system_id", "volume_mn", "value_cr",
        "share_of_volume_pct", "share_of_value_pct"
    ]

    fact_df = psi_df[fact_cols].copy()
    fact_df = fact_df.where(pd.notna(fact_df), other=None)

    # Drop rows where FK mapping failed
    missing_dates = fact_df["date_id"].isna().sum()
    if missing_dates > 0:
        logger.warning(
            f"{missing_dates} rows dropped — date_id mapping failed"
        )
    fact_df = fact_df.dropna(subset=["date_id", "system_id"])

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_payment_systems"))
        fact_df.to_sql(
            "fact_payment_systems", conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    logger.info(f"fact_payment_systems loaded: {len(fact_df)} rows")


# ── LOADER 5: fact_news_sentiment ──────────────────────────────────────────────
def load_fact_news_sentiment(engine, date_lookup: dict) -> None:
    """
    Loads scraped news headlines if the file exists.
    Skips silently if news hasn't been scraped yet.
    """
    news_path = BASE_DIR / "data" / "raw" / "news" / "news_raw.csv"

    if not news_path.exists():
        logger.info(
            "news_raw.csv not found — skipping news load. "
            "Run news_scraper.py first."
        )
        return

    news_df = pd.read_csv(news_path, encoding="utf-8-sig")

    # Try to map article date to a month in dim_date
    # News dates may not map perfectly — that's OK, date_id is nullable
    if "date" in news_df.columns:
        try:
            news_df["parsed_date"] = pd.to_datetime(
                news_df["date"], errors="coerce"
            ).dt.strftime("%Y-%m-01")
            news_df["date_id"] = news_df["parsed_date"].map(date_lookup)
        except Exception:
            news_df["date_id"] = None
    else:
        news_df["date_id"] = None

    fact_cols = [
        "date_id", "headline", "fraud_type",
        "source", "url", "scraped_at"
    ]

    # Only keep columns that exist
    available = [c for c in fact_cols if c in news_df.columns]
    fact_df = news_df[available].copy()

    # Rename url -> article_url to match schema
    if "url" in fact_df.columns:
        fact_df = fact_df.rename(columns={"url": "article_url"})

    fact_df = fact_df.where(pd.notna(fact_df), other=None)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_news_sentiment"))
        fact_df.to_sql(
            "fact_news_sentiment", conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    logger.info(f"fact_news_sentiment loaded: {len(fact_df)} rows")


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("Starting DB loading pipeline")
    logger.info("=" * 60)

    # Load cleaned CSVs
    upi_df = pd.read_csv(
        PROCESSED_DIR / "clean_upi_monthly.csv", encoding="utf-8-sig"
    )
    psi_df = pd.read_csv(
        PROCESSED_DIR / "clean_payment_systems.csv", encoding="utf-8-sig"
    )
    logger.info(
        f"Loaded CSVs: upi={len(upi_df)} rows, psi={len(psi_df)} rows"
    )

    engine = get_engine()

    # Load in correct dependency order
    date_lookup   = load_dim_date(engine, upi_df, psi_df)
    system_lookup = load_dim_payment_system(engine)

    load_fact_upi_monthly(engine, upi_df, date_lookup)
    load_fact_payment_systems(engine, psi_df, date_lookup, system_lookup)
    load_fact_news_sentiment(engine, date_lookup)

    # Quick row count verification
    logger.info("\nFinal row counts:")
    with engine.connect() as conn:
        for table in [
            "dim_date", "dim_payment_system",
            "fact_upi_monthly", "fact_payment_systems",
            "fact_news_sentiment"
        ]:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table}")
            ).scalar()
            logger.info(f"  {table:<30s}: {count} rows")

    logger.info("\nDB loading complete.")


if __name__ == "__main__":
    main()

