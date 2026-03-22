"""
anomaly_detector.py
-------------------
Detects anomalous months in UPI transaction data using
Isolation Forest — the same algorithm used in production
fraud detection systems at fintech companies.

What it flags:
- Months where volume growth deviated significantly from pattern
- Months where avg transaction value behaved unexpectedly
- Months where volume and value moved in opposite directions

Output:
- Prints anomaly report to terminal
- Saves results to data/processed/anomaly_results.csv
- Saves results to PostgreSQL (anomaly_results table)
- Generates and saves a visualization

Run:
    python scripts/anomaly_detector.py
"""

from multiprocessing import context

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for Windows
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
from dotenv import load_dotenv
import os
import logging

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/anomaly_detection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
load_dotenv(BASE_DIR / ".env", override=True)


# ── DB Connection ──────────────────────────────────────────────
def get_engine():
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host":     os.getenv("DB_HOST"),
            "port":     int(os.getenv("DB_PORT")),
            "dbname":   os.getenv("DB_NAME"),
            "user":     os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        },
        echo=False
    )
    return engine


# ── STEP 1: Pull data from PostgreSQL ─────────────────────────
def load_upi_data(engine) -> pd.DataFrame:
    """
    Pulls UPI monthly data joined with date dimension.
    This is why we built the star schema — clean joins.
    """
    query = """
        SELECT
            d.report_month,
            d.month_name,
            d.calendar_year,
            d.financial_year,
            d.fy_month_num,
            f.volume_mn,
            f.value_cr,
            f.volume_mom_pct,
            f.value_mom_pct,
            f.avg_txn_value_rs,
            f.banks_live
        FROM fact_upi_monthly f
        JOIN dim_date d ON f.date_id = d.date_id
        ORDER BY d.report_month
    """
    df = pd.read_sql(query, engine)
    df["report_month"] = pd.to_datetime(df["report_month"])
    logger.info(f"Loaded {len(df)} months of UPI data")
    return df


# ── STEP 2: Feature Engineering ────────────────────────────────
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates features for anomaly detection.

    We can't just use raw volume — a high volume month in a
    growing trend is normal, not anomalous. We need features
    that capture DEVIATION from expected pattern.

    Features:
    1. volume_mom_pct     — how much volume changed MoM
    2. value_mom_pct      — how much value changed MoM
    3. vol_val_divergence — volume and value moving differently
    4. avg_txn_delta      — unexpected changes in avg txn size
    5. fy_position        — where in financial year (Apr=1, Mar=12)
    """

    df = df.copy()

    # Drop first row — MoM metrics are NaN for April 2023
    df = df.dropna(subset=["volume_mom_pct", "value_mom_pct"])

    # Feature 1 & 2: already have these
    # Feature 3: divergence between volume growth and value growth
    # Normal: both move together. Anomaly: one grows, other shrinks
    df["vol_val_divergence"] = (
        df["volume_mom_pct"] - df["value_mom_pct"]
    )

    # Feature 4: month-over-month change in avg transaction value
    df["avg_txn_delta"] = df["avg_txn_value_rs"].pct_change() * 100

    # Feature 5: position in financial year (seasonality signal)
    # April = 1, March = 12
    df["fy_position"] = df["fy_month_num"]

    # Drop any remaining NaN rows
    df = df.dropna()

    logger.info(f"Features engineered: {len(df)} rows ready for model")
    return df

# Known seasonal events — statistically anomalous but business-expected
# Real fintech teams maintain this kind of events calendar
KNOWN_EVENTS = {
    "2023-10-01": "Festival season (Navratri/Dussehra) — expected spike",
    "2023-11-01": "Post-Diwali correction — expected dip",
    "2024-03-01": "FY-end surge — expected spike",
    "2024-10-01": "Festival season (Navratri/Dussehra) — expected spike",
    "2024-11-01": "Post-Diwali correction — expected dip",
    "2025-02-01": "Short month (Feb, 28 days) — expected dip",
    "2025-03-01": "FY-end surge — expected spike",
}

# ── STEP 3: Run Isolation Forest ───────────────────────────────
def run_isolation_forest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Isolation Forest works by randomly partitioning data.
    Anomalous points require fewer partitions to isolate —
    they're 'different' from the crowd.

    contamination=0.15 means we expect ~15% of months
    to be anomalous. With 23 months, that's ~3-4 flagged months.

    Why StandardScaler first:
    Features have different scales (MoM% is small, value_cr is huge)
    Scaling puts them on the same footing so no feature dominates.
    """
    feature_cols = [
        "volume_mom_pct",
        "value_mom_pct",
        "vol_val_divergence",
        "avg_txn_delta",
        "fy_position"
    ]

    X = df[feature_cols].values

    # Scale features to same range
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train Isolation Forest
    # random_state=42 ensures reproducible results
    model = IsolationForest(
        contamination=0.15,
        random_state=42,
        n_estimators=100
    )
    df["anomaly_score"]  = model.fit_predict(X_scaled)
    df["anomaly_raw"]    = model.decision_function(X_scaled)

    # Isolation Forest returns: -1 = anomaly, 1 = normal
    df["anomaly_flag"] = df["anomaly_score"].apply(
        lambda x: "ANOMALY" if x == -1 else "NORMAL"
    )

    # ── Z-Score calculation ─────────────────────────────────
    # Z-score measures how many standard deviations a month
    # is from the mean. |Z| > 1.5 = statistically unusual
    mean_vol = df["volume_mom_pct"].mean()
    std_vol  = df["volume_mom_pct"].std()

    df["z_score"] = (
        (df["volume_mom_pct"] - mean_vol) / std_vol
    ).round(2)

    anomaly_count = (df["anomaly_flag"] == "ANOMALY").sum()
    logger.info(
        f"Isolation Forest complete: "
        f"{anomaly_count} anomalies detected out of {len(df)} months"
    )

    return df


# ── STEP 4: Generate Anomaly Report ────────────────────────────
def print_anomaly_report(df: pd.DataFrame) -> None:
    """
    Prints a clean human-readable report explaining each anomaly.
    This is what you'd present to a risk team.
    """
    print("\n" + "="*60)
    print("UPI ECOSYSTEM — ANOMALY DETECTION REPORT")
    print("="*60)

    anomalies = df[df["anomaly_flag"] == "ANOMALY"].copy()
    anomalies = anomalies.sort_values("report_month")

    print(f"\nTotal months analysed : {len(df)}")
    print(f"Anomalies detected    : {len(anomalies)}")
    print(f"Normal months         : {len(df) - len(anomalies)}")

    print("\n--- ANOMALOUS MONTHS ---")
    for _, row in anomalies.iterrows():
        direction = "SPIKE" if row["volume_mom_pct"] > 0 else "DIP"
        print(f"\n  {row['month_name']} {int(row['calendar_year'])}")
        print(f"  Type         : {direction}")
        month_key = row["report_month"].strftime("%Y-%m-01")
        context = KNOWN_EVENTS.get(month_key, "Investigate — no known cause")
        print(f"  Context      : {context}")
        print(f"  Volume MoM   : {row['volume_mom_pct']:+.2f}%")
        print(f"  Value MoM    : {row['value_mom_pct']:+.2f}%")
        print(f"  Z-Score      : {row['z_score']}")
        print(f"  Divergence   : {row['vol_val_divergence']:+.2f}%")

    print("\n--- NORMAL MONTHS SUMMARY ---")
    normal = df[df["anomaly_flag"] == "NORMAL"]
    print(f"  Avg MoM growth : {normal['volume_mom_pct'].mean():+.2f}%")
    print(f"  Max MoM growth : {normal['volume_mom_pct'].max():+.2f}%")
    print(f"  Min MoM growth : {normal['volume_mom_pct'].min():+.2f}%")
    print("="*60 + "\n")


# ── STEP 5: Save visualization ─────────────────────────────────
def save_visualization(df: pd.DataFrame) -> None:
    """
    Creates a professional anomaly chart showing:
    - Monthly UPI volume as a line
    - Normal months as blue dots
    - Anomaly months as red stars
    - Z-score annotations on anomaly points
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    fig.suptitle(
        "UPI Ecosystem — Anomaly Detection Analysis",
        fontsize=16, fontweight="bold", y=0.98
    )

    # ── Chart 1: Volume with anomaly markers ─────────────────
    normal   = df[df["anomaly_flag"] == "NORMAL"]
    anomalies = df[df["anomaly_flag"] == "ANOMALY"]

    ax1.plot(
        df["report_month"], df["volume_mn"],
        color="#4A90D9", linewidth=2,
        marker="o", markersize=4, label="Monthly Volume"
    )

    # Highlight anomaly points as red stars
    ax1.scatter(
        anomalies["report_month"], anomalies["volume_mn"],
        color="#FF1744", s=200, zorder=5,
        marker="*", label="Anomaly Detected"
    )

    # Annotate each anomaly with month name + Z-score
    for _, row in anomalies.iterrows():
        ax1.annotate(
            f"{row['month_name'][:3]} {int(row['calendar_year'])}\nZ={row['z_score']}",
            xy=(row["report_month"], row["volume_mn"]),
            xytext=(10, 15), textcoords="offset points",
            fontsize=8, color="#FF1744",
            arrowprops=dict(arrowstyle="->", color="#FF1744", lw=1)
        )

    ax1.set_title("UPI Monthly Volume (Mn) — Anomalies Highlighted", 
                  fontsize=12)
    ax1.set_ylabel("Volume (Mn transactions)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_facecolor("#F8F9FA")

    # ── Chart 2: MoM Growth with anomaly bands ────────────────
    colors = [
        "#FF1744" if flag == "ANOMALY"
        else "#00C853" if pct >= 0
        else "#FF6B35"
        for flag, pct in zip(
            df["anomaly_flag"], df["volume_mom_pct"]
        )
    ]

    bars = ax2.bar(
        df["report_month"], df["volume_mom_pct"],
        color=colors, width=20, alpha=0.8
    )

    # Add zero line
    ax2.axhline(y=0, color="black", linewidth=0.8, linestyle="-")

    # Add mean line
    mean_growth = df["volume_mom_pct"].mean()
    ax2.axhline(
        y=mean_growth, color="#4A90D9",
        linewidth=1.5, linestyle="--",
        label=f"Mean: {mean_growth:.1f}%"
    )

    ax2.set_title(
        "Month-over-Month Volume Growth % — Red = Anomaly",
        fontsize=12
    )
    ax2.set_ylabel("MoM Growth %")
    ax2.set_xlabel("Month")
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.set_facecolor("#F8F9FA")

    # Legend patches
    patches = [
        mpatches.Patch(color="#FF1744", label="Anomaly month"),
        mpatches.Patch(color="#00C853", label="Normal growth"),
        mpatches.Patch(color="#FF6B35", label="Normal dip"),
    ]
    ax2.legend(handles=patches, loc="upper left")

    plt.tight_layout()

    # Save to project folder
    output_path = PROCESSED_DIR / "anomaly_chart.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info(f"Chart saved: {output_path}")
    print(f"Chart saved to: {output_path}")
    plt.close()


# ── STEP 6: Save results to PostgreSQL ─────────────────────────
def save_to_db(df: pd.DataFrame, engine) -> None:
    """
    Creates an anomaly_results table and saves findings.
    This makes the anomaly data available to Power BI.
    """
    # Create table if not exists
    create_table_sql = """
        DROP TABLE IF EXISTS anomaly_results;
        CREATE TABLE anomaly_results (
            id              SERIAL PRIMARY KEY,
            report_month    DATE,
            month_name      VARCHAR(20),
            calendar_year   INT,
            financial_year  VARCHAR(10),
            volume_mn       NUMERIC(12,2),
            volume_mom_pct  NUMERIC(8,2),
            value_mom_pct   NUMERIC(8,2),
            z_score         NUMERIC(8,2),
            anomaly_flag    VARCHAR(10),
            anomaly_raw     NUMERIC(10,6)
        );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    # Save results
    save_cols = [
        "report_month", "month_name", "calendar_year",
        "financial_year", "volume_mn", "volume_mom_pct",
        "value_mom_pct", "z_score", "anomaly_flag", "anomaly_raw"
    ]

    result_df = df[save_cols].copy()
    result_df["report_month"] = result_df["report_month"].dt.date

    with engine.begin() as conn:
        result_df.to_sql(
            "anomaly_results", conn,
            if_exists="append",
            index=False,
            method="multi"
        )

    logger.info(f"Saved {len(result_df)} rows to anomaly_results table")


# ── STEP 7: Save CSV ───────────────────────────────────────────
def save_to_csv(df: pd.DataFrame) -> None:
    output_path = PROCESSED_DIR / "anomaly_results.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"CSV saved: {output_path}")


# ── MAIN ───────────────────────────────────────────────────────
def main():
    logger.info("="*60)
    logger.info("Starting anomaly detection pipeline")
    logger.info("="*60)

    engine = get_engine()

    # Run pipeline
    df       = load_upi_data(engine)
    df       = engineer_features(df)
    df       = run_isolation_forest(df)

    # Output
    print_anomaly_report(df)
    save_visualization(df)
    save_to_db(df, engine)
    save_to_csv(df)

    logger.info("Anomaly detection complete.")


if __name__ == "__main__":
    main()