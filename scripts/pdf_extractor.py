"""
pdf_extractor.py
----------------
Extracts transaction tables from NPCI and RBI PDF reports.
Saves raw extracted data as CSVs into data/processed/

Why pdfplumber first, tabula as fallback?
- pdfplumber handles text-based PDFs better
- tabula handles scanned/image PDFs better
- Some NPCI PDFs are inconsistent, so we try both
"""

import pdfplumber
import tabula
import pandas as pd
import os
import logging
from pathlib import Path

# ── Logging setup ──────────────────────────────────────────────────────────────
# Logs go to both terminal AND a log file so you can debug later
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Path setup ─────────────────────────────────────────────────────────────────
# Using pathlib — works correctly on Windows, Mac, Linux without path issues
BASE_DIR   = Path(__file__).resolve().parent.parent
PDF_DIR    = BASE_DIR / "data" / "raw" / "pdfs"
OUTPUT_DIR = BASE_DIR / "data" / "processed"

# Create output dir if it doesn't exist yet
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ── FUNCTION 1: Extract using pdfplumber ───────────────────────────────────────
def extract_with_pdfplumber(pdf_path: Path) -> list[pd.DataFrame]:
    """
    Opens a PDF and extracts ALL tables from ALL pages.
    Returns a list of DataFrames — one per table found.
    
    Why we extract all tables:
    NPCI PDFs have multiple tables per report (volume table, value table, 
    PSP-wise table). We grab everything and filter later in data_cleaner.py
    """
    tables = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            logger.info(f"Opened PDF: {pdf_path.name} | Pages: {len(pdf.pages)}")
            
            for page_num, page in enumerate(pdf.pages, start=1):
                # extract_tables() returns a list of tables on that page
                # Each table is a list of rows, each row is a list of cell values
                raw_tables = page.extract_tables()
                
                if not raw_tables:
                    continue  # skip pages with no tables
                
                for table_idx, raw_table in enumerate(raw_tables):
                    if not raw_table or len(raw_table) < 2:
                        continue  # skip empty or single-row tables
                    
                    # First row is usually the header
                    headers = raw_table[0]
                    rows    = raw_table[1:]
                    
                    df = pd.DataFrame(rows, columns=headers)
                    
                    # Tag each table with its source for traceability
                    df["_source_file"] = pdf_path.name
                    df["_page_number"] = page_num
                    df["_table_index"] = table_idx
                    
                    tables.append(df)
                    logger.info(
                        f"  Page {page_num} | Table {table_idx} | "
                        f"Shape: {df.shape}"
                    )
        
    except Exception as e:
        logger.error(f"pdfplumber failed on {pdf_path.name}: {e}")
    
    return tables


# ── FUNCTION 2: Extract using tabula (fallback) ────────────────────────────────
def extract_with_tabula(pdf_path: Path) -> list[pd.DataFrame]:
    """
    Fallback extractor using tabula-py.
    Used when pdfplumber returns empty or garbled tables.
    
    tabula works differently — it uses Java under the hood and 
    is better at detecting table borders in scanned PDFs.
    """
    tables = []
    
    try:
        # pages="all" extracts from every page
        # multiple_tables=True returns each table separately
        raw_tables = tabula.read_pdf(
            str(pdf_path),
            pages="all",
            multiple_tables=True,
            silent=True      # suppresses Java warnings in terminal
        )
        
        for idx, df in enumerate(raw_tables):
            if df.empty:
                continue
            df["_source_file"] = pdf_path.name
            df["_table_index"] = idx
            tables.append(df)
            logger.info(f"  tabula | Table {idx} | Shape: {df.shape}")
            
    except Exception as e:
        logger.error(f"tabula failed on {pdf_path.name}: {e}")
    
    return tables


# ── FUNCTION 3: Smart extractor — tries pdfplumber first ──────────────────────
def extract_pdf(pdf_path: Path) -> list[pd.DataFrame]:
    """
    Main extraction function.
    Tries pdfplumber first. If it returns nothing useful, falls back to tabula.
    This handles the inconsistency across different NPCI/RBI PDF formats.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing: {pdf_path.name}")
    
    tables = extract_with_pdfplumber(pdf_path)
    
    # If pdfplumber found nothing meaningful, try tabula
    if not tables:
        logger.warning(
            f"pdfplumber found no tables in {pdf_path.name}. "
            f"Trying tabula fallback..."
        )
        tables = extract_with_tabula(pdf_path)
    
    logger.info(f"Total tables extracted from {pdf_path.name}: {len(tables)}")
    return tables


# ── FUNCTION 4: Save extracted tables as CSVs ─────────────────────────────────
def save_tables(tables: list[pd.DataFrame], pdf_name: str) -> None:
    """
    Saves each extracted DataFrame as a separate CSV.
    
    Naming convention: npci_2024_01_table_0.csv
    Clean names = easy to identify + load in next script
    """
    # Strip .pdf extension, replace spaces with underscores
    base_name = pdf_name.replace(".pdf", "").replace(" ", "_").lower()
    
    for idx, df in enumerate(tables):
        output_path = OUTPUT_DIR / f"{base_name}_table_{idx}.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        # utf-8-sig handles special characters in Indian text correctly
        logger.info(f"Saved: {output_path.name}")


# ── MAIN: Process all PDFs in data/raw/pdfs/ ──────────────────────────────────
def main():
    """
    Loops through every PDF in the pdfs folder and extracts tables.
    Drop any new PDF in that folder and re-run — it gets processed automatically.
    """
    pdf_files = list(PDF_DIR.glob("*.pdf"))
    
    if not pdf_files:
        logger.warning(
            f"No PDFs found in {PDF_DIR}. "
            f"Please download NPCI/RBI reports and place them there."
        )
        return
    
    logger.info(f"Found {len(pdf_files)} PDF(s) to process")
    
    for pdf_path in pdf_files:
        tables = extract_pdf(pdf_path)
        
        if tables:
            save_tables(tables, pdf_path.name)
        else:
            logger.warning(f"No tables extracted from {pdf_path.name}")
    
    logger.info("\nExtraction complete. Check data/processed/ for output CSVs.")


if __name__ == "__main__":
    main()