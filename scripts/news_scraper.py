"""
news_scraper.py
---------------
Scrapes fintech/UPI related news headlines from Inc42.
Saves results to data/raw/news/news_raw.csv

Why scrape news?
We tag fraud-related headlines by type (phishing, SIM swap, etc.)
and correlate news frequency with UPI anomaly months in Phase 6.
This is what makes the project analytically unique.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import json
from pathlib import Path
from datetime import datetime

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/scraping.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent
NEWS_DIR  = BASE_DIR / "data" / "raw" / "news"
NEWS_DIR.mkdir(parents=True, exist_ok=True)

# ── Headers — makes your scraper look like a real browser ─────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Fraud keyword taxonomy ─────────────────────────────────────────────────────
# These categories are how we'll classify news headlines
# directly maps to our fraud_taxonomy treemap in Power BI
FRAUD_KEYWORDS = {
    "phishing":       ["phishing", "fake link", "fake upi", "fake id"],
    "sim_swap":       ["sim swap", "sim cloning", "number port"],
    "vishing":        ["vishing", "voice call fraud", "phone fraud"],
    "qr_fraud":       ["qr code", "fake qr", "scan and pay fraud"],
    "account_takeover":["account takeover", "hacked account", "unauthorized access"],
    "money_mule":     ["money mule", "mule account"],
    "general_fraud":  ["upi fraud", "digital payment fraud", "online fraud"]
}


# ── FUNCTION 1: Tag a headline with fraud category ────────────────────────────
def tag_fraud_type(headline: str) -> str:
    """
    Scans a headline for fraud keywords and returns the category.
    Returns 'not_fraud' if no fraud keywords found.
    
    Why we do this:
    Instead of manually reading 500 headlines, we auto-tag them.
    This lets us count fraud incidents by type per month.
    """
    headline_lower = headline.lower()
    
    for fraud_type, keywords in FRAUD_KEYWORDS.items():
        if any(kw in headline_lower for kw in keywords):
            return fraud_type
    
    return "not_fraud"


# ── FUNCTION 2: Scrape Inc42 UPI/fintech news ──────────────────────────────────
def scrape_inc42(max_pages: int = 5) -> list[dict]:
    """
    Scrapes news headlines + dates from Inc42's UPI tag page.
    
    max_pages: how many pages to scrape (5 pages ≈ ~50 headlines)
    Returns a list of dicts with headline, date, url, fraud_type
    """
    base_url = "https://inc42.com/tag/upi/"
    articles = []
    
    for page in range(1, max_pages + 1):
        url = base_url if page == 1 else f"{base_url}page/{page}/"
        
        logger.info(f"Scraping Inc42 page {page}: {url}")
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()  # raises error if status != 200
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            break
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Inc42 article cards — inspect element on Inc42 to verify these selectors
        # These are the most common article card patterns on Inc42
        article_cards = (
            soup.find_all("div", class_="post-content") or
            soup.find_all("article") or
            soup.find_all("div", class_="inc42-card")
        )
        
        if not article_cards:
            logger.warning(f"No articles found on page {page}. Site structure may have changed.")
            break
        
        for card in article_cards:
            try:
                # Extract headline
                title_tag = (
                    card.find("h2") or
                    card.find("h3") or
                    card.find("a", class_="post-title")
                )
                if not title_tag:
                    continue
                headline = title_tag.get_text(strip=True)
                
                # Extract URL
                link_tag = card.find("a", href=True)
                article_url = link_tag["href"] if link_tag else ""
                
                # Extract date
                date_tag = card.find("time") or card.find("span", class_="date")
                article_date = date_tag.get_text(strip=True) if date_tag else "unknown"
                
                # Tag fraud type automatically
                fraud_type = tag_fraud_type(headline)
                
                articles.append({
                    "headline":   headline,
                    "date":       article_date,
                    "url":        article_url,
                    "fraud_type": fraud_type,
                    "source":     "inc42",
                    "scraped_at": datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.warning(f"Error parsing article card: {e}")
                continue
        
        logger.info(f"Page {page}: {len(article_cards)} articles found")
        
        # Polite delay between requests — don't hammer the server
        # This is basic scraping ethics
        time.sleep(2)
    
    return articles


# ── FUNCTION 3: Save scraped data ─────────────────────────────────────────────
def save_news(articles: list[dict]) -> None:
    """
    Saves articles as both CSV (for DB loading) and JSON (for backup).
    """
    if not articles:
        logger.warning("No articles to save.")
        return
    
    df = pd.DataFrame(articles)
    
    # Save as CSV
    csv_path = NEWS_DIR / "news_raw.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Saved {len(df)} articles to {csv_path}")
    
    # Save as JSON backup
    json_path = NEWS_DIR / "news_raw.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON backup saved to {json_path}")
    
    # Quick summary
    fraud_counts = df[df["fraud_type"] != "not_fraud"]["fraud_type"].value_counts()
    logger.info(f"\nFraud article breakdown:\n{fraud_counts.to_string()}")


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    logger.info("Starting news scraper...")
    articles = scrape_inc42(max_pages=5)
    save_news(articles)
    logger.info("News scraping complete.")


if __name__ == "__main__":
    main()