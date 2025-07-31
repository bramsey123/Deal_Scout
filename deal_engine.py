print("Running Deal Engine...")

import os
import requests
from bs4 import BeautifulSoup
from airtable import Airtable
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import feedparser
import xml.etree.ElementTree as ET
from datetime import datetime
import time
import random
from fake_useragent import UserAgent

# Load secrets from .env
load_dotenv()
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE")
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_TABLE = os.getenv("AIRTABLE_TABLE", "Deals")

airtable = Airtable(AIRTABLE_BASE, AIRTABLE_TABLE, AIRTABLE_TOKEN)

# URLs
MARKETS = {
    "BizBuySell": "https://www.bizbuysell.com/business-brokers/texas/houston-businesses-for-sale/",
    "DealStream": "https://dealstream.com/texas-businesses-for-sale?location=Houston"
}
import csv
from io import StringIO
import pandas as pd
from io import BytesIO
import mimetypes

SBA_FEED = "https://sba-llms-prd-public.sbalenderportal.com/SBA-Monthly-Lender7AActivity.xlsx"

# --- Scrapers ---

def scrape_dealstream_rss():
 from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

DEALSTREAM_URL = "https://dealstream.com/texas-businesses-for-sale"

def scrape_dealstream_playwright():
    print("Launching Playwright for DealStream …")
    listings = []
    ua = UserAgent()

    with sync_playwright() as p:
        # Launch with stealth settings
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        context = browser.new_context(
            user_agent=ua.random,
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York'
        )
        
        page = context.new_page()
        
        # Add stealth scripts
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
        """)
        
        # Navigate with random delay
        time.sleep(random.uniform(1, 3))
        page.goto(DEALSTREAM_URL, timeout=60000, wait_until='networkidle')
        
        # Wait and scroll more naturally
        page.wait_for_timeout(random.randint(2000, 4000))
        
        # Try multiple selectors for robustness
        selectors_to_try = [
            'div[data-testid*="listing"]',
            'div[class*="listing"]',
            'article[class*="card"]',
            '.listing-card',
            '[data-cy="listing-card"]'
        ]
        
        listings_found = False
        for selector in selectors_to_try:
            try:
                page.wait_for_selector(selector, timeout=10000)
                listings_found = True
                break
            except:
                continue
        
        if not listings_found:
            print("⚠️ Could not find listing elements. Trying generic approach...")
            page.wait_for_timeout(5000)
        
        # Scroll gradually to load more content
        for i in range(3):
            page.mouse.wheel(0, 1000)
            time.sleep(random.uniform(1, 2))
        
        html = page.content()
        
        # Debug: Save HTML to see what we're getting
        with open('/tmp/dealstream_debug.html', 'w') as f:
            f.write(html)
        print(f"📄 Saved page HTML to /tmp/dealstream_debug.html ({len(html)} chars)")
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract listings from JSON-LD structured data
        import json
        import re
        
        # Find JSON-LD script tags
        json_scripts = soup.find_all('script', type='application/ld+json')
        
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                
                # Look for SearchResultsPage with listings
                if data.get('@type') == 'SearchResultsPage' and 'about' in data:
                    for item_wrapper in data['about']:
                        item = item_wrapper.get('item', {})
                        
                        if item.get('@type') == 'Product':
                            # Extract basic info
                            title = item.get('name', 'Business Listing')
                            url = item.get('url', '')
                            description = item.get('description', '')
                            
                            # Extract price from offers
                            price = None
                            location = None
                            
                            offers = item.get('offers')
                            if offers and isinstance(offers, dict):
                                price_val = offers.get('price')
                                if price_val:
                                    price = f"${price_val:,}"
                                
                                # Extract location from offers
                                available_at = offers.get('availableAtOrFrom', {})
                                if available_at:
                                    address = available_at.get('address', {})
                                    city = address.get('addressLocality', '')
                                    region = address.get('addressRegion', '')
                                    if city and region:
                                        location = f"{city}, {region}"
                                    elif region:
                                        location = region
                            
                            # Clean and validate
                            if url and len(title) > 5:
                                listing = {
                                    "source": "DealStream",
                                    "title": title.replace(' - DealStream', ''),
                                    "url": url,
                                    "price": price,
                                    "location": location,
                                    "description": description[:200] + "..." if len(description) > 200 else description,
                                    "scraped_at": datetime.now().isoformat()
                                }
                                listings.append(listing)
                                
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                continue
        
        print(f"✓ Parsed {len(listings)} DealStream listings.")
        browser.close()
    
    return listings

def scrape_sba_feed(url):
    """
    Accepts either a CSV or XLS/XLSX SBA loan feed and returns a
    list of listing dicts. Automatically detects file type.
    """
    print("Fetching SBA loan feed…")
    res = requests.get(url, timeout=20)
    res.raise_for_status()

    listings = []

    # --- Detect format -------------------------------------------------
    mime, _ = mimetypes.guess_type(url)
    is_excel = (mime and "spreadsheet" in mime) or url.endswith((".xls", ".xlsx"))

    # --- Parse ---------------------------------------------------------
    if is_excel:
        df = pd.read_excel(BytesIO(res.content))        # Excel → DataFrame
        rows = df.to_dict(orient="records")
    else:
        reader = csv.DictReader(StringIO(res.text))     # CSV → DictReader
        rows = list(reader)

    # --- Extract fields ------------------------------------------------
    for row in rows:
        biz   = row.get("Business Name", "Unknown")
        city  = row.get("City", "")
        state = row.get("State", "")
        amt   = row.get("Gross Approval", "")
        listings.append({
            "source": "SBA 7a",
            "title": f"{biz} — ${amt} in {city}, {state}"
        })

    return listings

def filter_listings(listings, min_price=None, max_price=None, required_locations=None):
    """Filter listings based on basic criteria"""
    filtered = []
    
    for listing in listings:
        # Price filtering
        if min_price or max_price:
            price_str = listing.get('price', '')
            if price_str:
                # Extract numeric value from price string
                import re
                price_match = re.search(r'[\d,]+', price_str.replace('$', '').replace(',', ''))
                if price_match:
                    try:
                        price_val = int(price_match.group())
                        if min_price and price_val < min_price:
                            continue
                        if max_price and price_val > max_price:
                            continue
                    except ValueError:
                        pass
        
        # Location filtering
        if required_locations:
            location = listing.get('location') or ''
            title = listing.get('title') or ''
            if not any(loc.lower() in location.lower() or loc.lower() in title.lower() for loc in required_locations):
                continue
        
        filtered.append(listing)
    
    return filtered

# --- Main Engine ---

def run_engine():
    all_listings = []

    # Scrape DealStream
    try:
        dealstream_listings = scrape_dealstream_playwright()
        all_listings.extend(dealstream_listings)
        print(f"✓ Found {len(dealstream_listings)} DealStream listings")
    except Exception as e:
        print(f"⚠️ DealStream scraping failed: {e}")

    # --- SBA feed temporarily disabled ---------------------------------
    # try:
    #     all_listings.extend(scrape_sba_feed(SBA_FEED))
    # except requests.exceptions.RequestException as e:
    #     print(f"⚠️ SBA feed fetch failed: {e}")

    # Apply basic filters (customize these values)
    filtered_listings = filter_listings(
        all_listings,
        min_price=50000,      # Minimum $50k
        max_price=5000000,    # Maximum $5M
        required_locations=['houston', 'texas', 'tx']  # Texas/Houston focus
    )
    
    print(f"After filtering: {len(filtered_listings)} listings (from {len(all_listings)} total)")

    if not filtered_listings:
        print("No listings match criteria. Exiting.")
        return

    print("Uploading to Airtable...")
    uploaded = 0
    
    for listing in filtered_listings:
        try:
            # Start with basic fields that should exist
            record = {
                "Source": listing["source"],
                "Title": listing["title"]
            }
            
            # Add optional fields if they exist in your Airtable
            if listing.get("url"):
                record["URL"] = listing["url"]
            if listing.get("price"):
                record["Price"] = listing["price"]  
            if listing.get("location"):
                record["Location"] = listing["location"]
                
            airtable.insert(record, typecast=True)
            uploaded += 1
        except Exception as e:
            print(f"⚠️ Upload failed for {listing['title']}: {e}")

    print(f"✓ Upload complete. {uploaded}/{len(filtered_listings)} listings uploaded.")

if __name__ == "__main__":
    run_engine()