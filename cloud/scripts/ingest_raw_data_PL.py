import os
import json
import requests
import boto3
import pandas as pd  # <--- THIS IS THE MISSING LINE
from io import StringIO
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path

# --- 1. SMART PATHING (Finds .env in the parent folder) ---
script_dir = Path(__file__).resolve().parent
env_path = script_dir.parent / '.env'
load_dotenv(dotenv_path=env_path)

def get_s3_client():
    return boto3.client(
        service_name='s3',
        endpoint_url=os.getenv('R2_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
        region_name='auto'
    )

# --- 2. API INGESTION ---
def fetch_api_data():
    print("🚀 Fetching API data...")
    api_key = os.getenv('FOOTBALL_DATA_API_KEY')
    url = "https://api.football-data.org/v4/competitions/PL/standings"
    headers = {'X-Auth-Token': api_key}
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ API Success!")
            return response.json()
        else:
            print(f"❌ API Error: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ API Request failed: {e}")
        return None

# --- 3. SCRAPING (Pandas Mode) ---
def scrape_bbc_data():
    print("🕸️ Scraping BBC Sport (Robust Pandas Mode)...")
    url = "https://www.bbc.co.uk/sport/football/premier-league/table"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
    
    try:
        response = requests.get(url, headers=headers)
        tables = pd.read_html(StringIO(response.text))
        
        if not tables:
            print("⚠️ No tables found at all.")
            return None
            
        df = tables[0]
        
        # DEBUG: Let's see what the columns actually look like
        print(f"DEBUG: Columns found: {list(df.columns)}")

        # Strategy: Look for the column that has 'Arsenal' or 'Man City' in it
        # Or, just use the column that contains the most text-heavy strings
        teams = []
        for col in df.columns:
            # Check if this column has actual team-like names
            sample = df[col].astype(str).tolist()
            if any("Arsenal" in s or "City" in s for s in sample):
                teams = sample
                break
        
        if not teams:
            # Fallback to the 3rd column (index 2) which is usually the name
            teams = df.iloc[:, 2].tolist() if len(df.columns) > 2 else df.iloc[:, 1].tolist()

        # Clean up: BBC often merges the rank and name "1Man City" or adds "Form"
        # This regex/split cleans the junk off the names
        clean_teams = []
        for t in teams:
            t_str = str(t)
            # Remove "Form:" and anything after it
            t_str = t_str.split('Form:')[0]
            # Remove leading numbers (like "1Man City" -> "Man City")
            t_str = ''.join([i for i in t_str if not i.isdigit()]).strip()
            
            if len(t_str) > 2 and t_str not in ['Team', 'nan']:
                clean_teams.append(t_str)
        
        # Deduplicate while keeping order
        clean_teams = list(dict.fromkeys(clean_teams))

        print(f"✅ Scraper Found {len(clean_teams)} teams.")
        return {"scraped_teams": clean_teams[:20]}

    except Exception as e:
        print(f"❌ Scraping failed: {e}")
        return None

# --- 4. MAIN EXECUTION ---
if __name__ == "__main__":
    s3 = get_s3_client()
    bucket = os.getenv('BUCKET_NAME')

    # API Task
    api_data = fetch_api_data()
    if api_data:
        s3.put_object(Bucket=bucket, Key="raw/api_pl_standings.json", Body=json.dumps(api_data))
        print("💾 API data saved to R2 (Bronze).")

    # Scrape Task
    scrape_data = scrape_bbc_data()
    if scrape_data:
        s3.put_object(Bucket=bucket, Key="raw/bbc_scraped_teams.json", Body=json.dumps(scrape_data))
        print("💾 Scraped data saved to R2 (Bronze).")