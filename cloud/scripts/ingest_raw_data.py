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
    print("🕸️ Scraping BBC Sport (Multi-League Mode)...")
    
    # Define the leagues we want to grab
    leagues = {
        "premier-league": "Premier League",
        "championship": "Championship",
        "league-one": "League One",
        "league-two": "League Two"
    }
    
    all_leagues_data = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}

    for slug, formal_name in leagues.items():
        url = f"https://www.bbc.co.uk/sport/football/{slug}/table"
        try:
            response = requests.get(url, headers=headers)
            tables = pd.read_html(StringIO(response.text))
            
            if not tables:
                continue
                
            df = tables[0]
            
            # Use our robust logic from the last step to find the team column
            teams = []
            for col in df.columns:
                sample = df[col].astype(str).tolist()
                # Check for any common team indicator
                if any(len(s) > 3 for s in sample): 
                    teams = sample
                    break
            
            # Clean the names
            clean_teams = []
            for t in teams:
                t_str = str(t).split('Form:')[0]
                t_str = ''.join([i for i in t_str if not i.isdigit()]).strip()
                if len(t_str) > 2 and t_str not in ['Team', 'nan']:
                    clean_teams.append(t_str)
            
            all_leagues_data[formal_name] = list(dict.fromkeys(clean_teams))
            print(f"✅ Found {len(clean_teams)} teams in {formal_name}")

        except Exception as e:
            print(f"❌ Failed to scrape {formal_name}: {e}")

    return all_leagues_data

# --- UPDATED EXECUTION BLOCK ---
if __name__ == "__main__":
    s3 = get_s3_client()
    bucket = os.getenv('BUCKET_NAME')

    # API Task (Still just PL for now as per free tier)
    api_data = fetch_api_data()
    if api_data:
        s3.put_object(Bucket=bucket, Key="raw/api_pl_standings.json", Body=json.dumps(api_data))
        print("💾 API data saved to R2.")

    # Multi-League Scrape Task
    multi_league_results = scrape_bbc_data()
    if multi_league_results:
        s3.put_object(
            Bucket=bucket, 
            Key="raw/bbc_scraped_teams_all.json", 
            Body=json.dumps(multi_league_results)
        )
        print("💾 All Leagues data saved to R2 (Bronze).")