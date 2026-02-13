import os
import io
import re
import time
import ssl
import boto3
import urllib3
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from botocore.config import Config
from datetime import datetime
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# S3 Configuration
S3_BUCKET = os.getenv('BUCKET_NAME') 
S3_ENDPOINT = os.getenv('R2_ENDPOINT_URL')
AWS_ACCESS_KEY = os.getenv('R2_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('R2_SECRET_ACCESS_KEY')

ATTENDANCE_KEY_PATH = f"silver/attendance_stats_{datetime.now().strftime('%Y%m%d')}.csv"
FINANCIAL_KEY_PATH = f"silver/financial_valuations_{datetime.now().strftime('%Y%m%d')}.csv"
STATIC_FILE_NAME = 'silver/club-sample_static.csv'

LEAGUE_URLS = [
    "https://www.transfermarkt.co.uk/premier-league/besucherzahlen/wettbewerb/GB1",
    "https://www.transfermarkt.co.uk/championship/besucherzahlen/wettbewerb/GB2",
    "https://www.transfermarkt.co.uk/league-one/besucherzahlen/wettbewerb/GB3",
    "https://www.transfermarkt.co.uk/league-two/besucherzahlen/wettbewerb/GB4",
    "https://www.transfermarkt.co.uk/national-league/besucherzahlen/wettbewerb/CNAT"
]

TM_CLUBS_FOR_VALUE = {
    'Arsenal': 'https://www.transfermarkt.co.uk/arsenal-fc/startseite/verein/11',
    'Oldham Athletic': 'https://www.transfermarkt.co.uk/oldham-athletic/startseite/verein/1078'
}

def get_s3_client():
    return boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY, config=Config(signature_version='s3v4'), verify=False)

def clean_attendance(text):
    if not text: return 0
    clean = re.sub(r'[^\d]', '', text)
    try: return int(clean)
    except: return 0

def parse_tm_value(text):
    if not text: return 0
    match = re.search(r'([€£$]?[\d.,]+[kKmMbB]?[nN]?)', text)
    if not match: return 0
    raw_str = match.group(1).replace('€', '').replace('£', '').replace('$', '').lower()
    try:
        multiplier = 1
        if 'bn' in raw_str: multiplier = 1_000_000_000
        elif 'm' in raw_str: multiplier = 1_000_000
        elif 'k' in raw_str: multiplier = 1_000
        return int(float(re.sub(r'[^\d.]', '', raw_str)) * multiplier)
    except: return 0

def get_mapping_dict(s3_client):
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key='silver/all_english_teams_master.csv')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return dict(zip(df['raw_name'], df['standard_name']))
    except: return {}

def run_scraper():
    s3 = get_s3_client()
    mapping_dict = get_mapping_dict(s3)
    
    options = uc.ChromeOptions()
    options.add_argument('--start-maximized')
    driver = uc.Chrome(options=options, version_main=143)

    attendance_results = []
    financial_results = []

    try:
        # --- 1. ATTENDANCE SCRAPE ---
        print("🏟️ Starting Attendance Scrape...")
        for league_url in LEAGUE_URLS:
            league_name = league_url.split('/')[3]
            driver.get(league_url)
            time.sleep(4)
            
            try:
                table = driver.find_element(By.XPATH, "(//table[@class='items'])[1]")
                rows = table.find_elements(By.XPATH, ".//tbody//tr")
                limit = 20 if "premier" in league_name else 24
                
                for i, row in enumerate(rows):
                    if i >= limit: break
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 8: continue

                    # Clean Name (Remove Stadium Prefix)
                    raw_name_text = cells[1].get_attribute("textContent").strip()
                    # Regex: Find the first capital letter that is part of the club name
                    # (Standard TM format is: StadiumNameClubName)
                    clean_name = re.sub(r'^[a-zA-Z0-9\s\.\-]+(?=[A-Z][a-z])', '', raw_name_text).strip()
                    if not clean_name: clean_name = raw_name_text

                    avg_att = clean_attendance(cells[7].get_attribute("textContent"))
                    
                    if i == 0:
                        print(f"   [{league_name}] Scraped: '{clean_name}' | Att: {avg_att}")

                    standard_name = mapping_dict.get(clean_name, clean_name)
                    if avg_att > 0:
                        attendance_results.append({
                            "Club Name": standard_name,
                            "Season Year": 2026,
                            "Average Attendance": avg_att
                        })
            except Exception as e:
                print(f"   ❌ Error in {league_name}: {e}")

        # --- 2. FINANCIAL SCRAPE ---
        print("\n💰 Starting Financial Value Scrape...")
        for club_key, url in TM_CLUBS_FOR_VALUE.items():
            try:
                driver.get(url)
                time.sleep(3)
                val_text = driver.find_element(By.CLASS_NAME, "data-header__market-value-wrapper").get_attribute("textContent")
                val = parse_tm_value(val_text)
                financial_results.append({"Club Name": mapping_dict.get(club_key, club_key), "Financial Value": val})
                print(f"   Processed {club_key}... Value: {val}")
            except:
                print(f"   ⚠️ Could not find value for {club_key}")

        # --- 3. UPLOAD ---
        if attendance_results:
            df_att = pd.DataFrame(attendance_results).drop_duplicates(subset=['Club Name'])
            csv_att = io.StringIO()
            df_att.to_csv(csv_att, index=False)
            s3.put_object(Bucket=S3_BUCKET, Key=ATTENDANCE_KEY_PATH, Body=csv_att.getvalue().encode('utf-8'))
            print(f"\n🚀 SUCCESS: Uploaded {len(df_att)} attendance records.")

        if financial_results:
            df_fin = pd.DataFrame(financial_results)
            csv_fin = io.StringIO()
            df_fin.to_csv(csv_fin, index=False)
            s3.put_object(Bucket=S3_BUCKET, Key=FINANCIAL_KEY_PATH, Body=csv_fin.getvalue().encode('utf-8'))
            print(f"🚀 SUCCESS: Uploaded {len(df_fin)} financial records.")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_scraper()