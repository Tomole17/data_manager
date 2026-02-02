import os
import io
import re
import time
import ssl
import boto3
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from botocore.config import Config
from datetime import datetime
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# --- CONFIGURATION ---
S3_BUCKET = os.getenv('BUCKET_NAME') 
S3_ENDPOINT = os.getenv('R2_ENDPOINT_URL')
AWS_ACCESS_KEY = os.getenv('R2_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('R2_SECRET_ACCESS_KEY')

REPORTING_DATE = datetime.now().strftime("%Y-%m-%d")
# Cloud path remains date-based for folder organization
S3_KEY_PATH = f"silver/facebook_stats_{datetime.now().strftime('%Y%m%d')}.csv"

# Global SSL Patch to prevent 'sslv3 alert handshake failure' on Windows
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

# CLUB URL DICTIONARY (Expand this to all 92)
CLUBS = {
    'Arsenal': 'https://www.facebook.com/Arsenal',
    'Aston Villa': 'https://www.facebook.com/avfcofficial',
    'AFC Bournemouth': 'https://www.facebook.com/afcb/',
    'Brentford': 'https://www.facebook.com/brentfordfootballclub1889/',
    'Brighton & Hove Albion': 'https://www.facebook.com/OfficialBHAFC',
    'Chelsea': 'https://www.facebook.com/ChelseaFC',
    'Crystal Palace': 'https://www.facebook.com/officialcpfc',
    'Everton': 'https://www.facebook.com/Everton',
    'Fulham': 'https://www.facebook.com/FulhamFC',
    'Ipswich Town': 'https://www.facebook.com/OfficialITFC',
    'Leicester City': 'https://www.facebook.com/LCFC',
    'Liverpool': 'https://www.facebook.com/LiverpoolFC',
    'Manchester City': 'https://www.facebook.com/mancity',
    'Manchester United': 'https://www.facebook.com/manchesterunited',
    'Newcastle United': 'https://www.facebook.com/newcastleunited',
    'Nottingham Forest': 'https://www.facebook.com/officialnffc/',
    'Southampton': 'https://www.facebook.com/SouthamptonFC',
    'Tottenham Hotspur': 'https://www.facebook.com/TottenhamHotspur',
    'West Ham United': 'https://www.facebook.com/WestHam/',
    'Wolverhampton Wanderers': 'https://www.facebook.com/Wolves',
        
    'Blackburn Rovers': 'https://www.facebook.com/1Rovers/',
    'Bristol City': 'https://www.facebook.com/BristolCityFC',
    'Burnley': 'https://www.facebook.com/burnleyofficial/',
    'Cardiff City': 'https://www.facebook.com/CardiffCityFC',
    'Coventry City': 'https://www.facebook.com/coventrycityfc',
    'Derby County': 'https://www.facebook.com/dcfcofficial/',
    'Hull City': 'https://www.facebook.com/hullcity',
    'Leeds United': 'https://www.facebook.com/LeedsUnited',
    'Luton Town': 'https://www.facebook.com/LutonTown/',
    'Middlesbrough': 'https://www.facebook.com/MFCofficial/',
    'Millwall': 'https://www.facebook.com/MillwallFCOfficial/',
    'Norwich City': 'https://www.facebook.com/NorwichCityFootballClub/',
    'Oxford United': 'https://www.facebook.com/OUFCOfficial',
    'Plymouth Argyle': 'https://www.facebook.com/Argyle/',
    'Portsmouth': 'https://www.facebook.com/officialpfc/',
    'Preston North End': 'https://www.facebook.com/officialpnefc',
    'Queens Park Rangers': 'https://www.facebook.com/OfficialQPRFC/',
    'Sheffield United': 'https://www.facebook.com/sheffieldunited',
    'Sheffield Wednesday': 'https://www.facebook.com/sheffieldwednesday',
    'Stoke City': 'https://www.facebook.com/stokecity',
    'Sunderland': 'https://www.facebook.com/SunderlandAFC',
    'Swansea City': 'https://www.facebook.com/SwanseaCityFC',
    'Watford': 'https://www.facebook.com/watfordfc',
    'West Bromwich Albion': 'https://www.facebook.com/westbromwichalbionofficial/',

    'Barnsley': 'https://www.facebook.com/BarnsleyFC/',
    'Birmingham City': 'https://www.facebook.com/BCFCofficial',
    'Blackpool': 'https://www.facebook.com/officialblackpoolfc',
    'Bolton Wanderers': 'https://www.facebook.com/officialbwfc',
    'Bristol Rovers': 'https://www.facebook.com/OfficialBristolRovers',
    'Burton Albion': 'https://www.facebook.com/burtonalbionfc',
    'Cambridge United': 'https://www.facebook.com/OfficialCambridgeUnited/',
    'Charlton Athletic': 'https://www.facebook.com/OfficialCAFC/',
    'Crawley Town': 'https://www.facebook.com/crawleytown/',
    'Exeter City': 'https://www.facebook.com/exetercityfc',
    'Huddersfield Town': 'https://www.facebook.com/htafc/',
    'Leyton Orient': 'https://www.facebook.com/lofcofficial',
    'Lincoln City': 'https://www.facebook.com/lincolncityfc',
    'Mansfield Town': 'https://www.facebook.com/mansfieldtownfootballclub',
    'Northampton Town': 'https://www.facebook.com/northamptontownfc',
    'Peterborough United': 'https://www.facebook.com/theposh/',
    'Reading': 'https://www.facebook.com/ReadingFC',
    'Rotherham United': 'https://www.facebook.com/rotherhamunited',
    'Shrewsbury Town': 'https://www.facebook.com/shrewsburytown',
    'Stevenage': 'https://www.facebook.com/StevenageFCOfficial/',
    'Stockport County': 'https://www.facebook.com/stockportcounty/',
    'Wigan Athletic': 'https://www.facebook.com/wiganathletic',
    'Wrexham': 'https://www.facebook.com/wrexhamfootballclub/',
    'Wycombe Wanderers': 'https://www.facebook.com/wwfcofficial',

    'Accrington Stanley': 'https://www.facebook.com/ASFCofficial',
    'AFC Wimbledon': 'https://www.facebook.com/afcwimbledon',
    'Barrow': 'https://www.facebook.com/barrowofficial/',
    'Bradford City': 'https://www.facebook.com/officialbantams',
    'Bromley': 'https://www.facebook.com/BromleyFC/',
    'Carlisle United': 'https://www.facebook.com/carlisleunitedfc',
    'Cheltenham Town': 'https://www.facebook.com/CTFCofficial',
    'Chesterfield': 'https://www.facebook.com/ChesterfieldFC',
    'Colchester United': 'https://www.facebook.com/colchesterunitedfc',
    'Crawley Town': 'https://www.facebook.com/crawleytown',
    'Crewe Alexandra': 'https://www.facebook.com/crewealexofficial',
    'Doncaster Rovers': 'https://www.facebook.com/DoncasterRoversFC',
    'Fleetwood Town': 'https://www.facebook.com/FleetwoodTownFC',
    'Gillingham': 'https://www.facebook.com/GillinghamFootballClubOfficial',
    'Grimsby Town': 'https://www.facebook.com/officialgtfc',
    'Harrogate Town': 'https://www.facebook.com/HarrogateTownAFC/',
    'Milton Keynes Dons': 'https://www.facebook.com/mkdonsfc',
    'Morecambe': 'https://www.facebook.com/morecambefootballclub',
    'Newport County': 'https://www.facebook.com/newportcountyafc',
    'Notts County': 'https://www.facebook.com/NottsCountyFootballClub',
    'Port Vale': 'https://www.facebook.com/OfficialPVFC/',
    'Salford City': 'https://www.facebook.com/salfordcityfc',
    'Swindon Town': 'https://www.facebook.com/SwindonTownFootballClub',
    'Tranmere Rovers': 'https://www.facebook.com/trfcofficial/',
    'Walsall': 'https://www.facebook.com/OfficialWalsallFC',
    'Barnet': 'https://www.facebook.com/BarnetFootballClub',
    'Oldham Athletic': 'https://www.facebook.com/OfficialOAFC'
}

def get_s3_client():
    """Returns a Boto3 client with the specific cipher fix for Cloudflare R2."""
    context = ssl.create_default_context()
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    
    return boto3.client(
        service_name='s3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='auto',
        config=Config(signature_version='s3v4'),
        verify=False 
    )

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=1920,1080')
    driver = uc.Chrome(options=options, version_main=143)
    return driver

def parse_follower_count(text):
    if not text: return 0
    match = re.search(r'([\d.,]+[KM]?)\s*(?:followers|Followers)', text)
    if not match: return 0
    raw_val = match.group(1).upper().replace(',', '')
    try:
        if 'M' in raw_val: return int(float(raw_val.replace('M', '')) * 1_000_000)
        elif 'K' in raw_val: return int(float(raw_val.replace('K', '')) * 1_000)
        return int(float(raw_val))
    except: return 0

def run_scraper():
    driver = get_driver()
    results = []
    
    try:
        for club, url in CLUBS.items():
            print(f"🔍 Fetching {club}...")
            try:
                driver.get(url)
                time.sleep(10) # Essential for Facebook rendering
                
                full_page_text = driver.find_element(By.TAG_NAME, "body").text
                count = parse_follower_count(full_page_text)
                
                print(f" ✅ Result: {count} followers")
                results.append({
                    "Club Name": club, 
                    "Platform": "Facebook",
                    "Followers": count, 
                    "Reporting Date": REPORTING_DATE
                })
            except Exception as e:
                print(f" ❌ Error fetching {club}: {e}")

        if results:
            df = pd.DataFrame(results)
            
            # --- LOCAL SAVE PHASE ---
            # Timestamp format for local files: YYYYMMDD_HHMMSS
            local_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            local_filename = f"facebook_stats_{local_timestamp}.csv"
            
            df.to_csv(local_filename, index=False, encoding='utf-8-sig')
            print(f"💾 File saved locally: {local_filename}")
            
            # --- CLOUD UPLOAD PHASE ---
            try:
                print("☁️ Uploading to R2...")
                s3 = get_s3_client()
                
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, encoding='utf-8')
                
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=S3_KEY_PATH,
                    Body=csv_buffer.getvalue().encode('utf-8')
                )
                print(f"✅ SUCCESS: Uploaded to R2: {S3_KEY_PATH}")
                
            except Exception as upload_error:
                print(f"❌ S3 Upload failing: {upload_error}")

    finally:
        driver.quit()

if __name__ == "__main__":
    run_scraper()