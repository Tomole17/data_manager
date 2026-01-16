import pandas as pd
import boto3
import os
import json
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# --- STEP 1: YOUR CUSTOM MAPPING FROM THE IMAGE ---
TEAM_NAME_MAP = {
    "Arsenal FC": "Arsenal",
    "Aston Villa FC": "Aston Villa",
    "Bournemouth": "Bournemouth",
    "Brentford FC": "Brentford",
    "Brighton & Hove Albion": "Brighton",
    "Chelsea FC": "Chelsea",
    "Crystal Palace FC": "Crystal Palace",
    "Everton FC": "Everton",
    "Fulham FC": "Fulham",
    "Ipswich Town": "Ipswich",
    "Leicester City": "Leicester",
    "Liverpool FC": "Liverpool",
    "Manchester City FC": "Manchester City",
    "Manchester United FC": "Manchester United",
    "Newcastle United": "Newcastle",
    "Nottingham Forest": "Nottingham Forest",
    "Southampton FC": "Southampton",
    "Tottenham Hotspur": "Tottenham",
    "West Ham United": "West Ham",
    "Wolverhampton Wanderers": "Wolves"
}

def transform_to_silver():
    s3 = boto3.client(
        service_name='s3',
        endpoint_url=os.getenv('R2_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
        region_name='auto'
    )
    bucket = os.getenv('BUCKET_NAME')

    try:
        # 1. DOWNLOAD FROM BRONZE (RAW)
        print("📂 Fetching raw data from Bronze...")
        # Note: We use the filename from your ingestion script
        response = s3.get_object(Bucket=bucket, Key="raw/bbc_scraped_teams.json")
        raw_data = json.loads(response['Body'].read().decode('utf-8'))
        
        # 2. CONVERT TO DATAFRAME
        # Assuming the JSON looks like {'scraped_teams': ['Man City', ...]}
        df = pd.DataFrame(raw_data['scraped_teams'], columns=['raw_name'])

        # 3. APPLY MAPPING (DATA GOVERNANCE)
        print("✨ Applying team name mapping...")
        # .map() replaces names found in the dict; .fillna() keeps the original if not found
        df['standard_name'] = df['raw_name'].map(TEAM_NAME_MAP).fillna(df['raw_name'])

        # 4. UPLOAD TO SILVER
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3.put_object(
            Bucket=bucket,
            Key="silver/standardized_teams.csv",
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Success! Standardized data saved to: silver/standardized_teams.csv")

    except Exception as e:
        print(f"❌ Transformation failed: {e}")

if __name__ == "__main__":
    transform_to_silver()