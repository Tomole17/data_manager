import pandas as pd
import boto3
import os
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Load env
script_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=script_dir.parent / '.env')

def get_s3_client():
    return boto3.client(
        service_name='s3',
        endpoint_url=os.getenv('R2_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
        region_name='auto'
    )

def generate_gold_csv():
    s3 = get_s3_client()
    bucket = os.getenv('BUCKET_NAME')
    
    print("🥇 Generating Gold Presentation Layer from Silver...")

    try:
            # 1. Load Silver Data
            response = s3.get_object(Bucket=bucket, Key="silver/all_english_teams_master.csv")
            # Explicitly tell pandas to read the S3 body as utf-8
            gold_df = pd.read_csv(response['Body'], encoding='utf-8') 
            
            # 2. Re-format for Gold (Ensuring 4 distinct columns)
            final_output = pd.DataFrame({
                "ClubName": gold_df['standard_name'],
                "League": gold_df['league'],
                "Season Year": 2025,
                "Positions": gold_df.groupby('league').cumcount() + 1
            })

            # 3. NEW TIMESTAMP FORMAT: DDMMYYYY_hhmm
            timestamp = datetime.now().strftime("%d%m%Y_%H%M")
            filename = f"sample-league-ranking_{timestamp}.csv"
            
            # 4. Upload as true CSV
            csv_buffer = StringIO()
            # Ensure the buffer writes the emojis correctly
            final_output.to_csv(csv_buffer, index=False, encoding='utf-8') 
            
            s3.put_object(
                Bucket=bucket, 
                Key=f"gold/{filename}", 
                # Convert the string content into utf-8 bytes for S3 storage
                Body=csv_buffer.getvalue().encode('utf-8')
            )
            
            print(f"✅ Gold file created: gold/{filename}")

    except Exception as e:
        print(f"❌ Gold Generation failed: {e}")

if __name__ == "__main__":
    generate_gold_csv()