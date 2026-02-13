import pandas as pd
import boto3
import os
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from google.cloud import bigquery # New requirement

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
    
    # BigQuery Config
    bq_project = os.getenv('GBQ_PROJECT_ID')
    bq_table_id = f"{bq_project}.{os.getenv('GBQ_DATASET')}.{os.getenv('GBQ_TABLE')}"
    
    raw_key_path = os.getenv('GBQ_KEY_PATH')
    clean_key_path = str(Path(raw_key_path).resolve()) 
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = clean_key_path

    print("🥇 Generating Gold Presentation Layer...")

    try:
        # 1. Load Silver Data from R2
        response = s3.get_object(Bucket=bucket, Key="silver/all_english_teams_master.csv")
        gold_df = pd.read_csv(response['Body'], encoding='utf-8') 
        
        # 2. Re-format for Gold (Updated_At removed, Season_Year kept for BQ compatibility)
        final_output = pd.DataFrame({
            "ClubName": gold_df['standard_name'],
            "League": gold_df['league'],
            "Season Year": 2025, 
            "Positions": gold_df.groupby('league').cumcount() + 1
        })

        # 3. Timestamp & Filename
        timestamp = datetime.now().strftime("%d%m%Y_%H%M")
        filename = f"sample-league-ranking_{timestamp}.csv"
        
        # --- PHASE 1: LOCAL SAVE (Restored) ---
        local_dir = script_dir / "gold_backups"
        local_dir.mkdir(exist_ok=True)
        final_output.to_csv(local_dir / filename, index=False, encoding='utf-8')
        print(f"💾 Saved Locally: {filename}")

        # --- PHASE 2: CLOUDFLARE R2 UPLOAD (Restored) ---
        csv_buffer = StringIO()
        final_output.to_csv(csv_buffer, index=False, encoding='utf-8') 
        s3.put_object(
            Bucket=bucket, 
            Key=f"gold/{filename}", 
            Body=csv_buffer.getvalue().encode('utf-8')
        )
        print(f"✅ Uploaded to R2: gold/{filename}")

        # --- PHASE 3: BIGQUERY UPDATE ---
        print("📊 Updating BigQuery schema and data...")
        bq_client = bigquery.Client()
        
        # Using WRITE_TRUNCATE to ensure the removed 'Updated_At' column 
        # doesn't cause a schema mismatch error.
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        bq_job = bq_client.load_table_from_dataframe(final_output, bq_table_id, job_config=job_config)
        bq_job.result() 
        print(f"🚀 Successfully updated BigQuery table: {bq_table_id}")

    except Exception as e:
        print(f"❌ Gold Generation failed: {e}")

if __name__ == "__main__":
    generate_gold_csv()