import boto3
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO

# 1. Load configuration
load_dotenv()

# 2. Initialize the S3 Client for R2
s3_client = boto3.client(
    service_name='s3',
    endpoint_url=os.getenv('R2_ENDPOINT_URL'),
    aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'),
    region_name='auto'  # Cloudflare R2 handles regions automatically
)

def upload_test_data():
    bucket = os.getenv('BUCKET_NAME')
    
    # 3. Create dummy Football Success data
    data = {
        'team_name': ['Manchester City', 'Arsenal', 'Liverpool', 'Aston Villa'],
        'season': ['2023/24', '2023/24', '2023/24', '2023/24'],
        'league_pos': [1, 2, 3, 4],
        'goals_scored': [96, 91, 86, 76],
        'success_score_preview': [9.8, 9.2, 8.9, 8.1]
    }
    df = pd.DataFrame(data)
    
    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # 4. Upload to the /raw folder (The Bronze Layer)
    file_path = "raw/test_success_data.csv"
    
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=file_path,
            Body=csv_buffer.getvalue()
        )
        print(f"✅ Success! File uploaded to: {bucket}/{file_path}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

if __name__ == "__main__":
    upload_test_data()