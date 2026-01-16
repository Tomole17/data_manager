import os
from dotenv import load_dotenv
from pathlib import Path

# 1. This finds the folder where THIS script lives (e.g., /scripts)
script_dir = Path(__file__).resolve().parent

# 2. This points to the parent folder where your .env should be
env_path = script_dir.parent / '.env'

print(f"🔍 Diagnostic: Looking for .env at: {env_path}")
print(f"❓ Does file exist? {env_path.exists()}")

# 3. Load that specific file
load_dotenv(dotenv_path=env_path)

key = os.getenv('FOOTBALL_DATA_API_KEY')

if key is None:
    print("❌ ERROR: Still can't find FOOTBALL_DATA_API_KEY. Check the spelling in your .env file.")
else:
    print(f"✅ Key Found! Starts with: {key[:4]}...")
    
    # Now try the actual API call
    import requests
    url = "https://api.football-data.org/v4/competitions/PL/standings"
    headers = {'X-Auth-Token': key}
    
    response = requests.get(url, headers=headers)
    print(f"📡 API Status: {response.status_code}")
    if response.status_code == 200:
        print("🎉 Success! Your API key is working and has access to the Premier League.")
    elif response.status_code == 403:
        print("🚫 403 Forbidden: Your key is valid, but Football-Data.org is blocking this specific request.")