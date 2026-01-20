import pandas as pd
import boto3
import os
import json
from io import StringIO
from dotenv import load_dotenv
from pathlib import Path
from thefuzz import fuzz, process

# Load env
script_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=script_dir.parent / '.env')

# --- THE "GOLDEN RECORD" MAPPING ---
# (I've truncated this for the example, but keep your full list here)
RAW_MAPPING = {


    "Accrington Stanley FC": ["Accrington Stanley", "Accrington Stanley", "Accrington Stanley", "The Wham Stadium Accrington Stanley" "Accrington Stanley", "Accrington", "Accrington Stanley"],
    "AFC Bournemouth": ["AFC Bournemouth", "AFC Bournemouth", "AFC Bournemouth", "Vitality Stadium AFC Bournemouth" "AFC Bournemouth", "Bournemouth", "AFC Bournemouth", "AFC BOURNEMOUTH"],
    "AFC Wimbledon": ["AFC Wimbledon", "AFC Wimbledon", "AFC Wimbledon", "Cherry Red Records Stadium AFC Wimbledon" "AFC Wimbledon", "AFC Wimbledon", "AFC Wimbledon"],
    "Arsenal FC": ["Arsenal", "Arsenal FC", "Arsenal FC", "Emirates Stadium Arsenal FC", "Arsenal", "Arsenal", "Arsenal", "Arsenal", "ARSENAL"],
    "Aston Villa FC": ["Aston Villa", "Aston Villa", "Aston Villa", "Villa Park Aston Villa" "Aston Villa", "Aston Villa", "Aston Villa", "ASTON VILLA"],
    "Barnsley FC": ["Barnsley", "Barnsley FC", "Barnsley FC", "Oakwell Stadium Barnsley FC" "Barnsley", "Barnsley", "Barnsley", "BARNSLEY"],
    "Barrow AFC": ["Barrow", "Barrow AFC", "Barrow AFC", "SO Legal Stadium Barrow AFC" "Barrow", "Barrow", "Barrow"],
    "Birmingham City FC": ["Birmingham City", "Birmingham City", "Birmingham City", "St. Andrew’s @ Knighthead Park Birmingham City" "Birmingham City", "Birmingham", "Birmingham City", "BIRMINGHAM CITY"],
    "Blackburn Rovers FC": ["Blackburn Rovers", "Blackburn Rovers", "Blackburn Rovers", "Ewood Park Blackburn Rovers" "Blackburn Rovers", "Blackburn", "Blackburn Rovers", "BLACKBURN ROVERS"],
    "Blackpool FC": ["Blackpool", "Blackpool FC", "Blackpool FC", "Bloomfield Road Blackpool FC" "Blackpool", "Blackpool", "Blackpool", "BLACKPOOL"],
    "Bolton Wanderers FC": ["Bolton Wanderers", "Bolton Wanderers", "Bolton Wanderers", "Toughsheet Community Stadium Bolton Wanderers" "Bolton Wanderers", "Bolton", "Bolton Wanderers", "BOLTON WANDERERS"],
    "Bradford City AFC": ["Bradford City", "Bradford City", "Bradford City", "University of Bradford Stadium Bradford City" "Bradford City", "Bradford", "Bradford City", "BRADFORD CITY"],
    "Brentford FC": ["Brentford", "Brentford FC", "Brentford FC", "Gtech Community Stadium Brentford FC" "Brentford", "Brentford", "Brentford", "BRENTFORD"],
    "Brighton & Hove Albion FC": ["Brighton & Hove Albion", "Brighton & Hove Albion", "Brighton & Hove Albion", "AMEX Stadium Brighton & Hove Albion" "Brighton & Hove Albion", "Brighton", "Brighton & Hove Albion", "BRIGHTON AND HOVE ALBION"],
    "Bristol City FC": ["Bristol City", "Bristol City", "Bristol City", "Ashton Gate Bristol City" "Bristol City", "Bristol City", "Bristol City", "BRISTOL CITY"],
    "Bristol Rovers FC": ["Bristol Rovers", "Bristol Rovers", "Bristol Rovers", "Memorial Stadium Bristol Rovers" "Bristol Rovers", "Bristol Rovers", "Bristol Rovers"],
    "Burnley FC": ["Burnley", "Burnley FC", "Burnley FC", "Turf Moor Burnley FC" "Burnley", "Burnley", "Burnley", "BURNLEY"],
    "Burton Albion FC": ["Burton Albion", "Burton Albion", "Burton Albion", "Pirelli Stadium Burton Albion" "Burton Albion", "Burton Albion", "Burton Albion"],
    "Cambridge United FC": ["Cambridge United", "Cambridge United", "Cambridge United", "Cledara Abbey Stadium Cambridge United" "Cambridge United", "Cambridge Utd.", "Cambridge United"],
    "Cardiff City FC": ["Cardiff City[h]", "Cardiff City", "Cardiff City", "Cardiff City Stadium Cardiff City" "Cardiff City", "Cardiff", "Cardiff City", "CARDIFF CITY"],
    "Carlisle United FC": ["Carlisle United", "Carlisle United", "Carlisle United", "Brunton Park Carlisle United" "Carlisle United", "Carlisle United", "Carlisle United", "CARLISLE UNITED"],
    "Charlton Athletic FC": ["Charlton Athletic", "Charlton Athletic", "Charlton Athletic", "The Valley Charlton Athletic" "Charlton Athletic", "Charlton", "Charlton Athletic", "CHARLTON ATHLETIC"],
    "Chelsea FC": ["Chelsea", "Chelsea FC", "Chelsea FC", "Stamford Bridge Chelsea FC", "Chelsea", "Chelsea", "Chelsea", "Chelsea", "CHELSEA"],
    "Cheltenham Town FC": ["Cheltenham Town", "Cheltenham Town", "Cheltenham Town", "Completely-Suzuki Stadium Cheltenham Town" "Cheltenham Town", "Cheltenham", "Cheltenham Town"],
    "Colchester United FC": ["Colchester United", "Colchester United", "Colchester United", "JobServe Community Stadium Colchester United" "Colchester United", "Colchester Utd.", "Colchester United"],
    "Coventry City FC": ["Coventry City", "Coventry City", "Coventry City", "Coventry Building Society Arena Coventry City" "Coventry City", "Coventry", "Coventry City", "COVENTRY CITY"],
    "Crawley Town FC": ["Crawley Town", "Crawley Town", "Crawley Town", "Broadfield Stadium Crawley Town" "Crawley Town", "Crawley Town", "Crawley Town"],
    "Crewe Alexandra FC": ["Crewe Alexandra", "Crewe Alexandra", "Crewe Alexandra", "Mornflake Stadium Crewe Alexandra" "Crewe Alexandra", "Crewe Alexandra", "Crewe Alexandra"],
    "Crystal Palace FC": ["Crystal Palace", "Crystal Palace", "Crystal Palace", "Selhurst Park Crystal Palace", "Crystal Palace", "Crystal Palace", "Crystal Palace", "Crystal Palace", "CRYSTAL PALACE"],
    "Derby County FC": ["Derby County", "Derby County", "Derby County", "Pride Park Stadium Derby County" "Derby County", "Derby", "Derby County", "DERBY COUNTY"],
    "Doncaster Rovers FC": ["Doncaster Rovers", "Doncaster Rovers", "Doncaster Rovers", "Eco-Power Stadium Doncaster Rovers" "Doncaster Rovers", "Doncaster", "Doncaster Rovers"],
    "Everton FC": ["Everton", "Everton FC", "Everton FC", "Hill Dickinson Stadium Everton FC" "Everton", "Everton", "Everton", "EVERTON"],
    "Exeter City FC": ["Exeter City", "Exeter City", "Exeter City", "St James Park Exeter City" "Exeter City", "Exeter City", "Exeter City"],
    "Fleetwood Town FC": ["Fleetwood Town", "Fleetwood Town", "Fleetwood Town", "Highbury Stadium Fleetwood Town" "Fleetwood Town", "Fleetwood", "Fleetwood Town"],
    "Forest Green Rovers FC": ["Forest Green Rovers"   "Forest Green" ],
    "Fulham FC": ["Fulham", "Fulham FC", "Fulham FC", "Craven Cottage Fulham FC" "Fulham", "Fulham", "Fulham", "FULHAM"],
    "Gillingham FC": ["Gillingham", "Gillingham FC", "Gillingham FC", "Priestfield Stadium Gillingham FC" "Gillingham", "Gillingham", "Gillingham"],
    "Grimsby Town FC": ["Grimsby Town", "Grimsby Town", "Grimsby Town", "Blundell Park Grimsby Town" "Grimsby Town", "Grimsby Town", "Grimsby Town", "GRIMSBY TOWN"],
    "Harrogate Town AFC": ["Harrogate Town", "Harrogate Town", "Harrogate Town", "The Exercise Stadium Harrogate Town" "Harrogate Town", "Harrogate Town", "Harrogate Town"],
    "Huddersfield Town AFC": ["Huddersfield Town", "Huddersfield Town", "Huddersfield Town", "Accu Stadium Huddersfield Town" "Huddersfield Town", "Huddersfield", "Huddersfield Town", "HUDDERSFIELD TOWN"],
    "Hull City AFC": ["Hull City", "Hull City", "Hull City", "MKM Stadium Hull City" "Hull City", "Hull City", "Hull City", "HULL CITY"],
    "Ipswich Town FC": ["Ipswich Town", "Ipswich Town", "Ipswich Town", "Portman Road Ipswich Town" "Ipswich Town", "Ipswich", "Ipswich Town", "IPSWICH TOWN"],
    "Leeds United FC": ["Leeds United", "Leeds United", "Leeds United", "Elland Road Leeds United" "Leeds United", "Leeds", "Leeds United", "LEEDS UNITED"],
    "Leicester City FC": ["Leicester City", "Leicester City", "Leicester City", "King Power Stadium Leicester City", "Leicester City", "Leicester City", "Leicester", "Leicester City", "LEICESTER CITY"],
    "Leyton Orient FC": ["Leyton Orient", "Leyton Orient", "Leyton Orient", "BetWright Stadium Leyton Orient" "Leyton Orient", "Leyton Orient", "Leyton Orient", "LEYTON ORIENT"],
    "Lincoln City FC": ["Lincoln City", "Lincoln City", "Lincoln City", "LNER Stadium Lincoln City" "Lincoln City", "Lincoln City", "Lincoln City"],
    "Liverpool FC": ["Liverpool", "Liverpool FC", "Liverpool FC", "Anfield Liverpool FC", "Liverpool", "Liverpool", "Liverpool", "Liverpool", "LIVERPOOL"],
    "Luton Town FC": ["Luton Town", "Luton Town", "Luton Town", "Kenilworth Road Luton Town" "Luton Town", "Luton", "Luton Town", "LUTON TOWN"],
    "Manchester City FC": ["Manchester City", "Manchester City", "Manchester City", "Etihad Stadium Manchester City", "Manchester City", "Manchester City", "Man City", "Manchester City", "MANCHESTER CITY"],
    "Manchester United FC": ["Manchester United", "Manchester United", "Manchester United", "Old Trafford Manchester United", "Manchester United", "Manchester United", "Man Utd", "Manchester United", "MANCHESTER UNITED"],
    "Mansfield Town FC": ["Mansfield Town", "Mansfield Town", "Mansfield Town", "One Call Stadium Mansfield Town" "Mansfield Town", "Mansfield Town", "Mansfield Town"],
    "Middlesbrough FC": ["Middlesbrough", "Middlesbrough FC", "Middlesbrough FC", "Riverside Stadium Middlesbrough FC" "Middlesbrough", "Middlesbrough", "Middlesbrough", "MIDDLESBROUGH"],
    "Millwall FC": ["Millwall", "Millwall FC", "Millwall FC", "The Den Millwall FC" "Millwall", "Millwall", "Millwall", "MILLWALL"],
    "Milton Keynes Dons FC": ["Milton Keynes Dons", "Milton Keynes Dons", "Milton Keynes Dons", "Stadium mk Milton Keynes Dons" "Milton Keynes Dons", "MK Dons", "Milton Keynes Dons"],
    "Morecambe FC": ["Morecambe", "Morecambe FC", "Morecambe FC", "Mazuma Stadium Morecambe FC" "Morecambe", "Morecambe FC", "Morecambe"],
    "Newcastle United FC": ["Newcastle United", "Newcastle United", "Newcastle United", "St James' Park Newcastle United", "Newcastle United", "Newcastle United", "Newcastle", "Newcastle United", "NEWCASTLE UNITED"],
    "Newport County AFC": ["Newport County", "Newport County", "Newport County", "Rodney Parade Newport County" "Newport County", "Newport County", "Newport County"],
    "Northampton Town FC": ["Northampton Town", "Northampton Town", "Northampton Town", "Sixfields Stadium Northampton Town" "Northampton Town", "Northampton", "Northampton Town", "NORTHAMPTON TOWN"],
    "Norwich City FC": ["Norwich City", "Norwich City", "Norwich City", "Carrow Road Norwich City" "Norwich City", "Norwich", "Norwich City", "NORWICH CITY"],
    "Nottingham Forest FC": ["Nottingham Forest", "Nottingham Forest", "Nottingham Forest", "The City Ground Nottingham Forest" "Nottingham Forest", "Forest", "Nottingham Forest", "NOTTINGHAM FOREST"],
    "Notts County FC": ["Notts County", "Notts County", "Notts County", "Meadow Lane Notts County" "Notts County", "Notts County", "Notts County", "NOTTS COUNTY"],
    "Oxford United FC": ["Oxford United", "Oxford United", "Oxford United", "Kassam Stadium Oxford United" "Oxford United", "Oxford United", "Oxford United", "OXFORD UNITED"],
    "Peterborough United FC": ["Peterborough United", "Peterborough United", "Peterborough United", "Weston Homes Stadium Peterborough United" "Peterborough United", "Peterborough", "Peterborough United"],
    "Plymouth Argyle FC": ["Plymouth Argyle", "Plymouth Argyle", "Plymouth Argyle", "Home Park Plymouth Argyle" "Plymouth Argyle", "Plymouth", "Plymouth Argyle"],
    "Port Vale FC": ["Port Vale", "Port Vale FC", "Port Vale FC", "Vale Park Port Vale FC" "Port Vale", "Port Vale", "Port Vale"],
    "Portsmouth FC": ["Portsmouth", "Portsmouth FC", "Portsmouth FC", "Fratton Park Portsmouth FC" "Portsmouth", "Portsmouth", "Portsmouth", "PORTSMOUTH"],
    "Preston North End FC": ["Preston North End", "Preston North End", "Preston North End", "Deepdale Preston North End" "Preston North End", "Preston", "Preston North End", "PRESTON NORTH END"],
    "Queens Park Rangers FC": ["Queens Park Rangers", "Queens Park Rangers", "Queens Park Rangers", "Loftus Road Stadium Queens Park Rangers" "Queens Park Rangers", "QPR", "Queens Park Rangers", "QUEENS PARK RANGERS"],
    "Reading FC": ["Reading", "Reading FC", "Reading FC", "Select Car Leasing Stadium Reading FC" "Reading", "Reading", "Reading", "READING"],
    "Rotherham United FC": ["Rotherham United", "Rotherham United", "Rotherham United", "New York Stadium Rotherham United" "Rotherham United", "Rotherham", "Rotherham United"],
    "Salford City FC": ["Salford City", "Salford City", "Salford City", "The Peninsula Stadium Salford City" "Salford City", "Salford", "Salford City"],
    "Sheffield United FC": ["Sheffield United", "Sheffield United", "Sheffield United", "Bramall Lane Sheffield United" "Sheffield United", "Sheff Utd", "Sheffield United", "SHEFFIELD UNITED"],
    "Sheffield Wednesday FC": ["Sheffield Wednesday", "Sheffield Wednesday", "Sheffield Wednesday", "Hillsborough Sheffield Wednesday" "Sheffield Wednesday", "Sheff Wed", "Sheffield Wednesday", "SHEFFIELD WEDNESDAY"],
    "Shrewsbury Town FC": ["Shrewsbury Town", "Shrewsbury Town", "Shrewsbury Town", "Montgomery Waters Meadow Shrewsbury Town" "Shrewsbury Town", "Shrewsbury", "Shrewsbury Town"],
    "Southampton FC": ["Southampton", "Southampton FC", "Southampton FC", "St Mary's Stadium Southampton FC" "Southampton", "Southampton", "Southampton", "SOUTHAMPTON"],
    "Stevenage FC": ["Stevenage", "Stevenage FC", "Stevenage FC", "The Lamex Stadium Stevenage FC" "Stevenage", "Stevenage", "Stevenage"],
    "Stockport County FC": ["Stockport County", "Stockport County", "Stockport County", "Edgeley Park Stockport County" "Stockport County", "Stockport", "Stockport County"],
    "Stoke City FC": ["Stoke City", "Stoke City", "Stoke City", "bet365 Stadium Stoke City" "Stoke City", "Stoke City", "Stoke City", "STOKE CITY"],
    "Sunderland AFC": ["Sunderland", "Sunderland AFC", "Sunderland AFC", "Stadium of Light Sunderland AFC" "Sunderland", "Sunderland", "Sunderland", "SUNDERLAND"],
    "Sutton United FC": ["Sutton United"   "Sutton Utd" ],
    "Swansea City AFC": ["Swansea City[i]", "Swansea City", "Swansea City", "Swansea.com Stadium Swansea City" "Swansea City", "Swansea", "Swansea City", "SWANSEA CITY"],
    "Swindon Town FC": ["Swindon Town", "Swindon Town", "Swindon Town", "County Ground Swindon Town" "Swindon Town", "Swindon Town", "Swindon Town", "SWINDON TOWN"],
    "Tottenham Hotspur FC": ["Tottenham Hotspur", "Tottenham Hotspur", "Tottenham Hotspur", "Tottenham Hotspur Stadium Tottenham Hotspur", "Tottenham Hotspur", "Tottenham Hotspur", "Tottenham", "Tottenham Hotspur", "TOTTENHAM HOTSPUR"],
    "Tranmere Rovers FC": ["Tranmere Rovers", "Tranmere Rovers", "Tranmere Rovers", "Prenton Park Tranmere Rovers" "Tranmere Rovers", "Tranmere", "Tranmere Rovers"],
    "Walsall FC": ["Walsall", "Walsall FC", "Walsall FC", "Banks's Stadium Walsall FC" "Walsall", "Walsall", "Walsall"],
    "Watford FC": ["Watford", "Watford FC", "Watford FC", "Vicarage Road Watford FC" "Watford", "Watford", "Watford", "WATFORD"],
    "West Bromwich Albion FC": ["West Bromwich Albion", "West Bromwich Albion", "West Bromwich Albion", "The Hawthorns West Bromwich Albion" "West Bromwich Albion", "West Brom", "West Bromwich Albion", "WEST BROMWICH ALBION"],
    "West Ham United FC": ["West Ham United", "West Ham United", "West Ham United", "London Stadium West Ham United" "West Ham United", "West Ham", "West Ham United", "WEST HAM UNITED"],
    "Wigan Athletic FC": ["Wigan Athletic", "Wigan Athletic", "Wigan Athletic", "Brick Community Stadium Wigan Athletic" "Wigan Athletic", "Wigan", "Wigan Athletic", "WIGAN ATHLETIC"],
    "Wolverhampton Wanderers FC": ["Wolverhampton Wanderers", "Wolverhampton Wanderers", "Wolverhampton Wanderers", "Molineux Stadium Wolverhampton Wanderers" "Wolverhampton Wanderers", "Wolves", "Wolverhampton Wanderers", "WOLVERHAMPTON WANDERERS"],
    "Wycombe Wanderers FC": ["Wycombe Wanderers", "Wycombe Wanderers", "Wycombe Wanderers", "Adams Park Wycombe Wanderers" "Wycombe Wanderers", "Wycombe", "Wycombe Wanderers"],
    "Wrexham AFC": ["Wrexham AFC", "Wrexham AFC", "Wrexham AFC", "STōK Cae Ras Wrexham AFC" "Wrexham", "Wrexham", "Wrexham"],
    "Chesterfield FC": ["Chesterfield", "Chesterfield FC", "Chesterfield FC", "SMH Group Stadium Chesterfield FC" "Chesterfield", "Chesterfield", "Chesterfield"],
    "Bromley FC": ["Bromley", "Bromley FC", "Bromley FC", "H2T Group Stadium Bromley FC" "Bromley", "Bromley", "Bromley"],
    "Barnet FC": ["Barnet", "Barnet FC" "The Hive Stadium Barnet FC" "Barnet", "Barnet", "Barnet"],
    "Oldham Athletic AFC": ["Oldham Athletic", "Oldham Athletic" "Boundary Park Oldham Athletic" "Oldham Athletic", "Oldham Athletic", "Oldham Athletic", "OLDHAM ATHLETIC"],



}

def get_flattened_map():
    flat_map = {}
    for standard_name, aliases in RAW_MAPPING.items():
        for alias in aliases:
            flat_map[alias] = standard_name
    return flat_map

TEAM_NAME_MAP = get_flattened_map()

def transform_to_silver():
    s3 = boto3.client(service_name='s3', endpoint_url=os.getenv('R2_ENDPOINT_URL'), 
                      aws_access_key_id=os.getenv('R2_ACCESS_KEY_ID'), 
                      aws_secret_access_key=os.getenv('R2_SECRET_ACCESS_KEY'), region_name='auto')
    bucket = os.getenv('BUCKET_NAME')
    
    print("🥈 Starting Fuzzy Silver Transformation...")

    # 1. Load Data
    response = s3.get_object(Bucket=bucket, Key="raw/bbc_scraped_teams_all.json")
    raw_data = json.loads(response['Body'].read().decode('utf-8'))
    
    rows = []
    discrepancies = []

    # 2. Process with Fuzzy Matching
    # We compare every raw name against our "Standard Names" list
    standard_names_list = list(RAW_MAPPING.keys())

    for league, teams in raw_data.items():
        for t in teams:
            # Step A: Check for exact alias match first (Fastest)
            standard_name = TEAM_NAME_MAP.get(t)
            match_score = 100
            
            # Step B: If no exact match, try Fuzzy Logic
            if not standard_name:
                # Find the closest match in our "Golden" list
                best_match, match_score = process.extractOne(t, standard_names_list, scorer=fuzz.token_sort_ratio)
                
                if match_score > 80: # If it's a "Confident" match
                    standard_name = best_match
                else:
                    standard_name = "UNKNOWN"
            
            # Step C: Log Discrepancies for Review
            if match_score < 100:
                discrepancies.append({
                    "raw_input": t,
                    "guessed_name": standard_name,
                    "confidence_score": match_score,
                    "league": league
                })

            rows.append({
                "raw_name": t,
                "standard_name": standard_name,
                "league": league,
                "match_confidence": match_score
            })

    df_final = pd.DataFrame(rows)
    df_review = pd.DataFrame(discrepancies)

    # 3. Upload Results
    csv_buffer = StringIO()
    df_final.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key="silver/all_english_teams_master.csv", Body=csv_buffer.getvalue())

    # 4. Upload Review Log
    if not df_review.empty:
        review_buffer = StringIO()
        df_review.to_csv(review_buffer, index=False)
        s3.put_object(Bucket=bucket, Key="silver/review_needed_names.csv", Body=review_buffer.getvalue())
        print(f"⚠️ {len(df_review)} names flagged for review in silver/review_needed_names.csv")

    print(f"✅ Master file updated with {len(df_final)} teams.")

if __name__ == "__main__":
    transform_to_silver()