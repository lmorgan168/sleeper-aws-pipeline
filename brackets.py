import boto3
import json
import time
import requests
import logging

S3_BUCKET = "sleeper-fantasy-data"
SLEEPER_API_BASE = "http://api.sleeper.app/v1"
FOLDER = "leagues/redraft"
PROCESSED_FOLDER = "processed"
REQUEST_LIMIT = 1000  # Max requests per minute
REQUEST_WINDOW = 60  # 60 seconds
BATCH_SIZE = 25
api_requests = 0
window_start_time = time.time()  # Track the start of the window

s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_rate_limits():
    global api_requests, window_start_time

    now = time.time()
    elapsed = now - window_start_time

    if elapsed >= REQUEST_WINDOW:
        api_requests = 0
        window_start_time = now

    if api_requests >= REQUEST_LIMIT:
        sleep_time = REQUEST_WINDOW - elapsed
        logger.warning(f"API rate limit reached! Sleeping for {sleep_time: .2f} seconds...")
        time.sleep(sleep_time)

        api_requests = 0
        window_start_time = time.time()

    api_requests += 1

def list_json_files():
    """List JSON files in S3 that contain redraft leagues"""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=FOLDER)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".json")]

def is_file_processed(file_key):
    """Check if a .done file exists for a given JSON file"""
    done_key = f"{PROCESSED_FOLDER}/{file_key}.done"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=done_key)
        return True
    except s3.exceptions.ClientError:
        return False

def fetch_brackets(league_id):
    """Fetch brackets data from Sleeper API"""
    url = f"{SLEEPER_API_BASE}/league/{league_id}/brackets"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch brackets for {league_id}, Status Code: {response.status_code}")
        return None

def process_file(file_key):
    """Process a single JSON file, fetch bracket data, and store results"""
    logger.info(f"Processing file: {file_key}")
    
    # Read JSON file from S3
    obj = s3.get_object(Bucket=S3_BUCKET, Key=file_key)
    leagues = json.loads(obj['Body'].read().decode('utf-8'))
    
    brackets_data = []
    
    for league in leagues:
        league_id = league["league_id"]
        bracket_data = fetch_brackets(league_id)
        
        if bracket_data:
            brackets_data.append({"league_id": league_id, "bracket": bracket_data})

        if len(brackets_data) >= BATCH_SIZE:
            batch_write_to_s3(brackets_data, file_key)
            brackets_data = []  # Reset batch

    # Final write for remaining data
    if brackets_data:
        batch_write_to_s3(brackets_data, file_key)

    # Mark file as processed
    mark_file_processed(file_key)
    logger.info(f"Completed processing: {file_key}")

def batch_write_to_s3(data, file_key):
    """Write batch of bracket data to S3"""
    timestamp = int(time.time())
    output_key = f"brackets/brackets_{timestamp}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )
    logger.info(f"Stored bracket data in {output_key}")

def mark_file_processed(file_key):
    """Mark file as processed by writing an empty .done file"""
    done_key = f"{PROCESSED_FOLDER}/{file_key}.done"
    s3.put_object(Bucket=S3_BUCKET, Key=done_key, Body="")

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    files = list_json_files()

    for file_key in files:
        if not is_file_processed(file_key):
            process_file(file_key)