import sys
sys.path.append('/opt')

import boto3
import requests
import time
import json
import resource
import logging
import uuid

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("processed_items")
table2 = dynamodb.Table("sleeper_api_requests")

s3 = boto3.client("s3")

S3_BUCKET = "sleeper-fantasy-data"
SLEEPER_API_BASE = "http://api.sleeper.app/v1"

DEFAULT_USER_ID = "738190321317507072"
DEFAULT_YEAR = "2024"
BATCH_SIZE = 25

REQUEST_LIMIT = 1000
REQUEST_WINDOW = 60
api_requests = 0
last_request_time = time.time()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def check_api_limit():
    now = int(time.time())

    response = table2.scan(
        FilterExpression="expire_at >= :now",
        ExpressionAttributeValues={":now": now}
    )
    request_count = len(response.get("Items", []))

    return request_count

def log_api_request():
    now = int(time.time())
    expire_at = now + 60

    table2.put_item(
        Item={
            "request_id": str(uuid.uuid4()),
            "timestamp": now,
            "expire_at": expire_at
        }
    )

def log_memory_usage(label):
    """Logs current and peak memory usage in MB"""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    memory_used = usage.ru_maxrss / 1024  # Convert KB to MB
    logger.info(f"[MEMORY] {label} - Current: {memory_used:.2f} MB")


def get_sleeper_data(url, max_retries=5, backoff_factor=2):
    retries = 0
    # If 1000 requests have been made within a minute, sleep
    while retries <= max_retries:
        if check_api_limit() >= REQUEST_LIMIT:
            if retries == max_retries:  
                logger.error(f"🚨 API limit reached! Skipping request to {url} after {max_retries} retries.")
                return None 
            sleep_time = backoff_factor ** retries
            logger.warning(f"API limit reached! Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            retries +=1
            continue
    
        log_api_request()

        start_time = time.time()
        response = requests.get(url)
        elapsed_time = time.time() - start_time
    

        if response.status_code == 200:
            logger.info(f"Successful request to {url} - Took {elapsed_time:.2f} seconds")
            return response.json()

        logger.error(f"Failed request to {url} - Status Code: {response.status_code}")

        if response.status_code in [522, 500, 503]:
            wait_time = backoff_factor ** retries  # Exponential backoff (1s, 2s, 4s)
            logger.warning(f"Retrying request to {url} in {wait_time} seconds... (Attempt {retries+1}/{max_retries})")
            time.sleep(wait_time)
            retries += 1
        else:
            break

    logger.error(f"Request to {url} failed after {max_retries} retries. Skipping.")        
    return None

def is_processed(item_id, item_type):
    response = table.get_item(Key={"id": item_id, "type": item_type})
    return "Item" in response

def mark_as_processed(item_id, item_type, max_retries = 3):
    retries = 0
    while retries < max_retries:
        response = table.delete_item(Key={"id": item_id, "type": "users_to_process"})

        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            break
            
        logger.warning(f"Failed to delete {item_id} from users_to_process. Retrying...")
        retries +=1
        time.sleep(2 ** retries)

    if retries == max_retries:
        logger.error(f" Failed to delete {item_id} after {max_retries} retries. Skipping insert.")
        return

    table.put_item(Item={"id": item_id, "type": item_type, "timestamp": int(time.time())})
    if item_type == "user":
        logger.info(f"Successfully moved {item_id} from users_to_process to {item_type}.")
    else:
        logger.info(f"Successfully marked {item_id} as processed {item_type}")

def batch_write_items(table_name, items, max_retries = 3):
    if not items:
        return

    unique_items = {item["id"]["S"]: item for item in items}.values()
    dynamodb_client = boto3.client("dynamodb")

    MAX_BATCH_SIZE = 25
    for i in range(0, len(items), MAX_BATCH_SIZE):
        batch = list(unique_items)[i:i+MAX_BATCH_SIZE]
        request_items = {
            table_name: [{"PutRequest": {"Item": item}} for item in batch]
        }
    
        retries = 0
        while retries < max_retries:
            response = dynamodb_client.batch_write_item(RequestItems=request_items)
            
            #  Check for UnprocessedItems
            unprocessed_items = response.get("UnprocessedItems", {}).get(table_name, [])
            if not unprocessed_items:
                break  #  Success, exit retry loop

            #  Retry only unprocessed items
            request_items = {table_name: [{"PutRequest": {"Item": item}} for item in unprocessed_items]}
            retries += 1
            time.sleep(2 ** retries)  #  Exponential backoff

        if retries == max_retries:
            print(f"Warning: Some items in {table_name} were not written after {max_retries} retries.")


def batch_write_to_s3(data, folder, file_prefix):
    if not data:
        return
    
    timestamp = int(time.time())
    file_key = f"{DEFAULT_YEAR}/{folder}/{file_prefix}_{timestamp}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=file_key, 
        Body=json.dumps(data),
        ContentType="application/json"
    )

    logger.info(f"Batch written to S3: {file_key}")


def lambda_handler(event, context):
    log_memory_usage("Start of Lambda Execution")

    year = event.get("year", DEFAULT_YEAR)
    user_id = event.get("user_id", DEFAULT_USER_ID)

    log_memory_usage("Before DynamoDB table scan")

    ## Need to create globalsecondaryindex for the dynamodb table first
    response = table.query(
        IndexName="type-index",
        KeyConditionExpression="#t = :type",
        ExpressionAttributeNames={"#t": "type"},
        ExpressionAttributeValues={":type": "users_to_process"},
        Limit=50
    )
    users_to_process = [item["id"] for item in response.get("Items", [])]
    
    log_memory_usage("After DynamoDB table scan")

    if not users_to_process:
        logger.info("No users_to_process found. Starting with default user.")
        users_to_process.append(user_id)

    logger.info(f"Users to process: {users_to_process}")

    new_users = []
    user_data = []
    draft_picks = []
    best_ball_leagues = []
    idp_leagues = []
    large_league_leagues = []
    keeper_leagues = []
    dynasty_leagues = []
    redraft_leagues = []


    
    for user_id in users_to_process:
        if is_processed(user_id, "user"):
            continue

        log_memory_usage("Before Fetching Leagues")

        leagues = get_sleeper_data(f"{SLEEPER_API_BASE}/user/{user_id}/leagues/nfl/{year}")
        log_memory_usage("After Fetching Leagues")

        if not leagues:
            logger.warning(f"No leagues found for user {user_id}. Skipping.")
            continue
        logger.info(f"Fetched {len(leagues)} leagues for user {user_id}")

        mark_as_processed(user_id, "user")
        
        log_memory_usage("Before Processing Leagues")

        for league in leagues:
            league_id = league["league_id"]
            if is_processed(league_id, "league"):
                continue

            mark_as_processed(league_id, "league")

            league_users = get_sleeper_data(f"{SLEEPER_API_BASE}/league/{league_id}/users")
            if league_users:
                for user in league_users:
                    if is_processed(user["user_id"], "user") == False:
                        new_users.append({
                            "id": {"S": user["user_id"]},
                            "type": {"S": "users_to_process"},
                            "timestamp": {"N": str(int(time.time()))}
                        })
                        user_data.append(user)
                    if len(new_users) >= BATCH_SIZE:
                        batch_write_items("processed_items", new_users)
                        new_users = []

                    if len(user_data) >= BATCH_SIZE:
                        batch_write_to_s3(user_data, "users", "user_data")
                        user_data = []

            draft_id = league.get("draft_id")

            if league.get("status") == "complete" and draft_id and league.get("total_rosters") >= 8:
                draft_results = get_sleeper_data(f"{SLEEPER_API_BASE}/draft/{draft_id}/picks")
                if not draft_results:
                    continue # Skip league if the draft picks are missing
                draft_picks.append(draft_results) ## draft_results are nested but have draft_id in them, so I just extend
                if len(draft_picks) >= BATCH_SIZE:
                    batch_write_to_s3(draft_picks, "drafts", "draft_picks")
                    draft_picks = []

                if league["settings"].get("best_ball") == 1:
                    best_ball_leagues.append(league)
                    if len(best_ball_leagues) >= BATCH_SIZE:
                        batch_write_to_s3(best_ball_leagues, "leagues/best_ball", "best_ball_leagues")
                        best_ball_leagues = []
                    continue
                if any(pos in league["roster_positions"] for pos in ["DL", "LB", "DB"]):
                    idp_leagues.append(league)
                    if len(idp_leagues) >= BATCH_SIZE:
                        batch_write_to_s3(idp_leagues, "leagues/idp", "idp_leagues")
                        idp_leagues = []
                    continue

                if league["settings"].get("type", 0) == 1:
                    keeper_leagues.append(league)
                    if len(keeper_leagues) >= BATCH_SIZE:
                        batch_write_to_s3(keeper_leagues, "leagues/keeper", "keeper_leagues")
                        keeper_leagues = []
                    continue

                if league["settings"].get("type", 0) == 2:
                    dynasty_leagues.append(league)
                    if len(dynasty_leagues) >= BATCH_SIZE:
                        batch_write_to_s3(dynasty_leagues, "leagues/dynasty", "dynasty_leagues")
                        dynasty_leagues = []
                    continue

                if league["settings"].get("num_teams",0) > 16:
                    large_league_leagues.append(league)
                    if len(large_league_leagues) >= BATCH_SIZE:
                        batch_write_to_s3(large_league_leagues, "leagues/large", "large_league_leagues")
                        large_league_leagues = []
                    continue
                # Guranteed redraft league

                redraft_leagues.append(league)
                if len(redraft_leagues) >= BATCH_SIZE:
                        batch_write_to_s3(redraft_leagues, "leagues/redraft", "redraft_leagues")
                        redraft_leagues = []
        log_memory_usage("After Processing Leagues")
        logger.info(f"Processed leaguees. ")

    log_memory_usage("Before Writing to DynamoDB")
    if new_users:
        logger.info("Writing new users to DynamoDB...")
        batch_write_items("processed_items", new_users)
        logger.info("Successfully wrote new users to DynamoDB!")

    log_memory_usage("After Writing to DynamoDB")

    log_memory_usage("Before Writing to S3")
    if user_data:
        batch_write_to_s3(user_data, "users", "user_data")
    
    if draft_picks:
        batch_write_to_s3(draft_picks, "drafts", "draft_picks")
    
    if best_ball_leagues:
        batch_write_to_s3(best_ball_leagues, "leagues/best_ball", "best_ball_leagues")

    if idp_leagues:
        batch_write_to_s3(idp_leagues, "leagues/idp", "idp_leagues")


    if keeper_leagues:
        batch_write_to_s3(keeper_leagues, "leagues/keeper", "keeper_leagues")
    
    if dynasty_leagues:
        batch_write_to_s3(dynasty_leagues, "leagues/dynasty", "dynasty_leagues")
        
    if large_league_leagues:
        batch_write_to_s3(large_league_leagues, "leagues/large", "large_league_leagues")

    if redraft_leagues:
        batch_write_to_s3(redraft_leagues, "leagues/redraft", "redraft_leagues")
    
    log_memory_usage("After Writing to S3")