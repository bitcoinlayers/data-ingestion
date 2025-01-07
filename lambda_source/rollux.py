import os
import logging
import requests
from datetime import datetime, timedelta, timezone
import psycopg2
from dotenv import load_dotenv

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

# Load environment variables
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
RPC_ROLLUX = os.getenv('RPC_ROLLUX')

# Database connection
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = conn.cursor()

# Fetch block by timestamp
def get_block_by_timestamp(timestamp):
    response = requests.get(f"{RPC_ROLLUX}/blocks", params={"timestamp": timestamp})
    if response.status_code == 200:
        blocks = response.json()
        if 'items' in blocks and len(blocks['items']) > 0 and 'height' in blocks['items'][0]:
            return blocks['items'][0]['height']
    log.error(f"Error fetching block for timestamp {timestamp}: {response.status_code}, Response: {response.text}")
    return None

# Fetch token supply
def get_total_supply(token_address):
    response = requests.get(f"{RPC_ROLLUX}/tokens/{token_address}")
    if response.status_code == 200:
        token_info = response.json()
        try:
            supply = float(token_info['total_supply']) / (10 ** int(token_info['decimals']))
            log.info(f"Fetched supply for {token_address}: {supply}")
            return supply
        except Exception as e:
            log.error(f"Error processing supply for {token_address}: {e}")
            return None
    log.error(f"Error fetching total supply for {token_address}: {response.status_code}")
    return None

# Lambda handler function
def lambda_handler(event, context):
    log.info("Starting Rollux Lambda")

    # Fetch yesterday's date
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    end_timestamp = int(datetime.combine(yesterday, datetime.max.time(), tzinfo=timezone.utc).timestamp())  # 23:59:59 UTC
    end_block = get_block_by_timestamp(end_timestamp)

    if not end_block:
        log.error(f"Could not fetch end block for {yesterday}")
        return

    cursor.execute("SELECT slug, token_address FROM token_implementations WHERE network = 'Rollux'")
    tokens = cursor.fetchall()

    for token in tokens:
        try:
            slug, token_address = token
            supply = get_total_supply(token_address)

            if supply is not None:
                log.info(f"{slug} Total Supply: {supply}")
                cursor.execute("""
                    INSERT INTO token_balances (token_implementation, date, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token_implementation, date)
                    DO UPDATE SET balance = EXCLUDED.balance
                """, (slug, yesterday, supply))
        except Exception as e:
            log.error(f"Error processing {slug}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    log.info("Rollux Lambda execution completed")
