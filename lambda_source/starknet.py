import os
import requests
import logging
import psycopg2
import helpers
from datetime import datetime, timedelta, timezone

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Starknet'

# `totalSupply` function selectors
TOTAL_SUPPLY_SELECTOR = "0x1557182e4359a1f0c6301278e8f5b35a776ab58d39892581e357578fb287836" # for wBTC

# Format Starknet contract address correctly (64 hex characters)
def format_starknet_address(address):
    address = address.lower().replace("0x", "")  # Remove "0x" prefix
    return "0x" + address.zfill(64)  # Zero-pad to ensure exactly 64 hex characters

def get_latest_block_number(rpc_url):
    response = requests.post(rpc_url, json={
        "jsonrpc": "2.0",
        "method": "starknet_blockNumber",
        "params": [],
        "id": 1
    }).json()

    if 'result' in response:
        return response['result']
    else:
        log.error(f"Error fetching latest block number: {response}")
        return None

# Fetch total supply for a token
def get_total_supply(token_address, rpc_url, decimals):
    """Get total supply of a token at the latest block."""
    starknet_address = format_starknet_address(token_address)  # Fix address formatting

    response = requests.post(rpc_url, json={
        "jsonrpc": "2.0",
        "method": "starknet_call",
        "params": [{
            "contract_address": starknet_address,
            "entry_point_selector": TOTAL_SUPPLY_SELECTOR,
            "calldata": []
        }, "latest"],
        "id": 1
    }).json()

    if 'result' not in response:
        log.error(f"Error fetching total supply for {token_address}: {response}")
        return None

    total_supply = int(response['result'][0], 16)
    try:
        supply = total_supply / (10 ** int(decimals))
        log.info(f"Supply fetched for {token_address}: {supply}")
        return supply
    except ValueError:
        log.error(f"Invalid decimals value: {decimals} for {token_address}")
        return None

# Lambda handler function
def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    starknet_rpc_url = api_secret.get('RPC_STARKNET')

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')

    # Incremental invocations -- run every 4 hours, update current date balance
    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()
    # Final invocations -- run at 00:15:00 UTC, update previous date balance
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)

    # Get the latest block number
    latest_block = get_latest_block_number(starknet_rpc_url)

    if latest_block is None:
        log.error(f"Could not fetch latest block for {day}")
        return

    token_values = {}

    for token in tokens:
        try:
            token_address = token.get('address')
            token_decimals = int(token.get('decimals')) if token.get('decimals') else None
            if not token_address:
                log.warning(f"No token_address for {token['slug']}")
                continue

            if not token_decimals:
                log.warning(f"No token_decimals for {token['slug']}")
                continue

            supply = get_total_supply(token_address, starknet_rpc_url, token_decimals)

            if not supply:
                log.warning(f"Error fetching total supply for {token['slug']}")
                continue

            log.info(f"{token['slug']} Total Supply: {supply} tokens")
            token_values[token['slug']] = supply
        except Exception as e:
            log.error(f"Error fetching total supply for {token['slug']}: {e}")

    # Insert total supply values into database
    conn = psycopg2.connect(
        dbname=db_secret.get('dbname'),
        user=db_secret.get('username'),
        password=db_secret.get('password'),
        host=db_secret.get('host'),
        port=db_secret.get('port')
    )

    try:
        with conn:
            with conn.cursor() as cursor:
                for token_slug, supply in token_values.items():
                    insert_query = """
                    INSERT INTO token_balances (token_implementation, date, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token_implementation, date)
                    DO UPDATE SET balance = EXCLUDED.balance
                    """
                    cursor.execute(insert_query, (
                        token_slug,
                        day,
                        supply
                    ))
                    conn.commit()
    except psycopg2.Error as e:
        log.error(f"Error connecting to DB: {e}")

    finally:
        conn.close()
