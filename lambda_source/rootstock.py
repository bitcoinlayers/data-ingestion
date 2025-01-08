from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import json
import helpers
from web3 import Web3

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

# Constants
network_slug = 'Rootstock'
SPECIAL_TOKEN = 'Rootstock-RBTC_Rootstock'
SPECIAL_MAX_SUPPLY = 21_000_000  # 21 million RBTC

# Fetch circulating supply for RBTC (special case)
def get_circulating_rbtc(token_address, rpc_url):
    """Calculate circulating RBTC supply by subtracting balance from max supply."""
    if not Web3.is_address(token_address):  # Validate address
        log.error(f"Invalid token address: {token_address}")
        return None

    # Prepare JSON-RPC request
    headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
    data = {
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [token_address, "latest"],
        "id": 0
    }

    # Query balance
    response = requests.post(rpc_url, headers=headers, data=json.dumps(data))
    result = response.json().get('result')

    if not result:
        log.error(f"Failed to fetch balance for {token_address}")
        return None

    # Convert balance and compute circulating supply
    balance_wei = int(result, 16)
    balance_rbtc = balance_wei / (10 ** 18)
    circulating_rbtc = SPECIAL_MAX_SUPPLY - balance_rbtc
    return circulating_rbtc


# Lambda handler function
def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    rpc_url = api_secret.get('RPC_ROOTSTOCK')

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')
    # reserves = network_config.get('network_reserves')

    # Incremental invocations -- run every 4 hours, update current date balance
    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()

    # Final invocations -- run at 00:15:00 UTC, update previous date balance
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)


    # Connect to database
    conn = psycopg2.connect(
        dbname=db_secret.get('dbname'),
        user=db_secret.get('username'),
        password=db_secret.get('password'),
        host=db_secret.get('host'),
        port=db_secret.get('port')
    )
    cursor = conn.cursor()

    try:
        # Process tokens
        for token in tokens:
            try:
                token_address = token.get('address')
                if not token_address or not Web3.is_address(token_address):
                    log.error(f"Invalid token address for {token['slug']}")
                    continue

                # Handle special token case for RBTC
                if token['slug'] == SPECIAL_TOKEN:
                    supply = get_circulating_rbtc(token_address, rpc_url)
                else:
                    # Generic ERC-20 tokens use totalSupply()
                    decimals = int(token.get('decimals', 18))  # Default 18 decimals
                    supply = get_erc20_supply(token_address, rpc_url, decimals)

                if supply is None:
                    log.error(f"Failed to fetch supply for {token['slug']}")
                    continue

                log.info(f"{token['slug']} Total Supply: {supply}")

                # Insert token balance into database
                cursor.execute("""
                    INSERT INTO token_balances (token_implementation, date, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token_implementation, date)
                    DO UPDATE SET balance = EXCLUDED.balance
                """, (token['slug'], day, supply))

            except Exception as e:
                log.error(f"Error processing {token['slug']}: {e}")

        conn.commit()

    except Exception as e:
        log.error(f"Database error: {e}")
    finally:
        cursor.close()
        conn.close()


# Generic ERC-20 total supply fetch
def get_erc20_supply(token_address, rpc_url, decimals):
    """Fetch ERC-20 total supply."""
    try:
        function_selector = Web3.keccak(text="totalSupply()")[:4].hex()
        data = {"to": token_address, "data": "0x" + function_selector}
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [data, "latest"],
            "id": 1
        }
        response = requests.post(rpc_url, headers={'Content-Type': 'application/json'}, data=json.dumps(payload))
        result = response.json().get('result')

        if not result:
            log.error(f"Failed to fetch total supply for {token_address}")
            return None

        total_supply = int(result, 16)
        return total_supply / (10 ** decimals)
    except Exception as e:
        log.error(f"Error fetching ERC-20 supply for {token_address}: {e}")
        return None


# Generic balance fetch
def fetch_balance(address, rpc_url, decimals):
    """Fetch balance for a given address."""
    try:
        data = {
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [address, "latest"],
            "id": 0
        }
        response = requests.post(rpc_url, headers={'Content-Type': 'application/json'}, data=json.dumps(data))
        result = response.json().get('result')

        if not result:
            return None

        balance_wei = int(result, 16)
        return balance_wei / (10 ** decimals)
    except Exception as e:
        log.error(f"Error fetching balance for {address}: {e}")
        return None
