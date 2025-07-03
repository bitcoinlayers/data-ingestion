from datetime import datetime, timedelta, timezone
import logging
from web3 import Web3
import psycopg2
import requests
import helpers

log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Mode'
total_supply_function_data = Web3.keccak(text="totalSupply()")[:4].hex()

def get_total_supply(token_address, decimals, rpc_url):
    response = requests.post(rpc_url, json={
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {"to": token_address, "data": "0x" + total_supply_function_data},
            "latest"
        ],
        "id": 1
    }).json()

    if 'result' not in response or not response['result']:
        log.error(f"Error fetching supply for {token_address}: {response}")
        return None

    total_supply = int(response['result'], 16)
    return total_supply / (10 ** decimals)

def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    mode_rpc_url = api_secret.get('RPC_MODE')

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')

    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)

    token_values = {}

    for token in tokens:
        try:
            token_address = token.get('address')
            token_decimals = int(token.get('decimals')) if token.get('decimals') else None
            if not token_address or not token_decimals:
                log.warning(f"Missing config for {token.get('slug')}")
                continue

            supply = get_total_supply(token_address, token_decimals, mode_rpc_url)
            if not supply:
                log.warning(f"Failed fetching supply for {token.get('slug')}")
                continue

            log.info(f"{token.get('slug')} Supply: {supply}")
            token_values[token.get('slug')] = supply
        except Exception as e:
            log.error(f"Error processing {token.get('slug')}: {e}")

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
                    cursor.execute("""
                        INSERT INTO token_balances (token_implementation, date, balance)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (token_implementation, date)
                        DO UPDATE SET balance = EXCLUDED.balance
                    """, (token_slug, day, supply))
    except psycopg2.Error as e:
        log.error(f"Database error: {e}")
    finally:
        conn.close()
