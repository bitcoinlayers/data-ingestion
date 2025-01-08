from datetime import datetime, timedelta, timezone
import logging
from web3 import Web3
import psycopg2
import requests
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Solana'

# Fetch total supply for a given token
def get_total_supply(token_address, solana_rpc_url):
    try:
        response = requests.post(solana_rpc_url, json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenSupply",
            "params": [token_address, { "commitment": "finalized" }]
        }).json()

        if 'result' not in response or 'value' not in response['result']:
            log.error(f"Error fetching total supply for {token_address}: {response}")
            return None

        value = response['result']['value']
        total_supply = int(value['amount'])
        decimals = int(value['decimals'])
        return total_supply / (10 ** decimals)

    except Exception as e:
        log.error(f"Exception fetching total supply for {token_address}: {e}")
        return None


# Lambda handler function
def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    solana_rpc_url = api_secret.get('RPC_SOLANA')

    # Get tokens from network config
    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')

    # Incremental invocations -- run every 4 hours, update current date balance
    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()

    # Final invocations -- run at 00:15:00 UTC, update previous date balance
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)

    # Establish DB connection
    conn = psycopg2.connect(
        dbname=db_secret.get('dbname'),
        user=db_secret.get('username'),
        password=db_secret.get('password'),
        host=db_secret.get('host'),
        port=db_secret.get('port')
    )

    try:
        token_values = {}

        # Fetch and process token supplies
        for token in tokens:
            try:
                token_address = token.get('address')
                if not token_address:
                    log.warning(f"No address for {token['slug']}")
                    continue

                supply = get_total_supply(token_address, solana_rpc_url)
                if supply is None:
                    log.error(f"Error fetching supply for {token['slug']}")
                    continue

                log.info(f"{token['slug']} Total Supply: {supply} tokens")
                token_values[token['slug']] = supply
            except Exception as e:
                log.error(f"Error fetching total supply for {token['slug']}: {e}")

        # Insert data into DB
        with conn:
            with conn.cursor() as cursor:
                for token_slug, supply in token_values.items():
                    insert_query = """
                    INSERT INTO token_balances (token_implementation, date, balance)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (token_implementation, date)
                    DO UPDATE SET balance = EXCLUDED.balance
                    """
                    cursor.execute(insert_query, (token_slug, day, supply))
                    conn.commit()

    except psycopg2.Error as e:
        log.error(f"Database error: {e}")
    finally:
        conn.close()
