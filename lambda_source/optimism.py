from datetime import datetime, timedelta, timezone
import logging
from web3 import Web3
import psycopg2
import requests
import helpers
from alchemy import Alchemy, Network

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Optimism'
alchemy_network = Network.OPT_MAINNET

# Load totalSupply function selector
total_supply_function_data = Web3.keccak(text="totalSupply()")[:4].hex()

# Get block number based on timestamp using binary search
def get_block_by_timestamp(timestamp, optimism_rpc_url):
    lower_bound = 0
    upper_bound = int(requests.post(optimism_rpc_url, json={
        "jsonrpc": "2.0",
        "method": "eth_blockNumber",
        "params": [],
        "id": 1
    }).json().get('result', '0x0'), 16)

    while lower_bound <= upper_bound:
        mid_point = (lower_bound + upper_bound) // 2
        response = requests.post(optimism_rpc_url, json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(mid_point), False],
            "id": 1
        }).json()

        block = response['result']
        block_timestamp = int(block['timestamp'], 16)

        if block_timestamp < timestamp:
            lower_bound = mid_point + 1
        elif block_timestamp > timestamp:
            upper_bound = mid_point - 1
        else:
            return mid_point
    return upper_bound

# Fetch total supply for a given token and block
def get_total_supply(token_address, block_identifier, decimals, optimism_rpc_url):
    log.info(f"Fetching total supply for token: {token_address} at block: {block_identifier}")

    response = requests.post(optimism_rpc_url, json={
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": token_address,
            "data": "0x" + total_supply_function_data
        }, hex(block_identifier)],
        "id": 1
    }).json()

    if 'result' not in response:
        log.error(f"Error fetching total supply for {token_address}: {response}")
        return None

    total_supply = int(response['result'], 16)
    return total_supply / (10 ** decimals)

# Fetch total supply for a given reserve
def get_reserve_supply(alchemy, reserve_slug, reserve_address, collateral_token_address, decimals):
    try:
        collateral_balance = alchemy.core.get_token_balances(address=reserve_address, data=[collateral_token_address])
        total_supply = int(collateral_balance.get('token_balances')[0].token_balance, 16)
        return total_supply / (10 ** int(decimals))
    except Exception as e:
        log.error(f"Error fetching total supply for {reserve_slug}: {e}")

# Lambda handler function
def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    optimism_rpc_url = api_secret.get('RPC_OPTIMISM')

    api_key = api_secret.get('API_KEY_ALCHEMY')
    alchemy = Alchemy(api_key, alchemy_network, max_retries=3) 

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')
    reserves = network_config.get('network_reserves')

    # Incremental invocations -- run every 4 hours, update current date balance
    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()
        timestamp = int(datetime.now(timezone.utc).timestamp())

    # Final invocations -- run at 00:15:00 UTC, update previous date balance
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)
        timestamp = int(datetime.combine(day, datetime.max.time(), tzinfo=timezone.utc).timestamp())  # 23:59:59 UTC

    block_number = get_block_by_timestamp(timestamp, optimism_rpc_url)

    if not block_number:
        log.error(f"Could not fetch block for {day}")
        return

    token_values = {}
    reserve_values = {}

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

            supply = get_total_supply(token_address, block_number, token_decimals, optimism_rpc_url)

            if not supply:
                log.warning(f"Error fetching total supply for {token['slug']}")
                continue

            log.info(f"{token['slug']} Total Supply: {supply} tokens")
            token_values[token['slug']] = supply
        except Exception as e:
            log.error(f"Error fetching total supply for {token['slug']}: {e}")

    for reserve in reserves:
        try:
            reserve_address = reserve.get('address')
            reserve_slug = reserve.get('slug')
            reserve_implementation_id = reserve.get('id')
            collateral_token = reserve.get('collateral_token')
            derivative_token = reserve.get('derivative_token')

            if not collateral_token:
                log.warning(f"No collateral_token for reserve: {reserve_slug}")
                continue
            
            collateral_token_address = collateral_token.get('address')
            collateral_token_decimals = collateral_token.get('decimals')

            if not collateral_token_address:
                log.warning(f"No collateral_token_address for {reserve_slug}")
                continue

            if not collateral_token_decimals:
                log.warning(f"No collateral_token_decimals for {reserve_slug}")
                continue

            supply = get_reserve_supply(alchemy, reserve_slug, reserve_address, collateral_token_address, collateral_token_decimals)

            if not supply:
                log.warning(f"Error fetching total supply for {reserve_slug}")
                continue

            reserve_values[reserve_implementation_id] = {
                'balance': supply,
                'reserve_network': network_slug,
                'collateral_token': collateral_token.get('slug'),
                'derivative_token': derivative_token.get('slug'),
                'reserve_address': reserve_address,
            }

        except Exception as e:
            log.error(f"[RESERVE] Error fetching total supply for {reserve['slug']}: {e}")

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

                for reserve_implementation_id, balance_data in reserve_values.items():
                    insert_query = """
                    INSERT INTO reserve_balances (
                        date, 
                        balance, 
                        reserve_implementation_id, 
                        reserve_network, 
                        collateral_token, 
                        derivative_token, 
                        reserve_address
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (
                        date, 
                        reserve_network, 
                        reserve_address, 
                        collateral_token, 
                        derivative_token
                    )
                    DO UPDATE SET balance = EXCLUDED.balance
                    """
                    cursor.execute(insert_query, (
                        day,
                        balance_data['balance'],
                        reserve_implementation_id,
                        balance_data['reserve_network'],
                        balance_data['collateral_token'],
                        balance_data['derivative_token'],
                        balance_data['reserve_address']
                    ))
                    conn.commit()
    except psycopg2.Error as e:
        log.error(f"Error connecting to DB: {e}")


    finally:
        conn.close()
