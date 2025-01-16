from datetime import datetime, timedelta, timezone
import logging
from web3 import Web3
import psycopg2
import requests
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Optimism'

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

# Lambda handler function
def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    optimism_rpc_url = api_secret.get('RPC_OPTIMISM')

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
            log.info(f"{reserve_slug} - {reserve_address}")

            # Here we pull reserve totals, assigning a key:value to reserve_values corresponding
            # to the reserve_slug and balance

            # Here is the structure of a reserve in network_reserves:
            # [
            #     {
            #         "tag": "13",
            #         "slug": "Solv-SolvBTC_shared__backedby__BitGo-wBTC_Arbitrum__13",
            #         "address": "0x032470aBBb896b1255299d5165c1a5e9ef26bcD2",
            #         "collateral_token": {
            #             "slug": "BitGo-wBTC_Arbitrum",
            #             "address": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
            #             "decimals": "8"
            #         },
            #         "derivative_token": {
            #             "slug": "Solv-SolvBTC_shared",
            #             "address": "",
            #             "decimals": ""
            #         }
            #     },
            #     {
            #         "tag": "26",
            #         "slug": "Pump-pumpBTC_shared__backedby__FireBitcoin-FBTC_Arbitrum__26",
            #         "address": "0x4413ca15da17db82826caee058c083f573c1f16c",
            #         "collateral_token": {
            #             "slug": "FireBitcoin-FBTC_Arbitrum",
            #             "address": "0xC96dE26018A54D51c097160568752c4E3BD6C364",
            #             "decimals": "8"
            #         },
            #         "derivative_token": {
            #             "slug": "Pump-pumpBTC_shared",
            #             "address": "",
            #             "decimals": ""
            #         }
            #     }
            # ]

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

                # Uncomment when we implement reserve balance fetches
                # for reserve_slug, supply in reserve_values.items():
                #     insert_query = """
                #     INSERT INTO reserve_balances (reserve_implementation, date, balance)
                #     VALUES (%s, %s, %s)
                #     ON CONFLICT (reserve_implementation, date)
                #     DO UPDATE SET balance = EXCLUDED.balance
                #     """
                #     cursor.execute(insert_query, (
                #         reserve_slug,
                #         day,
                #         supply
                #     ))
                #     conn.commit()
    except psycopg2.Error as e:
        log.error(f"Error connecting to DB: {e}")


    finally:
        conn.close()
