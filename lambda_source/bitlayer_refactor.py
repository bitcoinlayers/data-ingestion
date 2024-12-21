from datetime import datetime, timezone, timedelta
import time
import logging
from web3 import Web3
import psycopg2
import requests
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Bitlayer'

bitlayer_rpc_url = "https://rpc.bitlayer.org"

# Hardcoded ABI for the token contracts
token_abi = [
    {"inputs": [], "name": "totalSupply", "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"}
]


# Function to get the block number closest to the provided timestamp
def get_block_by_timestamp(timestamp):
    try:
        response = requests.get(f"https://api.btrscan.com/scan/api?module=block&action=getblocknobytime&timestamp={timestamp}&closest=before")
        if response.status_code == 200:
            result = response.json()
            block_number = result.get('result', None)
            if block_number:
                return int(block_number)
            else:
                log.error(f"Error: No block found for timestamp {timestamp}")
                return None
        else:
            log.error(f"Error fetching block number: {response.status_code}")
            return None
    except Exception as e:
        log.error(f"Exception during block fetch: {e}")
        return None

# Function to call totalSupply() at a specific block
def get_total_supply_at_block(block_number, token_address, token_decimals):
    try:
        web3 = Web3(Web3.HTTPProvider(bitlayer_rpc_url))
        contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=token_abi)
        total_supply = contract.functions.totalSupply().call(block_identifier=block_number)
        return total_supply / (10 ** token_decimals)
    except Exception as e:
        log.error(f"Exception during totalSupply call: {e}")
        return None

# Lambda handler function
def lambda_handler(event, context):
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    eth_rpc_url = f"https://arb-mainnet.g.alchemy.com/v2/{api_secret.get('API_KEY_ALCHEMY')}"

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')
    reserves = network_config.get('network_reserves')

    current_date = datetime.now(timezone.utc)

    # Get the previous day's midnight UTC timestamp
    previous_day = current_date - timedelta(days=1)
    previous_day_timestamp = int(time.mktime(previous_day.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))

    block_number = get_block_by_timestamp(previous_day_timestamp)

    if not block_number:
        log.error(f"Could not fetch block for {previous_day}")
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

            supply = get_total_supply_at_block(block_number, token_address, token_decimals)

            if not supply:
                log.error(f"Error fetching total supply for {token['slug']}")
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
                        previous_day.strftime('%Y-%m-%d'),
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
                #         yesterday,
                #         supply
                #     ))
                #     conn.commit()
    except psycopg2.Error as e:
        log.error(f"Error connecting to DB: {e}")


    finally:
        conn.close()
