from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Liquid'

def get_lbtc_circulating_supply(liquid_rpc_url):
    response = requests.get(liquid_rpc_url)
    if response.status_code == 200:
        supply = response.json()
        return float(supply) / 10**8
    else:
        raise Exception(f"Error retrieving supply: {response.status_code}, {response.text}")


# Lambda handler function
def lambda_handler(event, context):
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    liquid_rpc_url = api_secret.get('RPC_LIQUID')

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')
    reserves = network_config.get('network_reserves')

    # Fetch yesterday's date
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

    token_values = {}
    reserve_values = {}

    for token in tokens:
        try:
            token_address = token.get('address')
            if not token_address:
                log.error(f"No token_address for {token['slug']}")
                continue

            supply = get_lbtc_circulating_supply(liquid_rpc_url)

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
                        yesterday,
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
