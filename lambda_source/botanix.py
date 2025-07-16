from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import json
import helpers
from web3 import Web3

log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Botanix'
SPECIAL_TOKEN = 'Botanix-BTC_Botanix'
SPECIAL_MAX_SUPPLY = 21_000_000
DECIMALS_SPECIAL = 18
SPECIAL_WALLET = '0x0Ea320990B44236A0cEd0ecC0Fd2b2df33071e78'

# totalSupply selector
total_supply_function_data = Web3.keccak(text="totalSupply()")[:4].hex()

def get_circulating_btx(wallet_addr, rpc_url):
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [wallet_addr, "latest"],
            "id": 1
        }
        response = requests.post(rpc_url, json=payload)
        result = response.json().get('result')

        if not result:
            log.error(f"Empty result for native balance at {wallet_addr}")
            return None

        native_balance = int(result, 16) / (10 ** DECIMALS_SPECIAL)
        return SPECIAL_MAX_SUPPLY - native_balance

    except Exception as e:
        log.error(f"Error in get_circulating_btx: {e}")
        return None

def get_total_supply(token_address, decimals, rpc_url):
    try:
        response = requests.post(rpc_url, json={
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "to": token_address,
                "data": "0x" + total_supply_function_data
            }, "latest"],
            "id": 1
        }).json()

        if 'result' not in response or response['result'] is None:
            log.error(f"Error fetching total supply for {token_address}: {response}")
            return None

        total_supply = int(response['result'], 16)
        return total_supply / (10 ** int(decimals))
    except Exception as e:
        log.error(f"Error in get_total_supply for {token_address}: {e}")
        return None

def lambda_handler(event, context):
    log.info("Botanix Lambda execution started.")
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    rpc_url = api_secret.get('RPC_BOTANIX')
    invocation_type = event.get('invocation_type', 'incremental')

    utc_now = datetime.now(timezone.utc)
    now = utc_now.date() if invocation_type == "incremental" else utc_now.date() - timedelta(days=1)

    log.info(f"UTC now: {utc_now.isoformat()} — inserting for: {now.isoformat()}")

    network_config = helpers.get_network_config(network_slug, db_secret)
    tokens = network_config.get('network_tokens')

    with psycopg2.connect(
        host=db_secret.get('host'),
        database=db_secret.get('dbname'),
        user=db_secret.get('username'),
        password=db_secret.get('password'),
        port=db_secret.get('port')
    ) as conn:
        with conn.cursor() as cursor:
            for token in tokens:
                try:
                    slug = token.get('slug')
                    address = token.get('address')
                    decimals = token.get('decimals')

                    if not address or not decimals:
                        log.warning(f"Skipping token {slug} due to missing address/decimals.")
                        continue

                    if slug == SPECIAL_TOKEN:
                        supply = get_circulating_btx(SPECIAL_WALLET, rpc_url)
                    else:
                        supply = get_total_supply(address, decimals, rpc_url)

                    if supply is not None:
                        log.info(f"{slug} Total Supply: {supply}")
                        cursor.execute("""
                            INSERT INTO token_balances (token_implementation, date, balance)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (token_implementation, date)
                            DO UPDATE SET balance = EXCLUDED.balance
                        """, (slug, now, supply))
                    else:
                        log.warning(f"No supply fetched for {slug}")

                except Exception as e:
                    log.error(f"Error processing {slug}: {e}")

            conn.commit()

    log.info("Botanix Lambda completed.")
    return {"status": "success"}
