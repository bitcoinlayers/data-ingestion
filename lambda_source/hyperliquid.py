from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import json
import helpers
from web3 import Web3

log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Hyperliquid'
SPECIAL_TOKEN = 'Unit-UBTC_Hyperliquid'
SPECIAL_MAX_SUPPLY = 21_000_000
DECIMALS = 8
UNMINTED_ADDR = '0x20000000000000000000000000000000000000c5'

def get_circulating_ubtc(token_address, rpc_url):
    try:
        if not Web3.is_address(token_address):
            log.error(f"Invalid token address: {token_address}")
            return None

        selector = Web3.keccak(text="balanceOf(address)")[:4].hex()
        padded_addr = UNMINTED_ADDR.lower().replace("0x", "").rjust(64, "0")
        data = "0x" + selector + padded_addr

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": token_address, "data": data}, "latest"],
            "id": 1
        }

        response = requests.post(rpc_url, headers={'Content-Type': 'application/json'}, data=json.dumps(payload))
        result = response.json().get('result')

        if not result:
            log.error(f"Empty result for unminted balance at {token_address}")
            return None

        unminted = int(result, 16) / (10 ** DECIMALS)
        return SPECIAL_MAX_SUPPLY - unminted

    except Exception as e:
        log.error(f"Error in get_circulating_ubtc: {e}")
        return None

def lambda_handler(event, context):
    log.info("Hyperliquid Lambda execution started.")
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    rpc_url = api_secret.get('RPC_HYPERLIQUID')
    invocation_type = event.get('invocation_type', 'incremental')

    utc_now = datetime.now(timezone.utc)
    if invocation_type == "final":
        now = utc_now.date() - timedelta(days=1)
    else:
        now = utc_now.date()

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
                    if slug != SPECIAL_TOKEN:
                        log.warning(f"Skipping unsupported token: {slug}")
                        continue

                    supply = get_circulating_ubtc(address, rpc_url)
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

    log.info("Hyperliquid Lambda completed.")
    return {"status": "success"}
