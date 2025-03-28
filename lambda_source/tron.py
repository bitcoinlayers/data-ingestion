import logging
import psycopg2
import requests
from datetime import datetime, timezone, timedelta
import helpers

log = logging.getLogger()
log.setLevel(logging.INFO)

TRONSCAN_BASE_URL = "https://apilist.tronscanapi.com/api"

def get_total_supply(token_address, api_key):
    url = f"{TRONSCAN_BASE_URL}/token_trc20/totalSupply?address={token_address}"
    headers = {"TRON-PRO-API-KEY": api_key}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response_data = response.json()

        if isinstance(response_data, dict) and "totalSupply" in response_data:
            return float(response_data["totalSupply"])
        elif isinstance(response_data, (int, float)):
            return float(response_data)
        else:
            log.warning(f"Unexpected response structure for {token_address}: {response_data}")
            return None
    except requests.exceptions.RequestException as e:
        log.error(f"HTTP request error for {token_address}: {e}")
        return None
    except ValueError as e:
        log.error(f"Error parsing JSON for {token_address}: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error for {token_address}: {e}")
        return None

def fetch_tron_current_data(api_secret, db_secret, invocation_type="incremental"):
    log.info("Fetching current total supply for Tron tokens...")

    utc_now = datetime.now(timezone.utc)
    if invocation_type == "final":
        now = utc_now.date() - timedelta(days=1)
    else:
        now = utc_now.date()

    log.info(f"UTC now: {utc_now.isoformat()} — inserting for: {now.isoformat()}")

    tron_api_key = api_secret.get('RPC_TRON_2')

    with psycopg2.connect(
        host=db_secret.get('host'),
        database=db_secret.get('dbname'),
        user=db_secret.get('username'),
        password=db_secret.get('password'),
        port=db_secret.get('port')
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT slug, token_address FROM token_implementations WHERE network = 'Tron'")
            tokens = cursor.fetchall()

            for slug, token_address in tokens:
                try:
                    supply = get_total_supply(token_address, tron_api_key)
                    if supply is not None:
                        log.info(f"{slug} Total Supply: {supply}")
                        cursor.execute("""
                            INSERT INTO token_balances (token_implementation, date, balance)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (token_implementation, date)
                            DO UPDATE SET balance = EXCLUDED.balance
                        """, (slug, now, supply))
                    else:
                        log.warning(f"Skipping {slug}: No supply data available.")
                except Exception as e:
                    log.error(f"Error processing {slug}: {e}")

            conn.commit()

    log.info("✅ Tron supply data updated successfully.")

def lambda_handler(event, context):
    log.info("🚀 Tron Lambda execution started.")
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    invocation_type = event.get("invocation_type", "incremental")
    fetch_tron_current_data(api_secret, db_secret, invocation_type=invocation_type)
    log.info("✅ Tron Lambda execution completed.")
    return {"status": "success"}

if __name__ == "__main__":
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    fetch_tron_current_data(api_secret, db_secret, invocation_type="incremental")
