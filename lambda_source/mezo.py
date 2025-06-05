from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import helpers

log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Mezo'
MEZO_API_BASE = "https://api.explorer.mezo.org/api/v2/tokens"

def get_mezo_supply(token_address: str):
    try:
        url = f"{MEZO_API_BASE}/{token_address}"
        headers = {"accept": "application/json"}
        res = requests.get(url, headers=headers)

        if res.status_code != 200:
            log.error(f"Failed to fetch {token_address}: {res.status_code}")
            return None

        data = res.json()
        raw_supply = int(data["total_supply"])
        decimals = int(data["decimals"])
        return raw_supply / (10 ** decimals)
    except Exception as e:
        log.error(f"Error in get_mezo_supply for {token_address}: {e}")
        return None

def get_latest_reserve_balance_for(slug: str, conn) -> float | None:
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT balance
                FROM reserve_balances rb
                JOIN reserve_implementations ri ON rb.reserve_implementation_id = ri.id
                WHERE ri.derivative_token = %s
                ORDER BY rb.date DESC
                LIMIT 1
            """, (slug,))
            row = cursor.fetchone()
            return float(row[0]) if row else None
    except Exception as e:
        log.error(f"Failed to get fallback reserve balance for {slug}: {e}")
        return None

def lambda_handler(event, context):
    log.info("Mezo Lambda execution started.")
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
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

                    if not address:
                        log.warning(f"No address for {slug}, attempting fallback to reserve balance...")
                        supply = get_latest_reserve_balance_for(slug, conn)
                        if supply is not None:
                            log.info(f"{slug} Fallback supply from reserve: {supply}")
                        else:
                            log.warning(f"No fallback balance found for {slug}. Skipping.")
                            continue
                    else:
                        if not decimals:
                            log.warning(f"No decimals for {slug}, skipping.")
                            continue
                        supply = get_mezo_supply(address)
                        if supply is None:
                            log.warning(f"Mezo supply fetch failed for {slug}. Skipping.")
                            continue
                        log.info(f"{slug} Total Supply from Mezo: {supply}")

                    cursor.execute("""
                        INSERT INTO token_balances (token_implementation, date, balance)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (token_implementation, date)
                        DO UPDATE SET balance = EXCLUDED.balance
                    """, (slug, now, supply))

                except Exception as e:
                    log.error(f"Error processing {slug}: {e}")

            conn.commit()

    log.info("Mezo Lambda completed.")
    return {"status": "success"}
