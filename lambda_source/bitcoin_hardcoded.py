# pulling data from the bitcoin side by hardcoding in token_implementation slugs and matching them to reserve addresses.

from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

BLOCKSTREAM_API = "https://blockstream.info/api"

# Fetch BTC balance for an address
def get_btc_balance(address):
    try:
        url = f'{BLOCKSTREAM_API}/address/{address}'
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        balance = data['chain_stats']['funded_txo_sum'] - data['chain_stats']['spent_txo_sum']
        return balance / 1e8
    except requests.exceptions.RequestException as e:
        log.error(f"Error fetching balance for {address}: {e}")
        return None

# Lambda handler function
def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')

    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()

    # Load slugs to track from network config (comma-separated string or list)
    target_slugs = "Simple-sBTC_Fractal"
    if isinstance(target_slugs, str):
        target_slugs = [s.strip() for s in target_slugs.split(',') if s.strip()]
    log.info(f"Target token slugs: {target_slugs}")

    if not target_slugs:
        log.warning("No token slugs provided. Exiting.")
        return

    # Incremental invocations -- run every 4 hours, update current date balance
    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)

    token_values = {}

    # Query reserve_implementations across all networks
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
                # Query for all relevant reserve implementations
                placeholders = ','.join(['%s'] * len(target_slugs))
                cursor.execute(f"""
                    SELECT derivative_token, reserve_address
                    FROM reserve_implementations
                    WHERE reserve_network = 'bitcoin'
                    AND derivative_token = ANY(ARRAY[{placeholders}])
                """, target_slugs)

                reserves = cursor.fetchall()

                for derivative_token, reserve_address in reserves:
                    try:
                        balance = get_btc_balance(reserve_address)
                        if balance is None:
                            log.warning(f"No balance fetched for {derivative_token} at {reserve_address}")
                            continue

                        log.info(f"{derivative_token} Balance for {reserve_address}: {balance} BTC")
                        token_values[derivative_token] = token_values.get(derivative_token, 0) + balance

                    except Exception as e:
                        log.error(f"Error processing {reserve_address}: {e}")

                # Insert aggregated balances into token_balances
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

    except psycopg2.Error as e:
        log.error(f"Database error: {e}")

    finally:
        conn.close()

    log.info("BTC Reserve Lambda completed.")
