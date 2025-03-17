import requests
import psycopg2
import logging
from datetime import datetime, timezone
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = "Sui"

# Fetch total supply using `suix_getTotalSupply`
def get_total_supply(coin_type, rpc_url):
    """Get the latest total supply of a Sui token using `suix_getTotalSupply`."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getTotalSupply",
        "params": [coin_type]
    }

    try:
        response = requests.post(rpc_url, json=payload, timeout=10).json()
        if "result" in response and "value" in response["result"]:
            return int(response["result"]["value"])  # Convert string to int

        log.warning(f"Total supply not found for {coin_type}: {response}")
        return None
    except requests.exceptions.RequestException as e:
        log.error(f"Network error fetching total supply for {coin_type}: {e}")
        return None

# Fetch and store current supply values
def fetch_sui_current_data(api_secret, db_secret):
    """Fetch and store the latest balances for all Sui tokens."""
    log.info(f"Fetching current total supply for Sui tokens...")

    rpc_url = api_secret.get("RPC_SUI")

    # Connect to the database
    conn = psycopg2.connect(
        dbname=db_secret.get("dbname"),
        user=db_secret.get("username"),
        password=db_secret.get("password"),
        host=db_secret.get("host"),
        port=db_secret.get("port"),
    )
    cursor = conn.cursor()

    # Query token implementations from the database
    cursor.execute("SELECT slug, token_address, token_decimals FROM token_implementations WHERE network = %s", (network_slug,))
    tokens = cursor.fetchall()

    # Get current date (UTC)
    now = datetime.now(timezone.utc).date()

    for slug, token_address, decimals in tokens:
        try:
            # Fetch total supply
            supply_raw = get_total_supply(token_address, rpc_url)

            if supply_raw is None:
                log.warning(f"Skipping {slug}: No supply data available.")
                continue  # Skip if no supply data is found

            # Convert raw supply using stored decimals
            supply = supply_raw / (10 ** int(decimals))
            log.info(f"{slug} Total Supply: {supply}")

            # Insert into database
            cursor.execute(
                """
                INSERT INTO token_balances (token_implementation, date, balance)
                VALUES (%s, %s, %s)
                ON CONFLICT (token_implementation, date)
                DO UPDATE SET balance = EXCLUDED.balance
                """,
                (slug, now, supply),
            )
        except Exception as e:
            log.error(f"Error processing {slug}: {e}")

    conn.commit()
    log.info("Supply data updated successfully.")

    # Close database connection
    cursor.close()
    conn.close()

# Lambda entry point
def lambda_handler(event, context):
    """AWS Lambda entry point."""
    log.info("Lambda execution started.")

    # Fetch secrets from AWS Secrets Manager
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()

    fetch_sui_current_data(api_secret, db_secret)

    log.info("Lambda execution completed.")
    return {"status": "success", "message": "Sui total supply updated."}
