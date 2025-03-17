import requests
import psycopg2
import logging
from datetime import datetime, timezone
import helpers
import json

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = "Sui"

def get_total_supply(coin_type, rpc_url):
    """
    Get the latest total supply of a Sui token using `suix_getTotalSupply`.
    If that fails, attempts to fetch the supply from `TreasuryCap`.
    """
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

        log.warning(f"⚠️ `suix_getTotalSupply` failed for {coin_type}. Trying TreasuryCap...")
        return None
    except requests.exceptions.RequestException as e:
        log.error(f"❌ Network error fetching total supply for {coin_type}: {e}")
        return None


def get_treasury_cap_supply(coin_type, rpc_url, fallback_ids=None):
    """
    Fetch total supply from the TreasuryCap object if `suix_getTotalSupply` fails.
    If fallback_ids (reserve_implementations) are provided, check them.
    """
    # Try to fetch TreasuryCap directly
    treasury_cap_object_id = f"0x2::coin::TreasuryCap<{coin_type}>"
    
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "suix_getObject",
        "params": [treasury_cap_object_id, {"showType": True, "showContent": True}],
    }

    try:
        response = requests.post(rpc_url, json=payload, timeout=10).json()
        if "result" in response and "data" in response["result"]:
            obj_data = response["result"]["data"]
            
            if "content" in obj_data and "fields" in obj_data["content"]:
                total_supply = obj_data["content"]["fields"].get("total_supply", {}).get("fields", {}).get("value")
                if total_supply:
                    return int(total_supply)

        log.warning(f"⚠️ Error: No TreasuryCap total supply found for {coin_type} in `suix_getObject`.")
    
    except requests.exceptions.RequestException as e:
        log.error(f"❌ Network error fetching TreasuryCap for {coin_type}: {e}")

    # If no supply found, try `reserve_implementations`
    if fallback_ids:
        for fallback_id in fallback_ids:
            log.warning(f"⚠️ Falling back to `reserve_implementation`: {fallback_id}...")

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sui_getObject",
                "params": [
                    fallback_id,
                    {
                        "showType": True,
                        "showContent": True,
                        "showOwner": False,
                        "showPreviousTransaction": False,
                        "showStorageRebate": False,
                    },
                ],
            }

            try:
                response = requests.post(rpc_url, json=payload, timeout=10).json()

                if "result" in response and "data" in response["result"]:
                    obj_data = response["result"]["data"]
                    
                    # Extract TreasuryCap from ControlledTreasury
                    treasury_cap = obj_data["content"]["fields"].get("treasury_cap", {})
                    if treasury_cap:
                        total_supply = treasury_cap.get("fields", {}).get("total_supply", {}).get("fields", {}).get("value")
                        if total_supply:
                            return int(total_supply)

            except requests.exceptions.RequestException as e:
                log.error(f"❌ Network error fetching `reserve_implementation` {fallback_id}: {e}")

    log.error(f"❌ Error: Could not find a valid TreasuryCap total supply for {coin_type}.")
    return None


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
    cursor.execute(
        "SELECT slug, token_address, token_decimals, reserve_implementations FROM token_implementations WHERE network = %s",
        (network_slug,)
    )
    tokens = cursor.fetchall()

    # Get current date (UTC)
    now = datetime.now(timezone.utc).date()

    for slug, token_address, decimals, reserve_implementations in tokens:
        try:
            # Fetch total supply
            supply_raw = get_total_supply(token_address, rpc_url)

            if supply_raw is None and reserve_implementations:
                log.warning(f"⚠️ Falling back to `reserve_implementations` for {slug}...")
                reserve_ids = json.loads(reserve_implementations) if isinstance(reserve_implementations, str) else reserve_implementations
                supply_raw = get_treasury_cap_supply(token_address, rpc_url, fallback_ids=reserve_ids)

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
            log.error(f"❌ Error processing {slug}: {e}")

    conn.commit()
    log.info("✅ Supply data updated successfully.")

    # Close database connection
    cursor.close()
    conn.close()


# Lambda entry point
def lambda_handler(event, context):
    """AWS Lambda entry point."""
    log.info("🚀 Lambda execution started.")

    # Fetch secrets from AWS Secrets Manager
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()

    fetch_sui_current_data(api_secret, db_secret)

    log.info("✅ Lambda execution completed.")
    return {"status": "success", "message": "Sui total supply updated."}
