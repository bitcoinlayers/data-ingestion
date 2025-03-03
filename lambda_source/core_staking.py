from datetime import datetime, timedelta, timezone
import logging
import psycopg2
import requests
import helpers

# Setup logging
log = logging.getLogger()
log.setLevel(logging.INFO)

network_slug = 'Core'
core_staking_api_url = 'https://staking-api.coredao.org/staking/summary/overall'

def get_staked_btc():
    log.info("Fetching staked BTC amount from Core Staking API")
    
    response = requests.get(core_staking_api_url, headers={'Accept': 'application/json'})
    response.raise_for_status()
    
    data = response.json()
    staked_btc = data.get("data", {}).get("stakedBTCAmount")
    
    if staked_btc is None:
        log.error("Failed to retrieve stakedBTCAmount from API response")
        return None
    
    return int(staked_btc) / 1e8  # Convert from sats to BTC

def lambda_handler(event, context):
    invocation_type = event.get('invocation_type', 'incremental')
    
    api_secret = helpers.get_api_secret()
    db_secret = helpers.get_db_secret()
    
    if invocation_type == 'incremental':
        day = datetime.now(timezone.utc).date()
    else:
        day = datetime.now(timezone.utc).date() - timedelta(days=1)
    
    try:
        staked_btc = get_staked_btc()
        
        if staked_btc is None:
            log.warning("No valid staked BTC amount retrieved")
            return
        
        log.info(f"Staked BTC: {staked_btc} BTC")
        
        conn = psycopg2.connect(
            dbname=db_secret.get('dbname'),
            user=db_secret.get('username'),
            password=db_secret.get('password'),
            host=db_secret.get('host'),
            port=db_secret.get('port')
        )
        
        with conn:
            with conn.cursor() as cursor:
                insert_query = """
                INSERT INTO token_balances (token_implementation, date, balance)
                VALUES ('Core-stakedBTC_staking', %s, %s)
                ON CONFLICT (token_implementation, date)
                DO UPDATE SET balance = EXCLUDED.balance
                """
                cursor.execute(insert_query, (day, staked_btc))
                conn.commit()
        
    except psycopg2.Error as e:
        log.error(f"Error connecting to DB: {e}")
    
    except Exception as e:
        log.error(f"Unexpected error: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()
