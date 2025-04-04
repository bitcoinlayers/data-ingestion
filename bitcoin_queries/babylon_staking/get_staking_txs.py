import os
import time
import requests
import csv
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

RPC_USER = os.getenv('RPC_USER_LOCAL')
RPC_PASS = os.getenv('RPC_PASS_LOCAL')
RPC_HOST = os.getenv('RPC_HOST_LOCAL')
RPC_PORT = os.getenv('RPC_PORT_LOCAL')

session = requests.Session()
session.headers.update({'content-type': 'application/json'})
session.auth = (RPC_USER, RPC_PASS)

def bitcoin_rpc(method, params=None):
    payload = {
        "method": method,
        "params": params or [],
        "jsonrpc": "2.0",
        "id": "python-btc"
    }
    print(f">>> Calling {method} with {params}")
    try:
        response = session.post(f"http://{RPC_HOST}:{RPC_PORT}", json=payload)
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            print(f"RPC Error: {data['error']}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        print(f"RPC request failed: {e}")
        return None

def parse_restaking_txs_to_csv(start_block, stop_block, version_prefix="6a4762626e31", output_file="babylon_staking_txs.csv"):
    print(f"Scanning from block {start_block} to block {stop_block}")

    with open(output_file, mode='w', newline='') as csvfile:
        fieldnames = ["block_height", "block_hash", "txid", "value", "script_hex", "timestamp"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    pbar = tqdm(total=(stop_block - start_block + 1), desc="Scanning blocks", unit="block")
    buffer = []
    buffer_size = 10

    for current_block in range(start_block, stop_block + 1):
        time.sleep(0.1)

        block_hash_resp = bitcoin_rpc("getblockhash", [current_block])
        if not block_hash_resp or "result" not in block_hash_resp:
            print(f"[{datetime.now().isoformat()}] Failed to get block hash for {current_block}")
            continue
        block_hash = block_hash_resp["result"]

        # Fetch full block with all txs included (fine for pruned nodes, but slow)
        block_resp = bitcoin_rpc("getblock", [block_hash, 2])
        if not block_resp or "result" not in block_resp:
            print(f"[{datetime.now().isoformat()}] Failed to get block {current_block}")
            with open("skipped_blocks.log", "a") as f:
                f.write(f"{current_block}\n")
            continue

        block = block_resp["result"]
        for tx in block["tx"]:
            for vout in tx.get("vout", []):
                script_hex = vout["scriptPubKey"].get("hex", "")
                if script_hex.startswith(version_prefix):
                    buffer.append({
                        "block_height": current_block,
                        "block_hash": block_hash,
                        "txid": tx["txid"],
                        "value": vout["value"],
                        "script_hex": script_hex,
                        "timestamp": datetime.fromtimestamp(block["time"]).isoformat()
                    })

        if len(buffer) >= buffer_size:
            with open(output_file, mode='a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerows(buffer)
            buffer.clear()

        with open(os.path.join(os.path.dirname(__file__), "last_block_checked.txt"), "w") as f:
            f.write(str(current_block))

        pbar.update(1)

    # Flush remaining
    if buffer:
        with open(output_file, mode='a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writerows(buffer)

    pbar.close()
    print(f"Finished scanning up to block {stop_block}. Output written to {output_file}")

if __name__ == "__main__":
    CONFIG_START_BLOCK = 857910  # before Babylon Phase 1 began

    blockchain_info = bitcoin_rpc("getblockchaininfo")
    if not blockchain_info or "result" not in blockchain_info:
        raise Exception("Unable to fetch blockchain info")

    info = blockchain_info["result"]
    CURRENT_BLOCK = info["blocks"]
    PRUNE_HEIGHT = info.get("pruneheight", 0)

    START_BLOCK = max(CONFIG_START_BLOCK, PRUNE_HEIGHT + 1)
    STOP_BLOCK = CURRENT_BLOCK

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, "babylon_staking_txs.csv")
    parse_restaking_txs_to_csv(start_block=START_BLOCK, stop_block=STOP_BLOCK, output_file=output_file)
