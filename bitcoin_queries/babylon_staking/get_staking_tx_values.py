import os
import csv
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

RPC_USER = os.getenv('RPC_USER_LOCAL')
RPC_PASS = os.getenv('RPC_PASS_LOCAL')
RPC_HOST = os.getenv('RPC_HOST_LOCAL')
RPC_PORT = os.getenv('RPC_PORT_LOCAL')

def bitcoin_rpc(method, params=None):
    url = f"http://{RPC_HOST}:{RPC_PORT}"
    headers = {'content-type': 'application/json'}
    payload = {
        "method": method,
        "params": params or [],
        "jsonrpc": "2.0",
        "id": "btc-staking"
    }
    try:
        response = requests.post(url, headers=headers, json=payload, auth=(RPC_USER, RPC_PASS))
        response.raise_for_status()
        result = response.json()
        if result.get("error"):
            print(f"RPC Error: {result['error']}")
            return None
        return result["result"]
    except requests.exceptions.RequestException as e:
        print(f"RPC failed: {e}")
        return None

def get_staking_values_from_blocks(input_csv, output_csv):
    with open(input_csv, mode='r') as infile, open(output_csv, mode='w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ['block_height', 'txid', 'value']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        rows = list(reader)
        seen = set()
        pbar = tqdm(total=len(rows), desc="Scanning blocks")

        for row in rows:
            txid = row["txid"]
            block_hash = row["block_hash"]
            block_height = row["block_height"]

            if txid in seen:
                pbar.update(1)
                continue
            seen.add(txid)

            block_data = bitcoin_rpc("getblock", [block_hash, 2])
            if not block_data:
                print(f"Failed to fetch block {block_hash}")
                pbar.update(1)
                continue

            tx = next((t for t in block_data["tx"] if t["txid"] == txid), None)
            if not tx:
                print(f"Tx {txid} not found in block {block_hash}")
                pbar.update(1)
                continue

            try:
                vout_0 = tx["vout"][0]
                writer.writerow({
                    "block_height": block_height,
                    "txid": txid,
                    "value": vout_0["value"]
                })
            except (IndexError, KeyError):
                print(f"Missing vout[0] in tx {txid}")

            pbar.update(1)

        pbar.close()
    print(f"Staking values saved to {output_csv}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_csv = os.path.join(script_dir, "babylon_staking_txs.csv")
    output_csv = os.path.join(script_dir, "babylon_staking_values.csv")

    get_staking_values_from_blocks(input_csv, output_csv)
