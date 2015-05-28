__author__ = 'nii236'
from pprint import pprint as pp
import json
import sys
import bitcoin
import bitcoin.rpc
import requests

# Choose the keys that you want in the database here. TXes are hardcoded as included.
DESIRED_BLOCK_KEYS = ("height", "nextblockhash", "previousblockhash", "size", "time", "difficulty")
DESIRED_TXIN_KEYS = ("vout", "txid")
DESIRED_TXOUT_KEYS = ("n", "addresses", "value")
DB_WRITE_URL = "http://127.0.0.1:64210/api/v1/write"
DB_QUERY_URL = "http://127.0.0.1:64210/api/v1/query/gremlin"
DB_WRITE_HEADERS = {'Content-type': 'application/json'}


def make_triples_for_vin(vin):
    pass

def make_triples_for_vout(tx):
    pass

def make_triples_for_tx(tx):
    tx_info = conn.getrawtransaction(tx, 1)
    tx_triples = []

    for v in tx_info['vin']:
        # Parse coinbase
        if 'coinbase' in v:
            tx_triples.append({
                "subject": tx_info['txid'],
                "predicate": 'coinbase',
                "object": v['coinbase']
            })
        else:
            # Parse non-coinbase
            tx_triples.append({
                "subject": tx_info['txid'],
                "predicate": "hex",
                "object": v['scriptSig']['hex']
            })
            for key in v:
                if (key != 'scriptSig' and key in DESIRED_TXIN_KEYS):
                    tx_triples.append({
                        "subject": v['scriptSig']['hex'],
                        "predicate": key,
                        "object": v[key]
                    })
    for v in tx_info['vout']:
        # unique ID for txouts
        tx_triples.append({
            "subject": str(tx_info['txid']),
            "predicate": "hex",
            "object": str(v['scriptPubKey']['hex'])
        })

        for key in v:
            # Append first level triples
            if (key != 'scriptPubKey' and key in DESIRED_TXOUT_KEYS):
                tx_triples.append({
                    "subject": str(v['scriptPubKey']['hex']),
                    "predicate": key,
                    "object": str(v[key])
                })
            else:
                # Append second level triples
                if (key == 'scriptPubKey'):
                    for k in v['scriptPubKey']:
                        if (k in DESIRED_TXOUT_KEYS and k != "hex"):
                            # Ignore the unique ID
                            if (k == "addresses"):
                                # Iterate through address array
                                for address in v['scriptPubKey'][k]:
                                    tx_triples.append({
                                        "subject": str(v['scriptPubKey']['hex']),
                                        "predicate": 'addresses',
                                        "object": str(address)
                                    })
                            else:
                                tx_triples.append({
                                    "subject": str(v['scriptPubKey']['hex']),
                                    "predicate": str(k),
                                    "object": str(v[k])
                                })
    return tx_triples

def make_triples_for_block(blocks):
    triples = []
    for block in blocks:
        for key in block:
            # Ignore self reference
            if (key == "hash"):
                continue
            # Iterate through transactions
            if (key == "tx"):
                for t in block[key]:
                    triples.append({
                        "subject": block['hash'],
                        "predicate": key,
                        "object": t
                    })
                    # Parse transactions and add to triples array
                    # tx_triples = make_triples_for_tx(t)
                    # for tx_triple in tx_triples:
                    #     triples.append(tx_triple)
            # Difficulty key value pair requires cleanup (value is always of form Decimal(foo)
            # Numbers cast to string for JSON POSTing
            # Append desired relations only
            if (key in DESIRED_BLOCK_KEYS):
                if (key == "difficulty"):
                    triples.append({
                        "subject": str(block['hash']),
                        "predicate": key,
                        "object": str(block[key])
                    })
                else:
                    triples.append({
                        "subject": str(block['hash']),
                        "predicate": key,
                        "object": str(block[key])
                    })

    return triples

def get_max_height_in_db():
    query = 'g.V().Out("height").All()'
    r = requests.post(DB_QUERY_URL, data=query, headers=DB_WRITE_HEADERS)
    result = r.json()
    max = 0
    for id in result['result']:
        if (int(id['id']) > max): max = int(id['id'])
    return max

def send_data(data):
    r = requests.post(DB_WRITE_URL, data=json.dumps(data), headers=DB_WRITE_HEADERS)
    pp(r)
    pp(r.text)

def make_string_triples(block_triples):
    clean_triples = []
    for block in block_triples:
        single_triple = {}
        for key in block:
            single_triple[key] = str(block[key])
        clean_triples.append(single_triple)
    return clean_triples

def main(start, end):
    pp("Starting from block " + str(start) + " to block " + str(end))
    commands = [ {"method": "getblockhash", "params": [height]} for height in range(int(start), int(end)) ]

    results = conn._batch(commands)
    block_hashes = [res['result'] for res in results]

    blocks = []
    for hash in block_hashes:
        blocks.append(conn.getblock(hash))

    block_triples = make_triples_for_block(blocks)
    pp(block_triples)
    clean_block_triples = make_string_triples(block_triples)

    send_data(clean_block_triples)

if __name__ == "__main__":
    conn = bitcoin.rpc.RawProxy()
    blockchain_info = conn._call("getinfo")
    best_block = blockchain_info['blocks']
    if len(sys.argv) > 1:
        if sys.argv[1] == "continue":
            # Subtract 1 in case previous filling didn't complete the last block
            start_point = get_max_height_in_db() - 1
            end_point = best_block
            curr_start = start_point
            if (end_point > 1000):
                curr_end = start_point + 1000
            else:
                curr_end = end_point
                main(curr_start, curr_end)

            while curr_end < end_point:
                main(curr_start, curr_end)
                if (end_point - curr_end > 1000):
                    curr_start += 1000
                    curr_end += 1000
                else:
                    curr_start += 1000
                    curr_end = end_point
                    main(curr_start, curr_end)
                    break
            pp("Job complete")

        if sys.argv[1] == "start":
            start_point = 0
            end_point = best_block
            curr_start = start_point
            if (end_point > 1000):
                curr_end = start_point + 1000
            else:
                curr_end = end_point
                main(curr_start, curr_end)

            while curr_end < end_point:
                main(curr_start, curr_end)
                if (end_point - curr_end > 1000):
                    curr_start += 1000
                    curr_end += 1000
                else:
                    curr_start += 1000
                    curr_end = end_point
                    main(curr_start, curr_end)
                    break
            pp("Job complete")

        if sys.argv[1] == "range":
            start_point = int(sys.argv[2])
            end_point = int(sys.argv[3])
            curr_start = start_point
            if (end_point > 1000):
                curr_end = start_point + 1000
            else:
                curr_end = end_point
                main(curr_start, curr_end)

            while curr_end < end_point:
                main(curr_start, curr_end)
                if (end_point - curr_end > 1000):
                    curr_start += 1000
                    curr_end += 1000
                else:
                    curr_start += 1000
                    curr_end = end_point
                    main(curr_start, curr_end)
                    break
            pp("Job complete")

    if len(sys.argv) == 1:
        pp("Please enter some arguments:")
        pp("    1. continue")
        pp("    2. start")
        pp("    3. range, start, end")