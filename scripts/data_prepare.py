#!/usr/bin/env python

import argparse
import json
import os

PARSER = argparse.ArgumentParser()
PARSER.add_argument("-is", "--input-suricata", dest="input_suricata", help="Path to suricata EVE file")
PARSER.add_argument("-iw", "--input-winlog", dest="input_winlog", help="Path to windows sysmon file")
PARSER.add_argument("-o", "--output", dest="output", help="Path to output folder")

ARGS = PARSER.parse_args()

INTERESTING_CODES = ["1", "3"]

def dump(items: list[dict], p: str) -> None:
    out = os.path.join(ARGS.output, p)
    print(f"writing relevant logs to {out}")
    with open(out, "w") as handle:
        for line in items:
            handle.write(json.dumps(line))
            handle.write("\n")

if __name__ == "__main__":
    print(f"first pass to extract community_id and entity ID values from {ARGS.input_winlog}")
    community_id = {}
    with open(ARGS.input_winlog, "r") as handle:
        for line in handle:
            log = json.loads(line)
            code = log.get("event", {}).get("code", None)
            if code == "3":
                community_id[log["network"]["community_id"]] = log["process"]["entity_id"]

    print(f"got {len(community_id)} items")

    print(f"second pass to extract related Suricata events from {ARGS.input_suricata}")
    relevant_suricata = []
    with open(ARGS.input_suricata, "r") as handle:
        for line in handle:
            log = json.loads(line)
            if log["community_id"] in community_id:
                relevant_suricata.append(log)

    print(f"got {len(relevant_suricata)} relevant suricata messages")

    print("cross referencing suricata and winlog")
    relevant_suricata_cid = set([e["community_id"] for e in relevant_suricata])
    community_id_xref = {}
    for k, v in community_id.items():
        if k in relevant_suricata_cid:
            community_id_xref[k] = v

    print(f"validated {len(community_id_xref)} values")

    print(f"third pass to extract all relevant winlog events from {ARGS.input_winlog}")
    set_entity_id = set(community_id_xref.values())
    relevant_winlog = []
    with open(ARGS.input_winlog, "r") as handle:
        for line in handle:
            log = json.loads(line)
            if "process" in log and log["process"]["entity_id"] in set_entity_id and log["event"]["code"] in INTERESTING_CODES:
                relevant_winlog.append(log)

    print(f"got {len(relevant_winlog)} relevant winlog messages")

    dump(relevant_suricata, "suricata.json")
    dump(relevant_winlog, "winlog.json")
