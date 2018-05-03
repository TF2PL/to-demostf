#!/usr/bin/env python3

import traceback
import logging
import requests
import collections
import json
import gzip
import shutil
import io
import os

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
    )

DEMOSTF_API_KEY = os.environ['DEMOSTF_API_KEY']
FACEIT_API_KEY = os.environ['FACEIT_API_KEY']

Match = collections.namedtuple("Match", "id, hub_name, demo_url, faction1, faction2")


def get_new_matches(hub):
    logging.info(f"Hub {hub['name']}")
    new_matches = collections.deque()
    offset = 0
    while True:
        logging.info(f"  getting page {offset // 10}")
        matches_request = requests.get(
                f'https://open.faceit.com/data/v4/hubs/{hub["id"]}/matches',
                headers=dict(authorization=f"Bearer {FACEIT_API_KEY}"),
                params=dict(
                    type="past",
                    offset=offset,
                    limit=10,
                ),
            )
        matches_request.raise_for_status()
        logging.info(f"    {matches_request.headers['X-RateLimit-Remaining-hour']} requests available")
        matches = matches_request.json()['items']
        if not matches:
            logging.info("  reached end of matches available")
            break
        for match in matches:
            if match['match_id'] == hub['last']:
                logging.info("  found last match uploaded")
                break
            elif match['status'] == "FINISHED":
                new_matches.appendleft(
                        Match(
                            match['match_id'],
                            hub['name'],
                            match['demo_url'][0],
                            match['teams']['faction1']['name'],
                            match['teams']['faction2']['name'],
                        )
                    )
        else:
            offset += 10
            continue
        break
    return new_matches


def upload_match(match):
    logging.info(f"Match in {match.hub_name}: {match.id} ({match.faction1} vs {match.faction2})")
    logging.info("  requesting demo")
    demo_request = requests.get(match.demo_url, stream=True)
    demo_request.raise_for_status()
    uncompressed = gzip.GzipFile(fileobj=demo_request.raw)
    logging.info("  patching gunzipped demo")
    patched = io.BytesIO(uncompressed.read())
    patched.getbuffer()[16:276] = (f"TF2PL.com {match.hub_name}, powered by FACEIT.com".encode('ascii') + (b"\x00" * 260))[:260]
    logging.info("  uploading to demos.tf")
    upload_request = requests.post(
            "https://api.demos.tf/upload",
            data=dict(
                key=DEMOSTF_API_KEY
                name=f"https://faceit.com/en//room/{match.id}.dem",
                blu=match.faction1,
                red=match.faction2,
            ),
            files=dict(
                demo=patched,
            ),
        )
    logging.info(f"  uploaded, got {upload_request.status_code}: {upload_request.text}")


def main():
    with open("data.json") as f:
        data = json.load(f)
    for hub in data:
        try:
            new_matches = get_new_matches(hub)
            for match in new_matches:
                upload_match(match)
                hub['last'] = match.id
        except Exception as e:
            logging.error("Ran into an exception")
            traceback.print_exc()
            pass
    with open("data.json", 'w') as f:
        json.dump(data, f)


if __name__ == "__main__":
    main()
