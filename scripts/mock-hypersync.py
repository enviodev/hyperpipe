#!/usr/bin/env python3
"""Paginating mock HyperSync server for local demos + the crash test.

Serves a deterministic USDC Transfer log for every block in [FROM, END): one log
per block, log_index 0, value == block number. Paginates STEP blocks per query
with a small DELAY so a backfill takes long enough to interrupt. Every /query
response carries a `rollback_guard` like the real server.

Reorg simulation: set REORG_AT to a block number. Once the pipeline's cursor
passes it, blocks >= REORG_AT - REORG_DEPTH switch to a new fork (different
hashes, different tx hashes) — the pipeline should detect the mismatch via the
rollback guard, emit a rollback control record, rewind, and re-ingest the
forked blocks.

Env: PORT (8799), FROM (19000000), END (19000300), STEP (10), DELAY (0.12),
     REORG_AT (0 = off), REORG_DEPTH (3).
"""
import json, os, time
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.environ.get("PORT", 8799))
FROM = int(os.environ.get("FROM", 19000000))
END = int(os.environ.get("END", 19000300))
STEP = int(os.environ.get("STEP", 10))
DELAY = float(os.environ.get("DELAY", 0.12))
REORG_AT = int(os.environ.get("REORG_AT", 0))
REORG_DEPTH = int(os.environ.get("REORG_DEPTH", 3))

TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
FROM_ADDR = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TO_ADDR = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

reorged = False  # flips once the cursor passes REORG_AT
FORK = max(REORG_AT - REORG_DEPTH, 0)


def on_new_fork(block):
    return reorged and REORG_AT and block >= FORK


def block_hash(block):
    # New-fork blocks hash differently — this is what the guard check catches.
    tag = "beef" if on_new_fork(block) else "0000"
    return "0x" + tag + format(block, "060x")


def log_for(block):
    tx_tag = "beef" if on_new_fork(block) else ""
    return {
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "topic0": TOPIC0, "topic1": FROM_ADDR, "topic2": TO_ADDR,
        "data": "0x" + format(block, "064x"),  # value = block number
        "block_number": block, "log_index": 0,
        "transaction_hash": "0x" + tx_tag + format(block, "064x")[len(tx_tag):],
    }


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        self._send({"height": END})

    def do_POST(self):
        global reorged
        n = int(self.headers.get("content-length", 0))
        req = json.loads(self.rfile.read(n) or b"{}")
        frm = int(req.get("from_block", FROM))
        req_to = int(req.get("to_block", END))
        end = min(req_to, END)
        to = min(frm + STEP, end)
        time.sleep(DELAY)
        # Trip the reorg once the cursor has moved past REORG_AT.
        if REORG_AT and not reorged and frm >= REORG_AT:
            reorged = True
        want_logs = bool(req.get("logs")) or not req.get("include_all_blocks")
        logs = [log_for(b) for b in range(frm, to)] if (frm < end and want_logs) else []
        blocks = [{"number": b, "timestamp": 1700000000 + b, "hash": block_hash(b)}
                  for b in range(frm, to)] if frm < end else []
        resp = {
            "archive_height": END,
            "next_block": max(to, frm),
            "data": {"blocks": blocks, "logs": logs},
        }
        if frm < to:
            resp["rollback_guard"] = {
                "block_number": to - 1,
                "timestamp": 1700000000 + to - 1,
                "hash": block_hash(to - 1),
                "first_block_number": frm,
                "first_parent_hash": block_hash(frm - 1),
            }
        self._send(resp)

    def _send(self, obj):
        body = json.dumps(obj).encode()
        try:
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # client (pipeline) was killed mid-response — expected in crash test


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", PORT), H).serve_forever()
