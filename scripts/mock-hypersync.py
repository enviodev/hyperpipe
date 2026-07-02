#!/usr/bin/env python3
"""Paginating mock HyperSync server for local demos + the crash test.

Serves a deterministic USDC Transfer log for every block in [FROM, END): one log
per block, log_index 0, value == block number. Paginates STEP blocks per query
with a small DELAY so a backfill takes long enough to interrupt.

Env: PORT (8799), FROM (19000000), END (19000300), STEP (10), DELAY (0.12).
"""
import json, os, time
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.environ.get("PORT", 8799))
FROM = int(os.environ.get("FROM", 19000000))
END = int(os.environ.get("END", 19000300))
STEP = int(os.environ.get("STEP", 10))
DELAY = float(os.environ.get("DELAY", 0.12))

TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
FROM_ADDR = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TO_ADDR = "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def log_for(block):
    return {
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "topic0": TOPIC0, "topic1": FROM_ADDR, "topic2": TO_ADDR,
        "data": "0x" + format(block, "064x"),  # value = block number
        "block_number": block, "log_index": 0,
        "transaction_hash": "0x" + format(block, "064x"),
    }


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        self._send({"height": END})

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        req = json.loads(self.rfile.read(n) or b"{}")
        frm = int(req.get("from_block", FROM))
        req_to = int(req.get("to_block", END))
        end = min(req_to, END)
        to = min(frm + STEP, end)
        time.sleep(DELAY)
        logs = [log_for(b) for b in range(frm, to)] if frm < end else []
        blocks = [{"number": b, "timestamp": 1700000000 + b, "hash": "0x" + format(b, "064x")}
                  for b in range(frm, to)] if frm < end else []
        self._send({
            "archive_height": END,
            "next_block": max(to, frm),
            "data": {"blocks": blocks, "logs": logs},
        })

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
