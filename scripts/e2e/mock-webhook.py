#!/usr/bin/env python3
"""Recording webhook receiver for the e2e suite.

Appends one line per received POST body to $RECORD as a JSON envelope:

    {"n": 0, "status": 500, "lines": ["{...}", "{...}"]}

`n` is the request index, `status` what we answered, and `lines` the NDJSON
lines of the body. That is enough to assert delivery, chunk sizes, retry
behaviour, and rollback forwarding without parsing a server log.

Env: PORT (8801), FAIL_FIRST (0 = answer 200 immediately; N = answer 500 to the
     first N requests, then 200 — drives the engine's retry path),
     FAIL_ALWAYS (1 = answer 500 forever, so the branch pauses),
     RECORD (path; default ./webhook.ndjson).
"""
import json, os, threading
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.environ.get("PORT", 8801))
FAIL_FIRST = int(os.environ.get("FAIL_FIRST", 0))
FAIL_ALWAYS = os.environ.get("FAIL_ALWAYS", "0") == "1"
RECORD = os.environ.get("RECORD", "webhook.ndjson")

lock = threading.Lock()
count = 0


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        global count
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n).decode("utf-8", "replace")
        with lock:
            i = count
            count += 1
            status = 500 if (FAIL_ALWAYS or i < FAIL_FIRST) else 200
            lines = [l for l in body.split("\n") if l.strip()]
            with open(RECORD, "a") as f:
                f.write(json.dumps({"n": i, "status": status, "lines": lines}) + "\n")
        self._send(status, b'{"ok":true}' if status == 200 else b'{"error":"nope"}')

    def do_GET(self):
        # Lets a test read the count without parsing the record file.
        with lock:
            self._send(200, json.dumps({"count": count}).encode())

    def _send(self, status, body):
        try:
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the pipeline was killed mid-response — expected in crash tests


if __name__ == "__main__":
    open(RECORD, "a").close()
    HTTPServer(("127.0.0.1", PORT), H).serve_forever()
