"""BoardReady server: python server.py  ->  http://localhost:8765"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pipeline

ROOT = pipeline.ROOT
RUNS = pipeline.RUNS
LOCK = threading.Lock()  # one write (approve) at a time


def safe_run_dir(run_id):
    if not run_id or "/" in run_id or "\\" in run_id or ".." in run_id:
        return None
    d = os.path.join(RUNS, run_id)
    return d if os.path.isdir(d) else None


def recent_calls(run_dir, n=12):
    out = []
    for name in ("stream_a.jsonl", "stream_b.jsonl"):
        p = os.path.join(run_dir, name)
        if not os.path.exists(p):
            continue
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    ev = json.loads(line)
                except ValueError:
                    continue
                if ev.get("type") == "assistant":
                    for b in (ev.get("message") or {}).get("content") or []:
                        if b.get("type") == "tool_use" and b.get("name") != "ToolSearch":
                            out.append({"run": name[7].upper(), "tool": pipeline.short_name(b["name"]),
                                        "input_summary": pipeline.summarize_input(b.get("input") or {})})
    return out[-n:], len(out)


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path in ("/", "/index.html"):
            with open(os.path.join(ROOT, "index.html"), "rb") as f:
                return self.send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/api/status":
            d = safe_run_dir(q.get("run_id"))
            if not d:
                return self.send(404, {"error": "no such run"})
            st = {}
            if os.path.exists(os.path.join(d, "status.json")):
                with open(os.path.join(d, "status.json"), encoding="utf-8") as f:
                    st = json.load(f)
            calls, total = recent_calls(d)
            return self.send(200, {**st, "recent_tool_calls": calls, "tool_calls_n": total})
        if u.path == "/api/run":
            d = safe_run_dir(q.get("run_id"))
            if not d or not os.path.exists(os.path.join(d, "run.json")):
                return self.send(404, {"error": "run not ready"})
            with open(os.path.join(d, "run.json"), "rb") as f:
                return self.send(200, f.read())
        if u.path == "/api/runs":
            runs = []
            for r in sorted(os.listdir(RUNS), reverse=True):
                p = os.path.join(RUNS, r, "run.json")
                if os.path.exists(p):
                    with open(p, encoding="utf-8") as f:
                        j = json.load(f)
                    runs.append({"run_id": r, "month": j.get("month"),
                                 "verified": f"{j['verification']['verified_n']}/{j['verification']['total_n']}"})
            runs.sort(key=lambda x: x["run_id"] != "demo")  # demo first, then newest
            return self.send(200, runs)
        if u.path == "/api/audit":
            out = []
            if os.path.exists(pipeline.AUDIT):
                with open(pipeline.AUDIT, encoding="utf-8") as f:
                    out = [json.loads(l) for l in f if l.strip()]
            return self.send(200, out[::-1])
        if u.path == "/api/default_month":
            return self.send(200, {"month": pipeline.default_month()})
        self.send(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        try:
            b = self.body()
        except ValueError:
            return self.send(400, {"error": "bad json"})
        if u.path == "/api/run":
            ym = b.get("month") or pipeline.default_month()
            import datetime as dt
            run_id = dt.datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + ym
            os.makedirs(os.path.join(RUNS, run_id), exist_ok=True)
            pipeline.set_status(os.path.join(RUNS, run_id), stage="starting", month=ym)
            threading.Thread(target=lambda: pipeline.run_pipeline(ym, run_id), daemon=True).start()
            return self.send(200, {"run_id": run_id})
        if u.path == "/api/approve":
            if not safe_run_dir(b.get("run_id")):
                return self.send(404, {"error": "no such run"})
            try:
                with LOCK:
                    res = pipeline.approve(b["run_id"], b.get("transaction_ids") or [],
                                           b.get("account_code"), b.get("tax_rate_code"))
                return self.send(200, res)
            except Exception as e:
                return self.send(400, {"error": str(e)})
        if u.path == "/api/skip":
            d = safe_run_dir(b.get("run_id"))
            if not d:
                return self.send(404, {"error": "no such run"})
            with LOCK:
                p = os.path.join(d, "run.json")
                with open(p, encoding="utf-8") as f:
                    run = json.load(f)
                for e in run["exceptions"]:
                    if e["transaction_id"] == b.get("transaction_id"):
                        e["status"] = "skipped"
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(run, f, indent=1)
            return self.send(200, {"ok": True})
        self.send(404, {"error": "not found"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8765))
    print(f"BoardReady on http://localhost:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
