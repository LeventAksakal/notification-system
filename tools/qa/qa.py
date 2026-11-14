"""QA helper for the compose stack (project notifier-qa). Stdlib only.

usage: python qa.py <command> [args]
  reset                      clear provider counters and faults
  load N [prio]              queue N per channel (batches of 1000), print batch ids
  wait [timeout_s]           wait until queueDepth is 0, print time
  stats                      provider stats + API metrics totals
  dedupe on|off
"""
import json, sys, time, urllib.request, urllib.error, uuid

API = "http://127.0.0.1:18080"
PROV = "http://127.0.0.1:18090"


def call(method, url, body=None, headers=None, timeout=60):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw.decode(errors="replace")


TO = {"sms": "+905551234567", "email": "ada@example.com", "push": "device-token-1"}


def load(n, prio=None, tag="qa"):
    ids = []
    for ch in ("sms", "email", "push"):
        left = n
        while left > 0:
            k = min(left, 1000)
            items = []
            for i in range(k):
                it = {"to": TO[ch], "channel": ch, "content": f"{tag} {ch} {left - i}"}
                if prio:
                    it["priority"] = prio if prio != "mix" else ("high", "normal", "low")[i % 3]
                items.append(it)
            st, body = call("POST", API + "/v1/notifications/batch", {"notifications": items},
                            {"Idempotency-Key": f"{tag}-{uuid.uuid4()}"})
            if st != 201:
                print("batch failed", st, str(body)[:300])
                sys.exit(1)
            ids.append(body["id"])
            left -= k
    return ids


def metrics():
    return call("GET", API + "/v1/metrics?windowSeconds=3600")[1]


def wait(timeout=300):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        st, m = call("GET", API + "/v1/metrics?windowSeconds=3600")
        if st == 200:
            d = m["totals"]["pending"] + m["totals"]["processing"]
            if d != last:
                last = d
            if d == 0:
                print(f"drained in {time.time() - t0:.1f}s; totals {m['totals']}")
                return True
        time.sleep(0.5)
    print(f"NOT drained after {timeout}s; depth {last}; totals {metrics()['totals']}")
    return False


def stats():
    st, s = call("GET", PROV + "/_control/stats")
    print(json.dumps(s, indent=1))
    m = metrics()
    print("api totals", m["totals"], "queueDepth", m["queueDepth"])
    print("api delivery", m["delivery"])


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "reset":
        print(call("POST", PROV + "/_control/reset"), call("DELETE", PROV + "/_control/faults"))
    elif cmd == "load":
        n = int(sys.argv[2])
        t0 = time.time()
        ids = load(n, sys.argv[3] if len(sys.argv) > 3 else None, tag=f"qa{int(t0)}")
        print(f"queued 3x{n} in {time.time() - t0:.1f}s; batches {len(ids)}")
    elif cmd == "wait":
        sys.exit(0 if wait(int(sys.argv[2]) if len(sys.argv) > 2 else 300) else 1)
    elif cmd == "stats":
        stats()
    elif cmd == "dedupe":
        print(call("PUT", PROV + "/_control/dedupe", {"enabled": sys.argv[2] == "on"}))
