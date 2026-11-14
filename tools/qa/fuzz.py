"""Negative and odd-input probe of the API. Prints every case whose status is
5xx or differs from the expected class. Stdlib only."""
import http.client, json, sys, uuid

HOST, PORT = "127.0.0.1", 18080


def raw(method, path, body=None, headers=None):
    c = http.client.HTTPConnection(HOST, PORT, timeout=30)
    h = dict(headers or {})
    if body is not None and "Content-Type" not in h:
        h["Content-Type"] = "application/json"
    if isinstance(body, str):
        body = body.encode()
    try:
        c.request(method, path, body=body, headers=h)
        r = c.getresponse()
        data = r.read()
        return r.status, data[:400].decode(errors="replace")
    except Exception as e:  # connection reset etc.
        return -1, repr(e)
    finally:
        c.close()


ok = {"to": "+905551234567", "channel": "sms", "content": "hello"}
J = json.dumps
u = str(uuid.uuid4())
N = "/v1/notifications"
cases = [
    # (name, method, path, body, headers, expected statuses)
    ("valid", "POST", N, J(ok), {}, {201}),
    ("empty body", "POST", N, "", {}, {400}),
    ("not json", "POST", N, "{not json", {}, {400}),
    ("json array", "POST", N, "[]", {}, {400}),
    ("json null", "POST", N, "null", {}, {400}),
    ("wrong content type", "POST", N, J(ok), {"Content-Type": "text/plain"}, {400, 415}),
    ("no content type", "POST", N, J(ok), {"Content-Type": ""}, {400, 415}),
    ("unknown field", "POST", N, J({**ok, "bogus": 1}), {}, {400}),
    ("missing to", "POST", N, J({"channel": "sms", "content": "x"}), {}, {400}),
    ("missing channel", "POST", N, J({"to": "+905551234567", "content": "x"}), {}, {400}),
    ("missing content", "POST", N, J({"to": "+905551234567", "channel": "sms"}), {}, {400}),
    ("bad channel", "POST", N, J({**ok, "channel": "fax"}), {}, {400}),
    ("bad priority", "POST", N, J({**ok, "priority": "urgent"}), {}, {400}),
    ("to wrong type", "POST", N, J({**ok, "to": 5}), {}, {400}),
    ("content wrong type", "POST", N, J({**ok, "content": {"a": 1}}), {}, {400}),
    ("empty content", "POST", N, J({**ok, "content": ""}), {}, {400}),
    ("whitespace content", "POST", N, J({**ok, "content": "   "}), {}, {201, 400}),
    ("sms 1600 chars", "POST", N, J({**ok, "content": "a" * 1600}), {}, {201}),
    ("sms 1601 chars", "POST", N, J({**ok, "content": "a" * 1601}), {}, {400}),
    ("sms 1600 emoji (astral)", "POST", N, J({**ok, "content": "\U0001F600" * 1600}), {}, {201}),
    ("sms 1601 emoji", "POST", N, J({**ok, "content": "\U0001F600" * 1601}), {}, {400}),
    ("sms bad number", "POST", N, J({**ok, "to": "05551234567"}), {}, {400}),
    ("sms number too long", "POST", N, J({**ok, "to": "+" + "9" * 16}), {}, {400}),
    ("email no at", "POST", N, J({"to": "nobody", "channel": "email", "content": "x"}), {}, {400}),
    ("email 255 chars", "POST", N, J({"to": "a" * 243 + "@example.com", "channel": "email", "content": "x"}), {}, {400}),
    ("email crlf injection", "POST", N, J({"to": "a@b.co\r\nBcc: x@y.z", "channel": "email", "content": "x"}), {}, {400}),
    ("push empty token", "POST", N, J({"to": "", "channel": "push", "content": "x"}), {}, {400}),
    ("push 256 token", "POST", N, J({"to": "t" * 256, "channel": "push", "content": "x"}), {}, {400}),
    ("push 1025 content", "POST", N, J({"to": "tok", "channel": "push", "content": "x" * 1025}), {}, {400}),
    ("nul byte content", "POST", N, J({**ok, "content": "a\u0000b"}), {}, {201, 400}),
    ("lone surrogate", "POST", N, '{"to":"+905551234567","channel":"sms","content":"\\ud800"}', {}, {201, 400}),
    ("invalid utf8", "POST", N, b'{"to":"+905551234567","channel":"sms","content":"\xff\xfe"}', {}, {201, 400}),
    ("sql-ish content", "POST", N, J({**ok, "content": "'; DROP TABLE notifications; --"}), {}, {201}),
    ("content and template", "POST", N, J({**ok, "template": {"name": "x", "variables": {}}}), {}, {400}),
    ("unknown template", "POST", N, J({"to": "+905551234567", "channel": "sms", "template": {"name": "nope-" + u, "variables": {}}}), {}, {400}),
    ("schedule in past", "POST", N, J({**ok, "scheduledAt": "2001-01-01T00:00:00Z"}), {}, {201, 400}),
    ("schedule far future", "POST", N, J({**ok, "scheduledAt": "2999-01-01T00:00:00Z"}), {}, {400}),
    ("schedule not a date", "POST", N, J({**ok, "scheduledAt": "tomorrow"}), {}, {400}),
    ("schedule with offset", "POST", N, J({**ok, "scheduledAt": "2026-11-20T10:00:00+03:00"}), {}, {201}),
    ("idem key empty", "POST", N, J(ok), {"Idempotency-Key": ""}, {201, 400}),
    ("idem key 300 chars", "POST", N, J(ok), {"Idempotency-Key": "k" * 300}, {400}),
    ("idem key non-ascii", "POST", N, J(ok), {"Idempotency-Key": "anahtar-ü".encode("utf-8").decode("latin-1")}, {400}),
    ("correlation id huge", "POST", N, J(ok), {"X-Correlation-ID": "c" * 5000}, {201, 400, 431}),
    ("correlation id odd chars", "POST", N, J(ok), {"X-Correlation-ID": "a b;c\"d"}, {201, 400}),
    ("bad traceparent", "POST", N, J(ok), {"traceparent": "garbage"}, {201}),
    ("deep nesting", "POST", N, "[" * 100000 + "]" * 100000, {}, {400}),
    ("huge number", "POST", N, '{"to":"+905551234567","channel":"sms","content":"x","priority":1e999}', {}, {400}),
    ("duplicate keys", "POST", N, '{"to":"+905551234567","channel":"sms","content":"a","content":"b"}', {}, {201, 400}),
    ("batch empty", "POST", N + "/batch", J({"notifications": []}), {}, {400}),
    ("batch missing key", "POST", N + "/batch", "{}", {}, {400}),
    ("batch not array", "POST", N + "/batch", J({"notifications": ok}), {}, {400}),
    ("batch 1001", "POST", N + "/batch", J({"notifications": [ok] * 1001}), {}, {400}),
    ("batch null item", "POST", N + "/batch", J({"notifications": [ok, None]}), {}, {400}),
    ("batch one bad of 3", "POST", N + "/batch", J({"notifications": [ok, {**ok, "channel": "fax"}, ok]}), {}, {400}),
    ("get bad id", "GET", N + "/not-a-uuid", None, {}, {400, 404}),
    ("get unknown id", "GET", N + "/" + u, None, {}, {404}),
    ("get id with sql", "GET", N + "/1%27%20OR%20%271%27=%271", None, {}, {400, 404}),
    ("cancel unknown", "POST", N + "/" + u + "/cancel", None, {}, {404}),
    ("retry unknown", "POST", N + "/" + u + "/retry", None, {}, {404}),
    ("batch get unknown", "GET", "/v1/batches/" + u, None, {}, {404}),
    ("batch get bad id", "GET", "/v1/batches/xyz", None, {}, {400, 404}),
    ("list limit 0", "GET", N + "?limit=0", None, {}, {400}),
    ("list limit -1", "GET", N + "?limit=-1", None, {}, {400}),
    ("list limit huge", "GET", N + "?limit=100000", None, {}, {400}),
    ("list limit text", "GET", N + "?limit=abc", None, {}, {400}),
    ("list bad status", "GET", N + "?status=nope", None, {}, {400}),
    ("list bad channel", "GET", N + "?channel=fax", None, {}, {400}),
    ("list bad cursor", "GET", N + "?cursor=!!!notbase64", None, {}, {400}),
    ("list forged cursor", "GET", N + "?cursor=eyJpZCI6IjEnIE9SICcxJz0nMSJ9", None, {}, {400}),
    ("list bad date", "GET", N + "?createdFrom=yesterday", None, {}, {400}),
    ("list from after to", "GET", N + "?createdFrom=2026-12-01T00:00:00Z&createdTo=2026-11-01T00:00:00Z", None, {}, {200, 400}),
    ("list bad batch id", "GET", N + "?batchId=zzz", None, {}, {400}),
    ("list repeated param", "GET", N + "?status=sent&status=failed", None, {}, {200, 400}),
    ("metrics window 0", "GET", "/v1/metrics?windowSeconds=0", None, {}, {400}),
    ("metrics window huge", "GET", "/v1/metrics?windowSeconds=999999", None, {}, {400}),
    ("metrics window text", "GET", "/v1/metrics?windowSeconds=x", None, {}, {400}),
    ("template bad name", "PUT", "/v1/templates/" + "n" * 300, J({"body": "x"}), {}, {400, 404}),
    ("template name with slash", "PUT", "/v1/templates/a%2Fb", J({"body": "x"}), {}, {400, 404}),
    ("template empty body", "PUT", "/v1/templates/qa-empty", J({"body": ""}), {}, {400}),
    ("template unbalanced", "PUT", "/v1/templates/qa-odd", J({"body": "Hi {{name"}), {}, {200, 201, 204, 400}),
    ("template huge body", "PUT", "/v1/templates/qa-huge", J({"body": "x" * 200001}), {}, {400}),
    ("template get unknown", "GET", "/v1/templates/nope-" + u, None, {}, {404}),
    ("template delete unknown", "DELETE", "/v1/templates/nope-" + u, None, {}, {204, 404}),
    ("unknown path", "GET", "/v1/nope", None, {}, {404}),
    ("wrong method", "DELETE", N, None, {}, {404, 405}),
    ("path traversal", "GET", "/v1/../../etc/passwd", None, {}, {400, 404, 301, 307}),
    ("ws without upgrade", "GET", "/v1/ws", None, {}, {400, 426}),
    ("ws bad id", "GET", "/v1/ws?notificationId=zzz", None, {"Connection": "Upgrade", "Upgrade": "websocket", "Sec-WebSocket-Version": "13", "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="}, {400, 404}),
    ("healthz", "GET", "/healthz", None, {}, {200}),
    ("readyz", "GET", "/readyz", None, {}, {200}),
    ("openapi", "GET", "/openapi.json", None, {}, {200}),
    ("docs", "GET", "/docs", None, {}, {200}),
]

bad = 0
for name, method, path, body, headers, want in cases:
    st, text = raw(method, path, body, headers)
    flag = ""
    if st >= 500 or st == -1:
        flag = "SERVER ERROR"
    elif st not in want:
        flag = "UNEXPECTED"
    if flag:
        bad += 1
        print(f"{flag:12} {name}: {st} (want {sorted(want)}) {text[:220]!r}")
    elif "-v" in sys.argv:
        print(f"ok           {name}: {st} {text[:100]!r}")
# 9 MiB body: over the documented 8 MiB limit
st, text = raw("POST", N, b'{"to":"+905551234567","channel":"email","content":"' + b"x" * (9 << 20) + b'"}')
print("9 MiB body:", st, text[:120])
print(f"{len(cases)} cases, {bad} flagged")
