import json, subprocess, sys, threading, time, urllib.request, urllib.error, datetime
API="http://127.0.0.1:18080"
at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
N=24
def body(i): return json.dumps({"notifications":[{"to":"device-%d-%d"%(i,j),"channel":"push","content":"apikill %d %d"%(i,j),"scheduledAt":at} for j in range(1000)]}).encode()
def post(i, out):
    req=urllib.request.Request(API+"/v1/notifications/batch", data=body(i), method="POST", headers={"Content-Type":"application/json","Idempotency-Key":"apikill-%s-%d"%(sys.argv[1],i)})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d=json.loads(r.read()); out[i]=(r.status, d["id"], r.headers.get("Idempotent-Replayed"))
    except urllib.error.HTTPError as e:
        out[i]=(e.code, e.read()[:120].decode(errors="replace"), None)
    except Exception as e:
        out[i]=(-1, type(e).__name__, None)
def scheduled():
    with urllib.request.urlopen(API+"/v1/metrics", timeout=30) as r: return json.loads(r.read())["totals"]["scheduled"]
before=scheduled()
first={}
ts=[threading.Thread(target=post,args=(i,first)) for i in range(N)]
for t in ts: t.start()
time.sleep(0.25)
subprocess.run(["docker","kill","-s","KILL","notifier-qa-app-1"],capture_output=True)
for t in ts: t.join()
ok=[i for i in first if first[i][0]==201]
print("first pass: %d of %d answered 201; others: %s"%(len(ok),N,sorted(set(first[i][:2] for i in first if first[i][0]!=201))[:4]))
subprocess.run(["docker","start","notifier-qa-app-1"],capture_output=True)
for _ in range(60):
    try:
        if urllib.request.urlopen(API+"/readyz",timeout=2).status==200: break
    except Exception: time.sleep(0.5)
second={}
for i in range(N): post(i, second)
print("retry pass: statuses", sorted(set(second[i][0] for i in second)), "replayed", sum(1 for i in second if second[i][2]=="true"))
same=sum(1 for i in ok if first[i][1]==second[i][1])
print("batches answered before the kill that got the same id on retry: %d of %d"%(same,len(ok)))
after=scheduled()
print("scheduled rows added: %d (want %d)"%(after-before, N*1000))
