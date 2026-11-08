#!/bin/zsh
set -e
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
BH=$BIN/blackhole
SV=$BIN/blackhole-server
W=$(mktemp -d /tmp/bh-server.XXXXXX)
export BLACKHOLE_ROOT=$W/root
cd $W
$BH init > /dev/null
OWNER=$($BH drive create fleet | sed 's/.*: //')
$SV --addr 127.0.0.1:9904 > $W/server.log 2>&1 &
SVPID=$!; trap "kill $SVPID 2>/dev/null" EXIT
for i in $(seq 1 30); do curl -s http://127.0.0.1:9904/healthz > /dev/null 2>&1 && break; sleep 0.2; done
head -1 $W/server.log
U=http://127.0.0.1:9904
echo "== key API: mint a scoped key for run-7 with the admin key"
R7J=$(curl -s -X POST $U/api/keys -H "Authorization: Bearer $OWNER" -H 'content-type: application/json' -d '{"drive":"fleet","principal":"run-7","prefix":"/","ttl_seconds":3600}')
R7=$(echo "$R7J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["secret"])')
R7ID=$(echo "$R7J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
echo "run-7 key minted: ${R7:0:8}…"
curl -s -X POST $U/api/keys -H "Authorization: Bearer $R7" -H 'content-type: application/json' -d '{"drive":"fleet","principal":"x"}' ; echo "  <- non-admin key refused"
echo "== SSE feed in the background"
curl -s -N "$U/feed/fleet?after=0" -H "Authorization: Bearer $OWNER" > $W/feed.txt &
FEEDPID=$!
sleep 0.5
echo "== S3 door through the same port, as run-7"
export AWS_ACCESS_KEY_ID=$R7ID AWS_SECRET_ACCESS_KEY=$R7 AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
echo "first draft" > a.txt
aws --endpoint-url $U s3 cp a.txt s3://fleet/out/a.txt --metadata intent="run-7 writes a" --no-progress
aws --endpoint-url $U s3 ls s3://fleet/out/
echo "== MCP over HTTP with a bearer key (streamable http)"
python3 - "$U" "$OWNER" "$R7" <<'PY'
import json, sys, urllib.request
base, owner, r7 = sys.argv[1], sys.argv[2], sys.argv[3]
def call(key, method, params=None, sid=None, id=1):
    body = {"jsonrpc":"2.0","id":id,"method":method}
    if params is not None: body["params"] = params
    req = urllib.request.Request(base + "/mcp", data=json.dumps(body).encode(), method="POST")
    req.add_header("content-type","application/json")
    req.add_header("accept","application/json, text/event-stream")
    req.add_header("authorization", f"Bearer {key}")
    if sid: req.add_header("mcp-session-id", sid)
    with urllib.request.urlopen(req) as r:
        sid = r.headers.get("mcp-session-id", sid)
        raw = r.read().decode()
    msgs = []
    for line in raw.splitlines():
        if line.startswith("data:") and line[5:].strip(): msgs.append(json.loads(line[5:].strip()))
    if not msgs and raw.strip(): msgs.append(json.loads(raw))
    return sid, msgs
sid, m = call(owner, "initialize", {"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}})
print("initialize ->", m[0]["result"]["serverInfo"]["name"], "session", (sid or "")[:8])
req = urllib.request.Request(base + "/mcp", data=json.dumps({"jsonrpc":"2.0","method":"notifications/initialized"}).encode(), method="POST")
req.add_header("content-type","application/json"); req.add_header("accept","application/json, text/event-stream"); req.add_header("authorization", f"Bearer {owner}"); req.add_header("mcp-session-id", sid)
urllib.request.urlopen(req).read()
def tool(key, name, args, id):
    _, m = call(key, "tools/call", {"name":name,"arguments":args}, sid, id)
    r = m[0].get("result") or m[0].get("error")
    txt = "".join(c.get("text","") for c in r.get("content", [])) if isinstance(r, dict) and "content" in r else json.dumps(r)
    return ("ERROR " if isinstance(r, dict) and r.get("isError") else "") + txt.strip()
print("read as owner  ->", tool(owner, "read", {"drive":"fleet","path":"/out/a.txt"}, 2).splitlines()[-1])
print("write as run-7 ->", tool(r7, "write", {"drive":"fleet","path":"/out/b.txt","content":"from mcp over http\n","intent":"run-7 via mcp"}, 3))
print("history        ->", tool(owner, "history", {"drive":"fleet","path":"/out/a.txt"}, 4))
print("no key         ->", tool("bogus", "list", {"drive":"fleet"}, 5)[:60])
PY
echo "== undo run-7 via the CLI against the same root; feed must show it"
$BH undo fleet --by run-7 --since 1h --key $OWNER | head -1
sleep 0.8
kill $FEEDPID 2>/dev/null || true
echo "-- feed events received:"
grep -c '^event: commit' $W/feed.txt | awk '{print "   commit events:", $1}'
grep '^data:' $W/feed.txt | python3 -c 'import sys,json; [print("  ", (d:=json.loads(l[5:]))["seq"], d["actor"], d["intent"], [p["path"] for p in d["paths"]]) for l in sys.stdin]'
echo "== attest and verify over HTTP"
curl -s $U/api/attest/fleet -H "Authorization: Bearer $OWNER"; echo
curl -s $U/api/verify/fleet -H "Authorization: Bearer $OWNER" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("  verify:", d["commits"], "commits,", d["bad"], "bad")'
echo "workspace: $W"
