import json
import os
import subprocess
import sys
import tempfile

BIN = os.environ.get("BIN") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "target", "release")
BH = os.path.join(BIN, "blackhole")
MCP = os.path.join(BIN, "blackhole-mcp")

root = tempfile.mkdtemp(prefix="bh-mcp-")
env = dict(os.environ, BLACKHOLE_ROOT=root)
subprocess.run([BH, "init"], env=env, check=True, capture_output=True)
out = subprocess.run([BH, "drive", "create", "notes"], env=env, check=True, capture_output=True, text=True).stdout
owner = out.strip().split(": ")[-1]
a7 = subprocess.run([BH, "key", "create", "notes", "--principal", "agent-7", "--key", owner], env=env, check=True, capture_output=True, text=True).stdout.strip()

proc = subprocess.Popen([MCP, "--root", root, "--key", owner], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr, text=True, bufsize=1)
next_id = [0]


def rpc(method, params=None, notify=False):
    msg = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        msg["params"] = params
    if not notify:
        next_id[0] += 1
        msg["id"] = next_id[0]
    proc.stdin.write(json.dumps(msg) + "\n")
    proc.stdin.flush()
    if notify:
        return None
    while True:
        line = proc.stdout.readline()
        if not line:
            raise SystemExit("server closed")
        resp = json.loads(line)
        if resp.get("id") == msg["id"]:
            return resp


def call(name, **args):
    resp = rpc("tools/call", {"name": name, "arguments": args})
    if "error" in resp:
        return f"RPC ERROR {resp['error']}"
    r = resp["result"]
    txt = "".join(c.get("text", "") for c in r.get("content", []))
    return ("ERROR " if r.get("isError") else "") + txt.rstrip()


init = rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "smoke", "version": "0"}})
print("server:", init["result"]["serverInfo"], "protocol", init["result"]["protocolVersion"])
rpc("notifications/initialized", notify=True)
tools = rpc("tools/list")["result"]["tools"]
print("tools:", ", ".join(sorted(t["name"] for t in tools)))
print()

steps = [
    ("drives", {}),
    ("write", dict(drive="notes", path="/docs/a.txt", content="hello world\n", intent="owner seeds a")),
    ("write", dict(drive="notes", path="/docs/b.txt", content="line1\nline2\n", intent="owner seeds b")),
    ("write", dict(drive="notes", path="/docs/a.txt", content="agent seven was here\n", intent="agent-7 rewrites a", key=a7)),
    ("write", dict(drive="notes", path="/docs/c.txt", content="agent seven new file\n", intent="agent-7 adds c", key=a7)),
    ("delete", dict(drive="notes", path="/docs/b.txt", intent="agent-7 deletes b", key=a7)),
    ("write", dict(drive="notes", path="/docs/b.txt", content="owner line\n", intent="owner recreates b")),
    ("list", dict(drive="notes", path="/docs")),
    ("read", dict(drive="notes", path="/docs/a.txt")),
    ("read", dict(drive="notes", path="/docs/a.txt", at="#1")),
    ("history", dict(drive="notes", path="/docs/a.txt")),
    ("log", dict(drive="notes", by="agent-7")),
    ("fork", dict(drive="notes", new_alias="notes-try")),
]
fork_key = None
for name, args in steps:
    r = call(name, **args)
    print(f"> {name} {json.dumps({k: v for k, v in args.items() if k != 'key'})}\n{r}\n")
    if name == "fork":
        fork_key = r.split("key for the fork: ")[-1].strip()

more = [
    ("write", dict(drive="notes-try", path="/docs/d.txt", content="made in the fork\n", intent="fork work", key=fork_key)),
    ("write", dict(drive="notes-try", path="/docs/a.txt", content="fork rewrote a\n", intent="fork edits a", key=fork_key)),
    ("delete", dict(drive="notes-try", path="/docs/c.txt", intent="fork drops c", key=fork_key)),
    ("read", dict(drive="notes", path="/docs/a.txt", key=fork_key)),
    ("write", dict(drive="notes", path="/docs/c.txt", content="owner also edits c\n", intent="owner edits c after fork")),
    ("diff", dict(fork="notes-try")),
    ("merge", dict(fork="notes-try")),
    ("list", dict(drive="notes", path="/docs")),
    ("read", dict(drive="notes", path="/docs/a.txt")),
    ("undo", dict(drive="notes", by="agent-7", since="1h")),
    ("list", dict(drive="notes", path="/docs")),
    ("read", dict(drive="notes", path="/docs/a.txt")),
    ("restore", dict(drive="notes", path="/docs", at="#2")),
    ("list", dict(drive="notes", path="/docs")),
    ("write", dict(drive="notes", path="/big.txt", content="x" * 700000, intent="big file")),
    ("read", dict(drive="notes", path="/big.txt", offset=524280, length=20)),
    ("write", dict(drive="notes", path="/../etc", content="nope", intent="bad path")),
    ("write", dict(drive="notes", path="/lock.txt", content="first\n", intent="create-only", if_version=0)),
    ("write", dict(drive="notes", path="/lock.txt", content="second\n", intent="create-only again must fail", if_version=0)),
    ("write", dict(drive="notes", path="/lock.txt", content="stale\n", intent="stale version must fail", if_version=3, key=a7)),
    ("changes", dict(drive="notes", after_seq=9)),
    ("write", dict(drive="notes", path="/utf8.txt", content="é" * 300000, intent="multi-byte paging test")),
    ("read", dict(drive="notes", path="/utf8.txt", offset=0, length=524287)),
]
for name, args in more:
    r = call(name, **args)
    shown = {k: (v if len(str(v)) < 60 else f"<{len(str(v))} chars>") for k, v in args.items() if k != "key"}
    if name == "read" and args.get("path") == "/utf8.txt":
        header, body = r.split("\n", 1)
        ok = "\ufffd" not in body and all(c == "é" for c in body)
        print(f"> {name} {json.dumps(shown)}\n{header}\nbody: {len(body)} chars, clean utf-8: {ok}\n")
        continue
    print(f"> {name} {json.dumps(shown)}\n{r}\n")

proc.stdin.close()
proc.wait(timeout=5)
print("server exit", proc.returncode, "root", root)
