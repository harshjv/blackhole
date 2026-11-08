#!/bin/zsh
set -e
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
BH=$BIN/blackhole
W=$(mktemp -d /tmp/bh-e2e.XXXXXX)
export BLACKHOLE_ROOT=$W/root
cd $W
echo "== init"
$BH init
OWNER=$($BH drive create notes | sed 's/.*: //')
echo "owner key $OWNER"
mkdir -p src/docs
echo "hello world" > src/docs/a.txt
printf 'line1\nline2\n' > src/docs/b.txt
head -c 3000000 /dev/urandom > src/big.bin
echo "== put dir as owner (one commit)"
$BH put src notes:/ --key $OWNER --intent "initial import"
echo "== agent keys: agent-7 scoped to /docs, auditor read-only"
A7=$($BH key create notes --principal agent-7 --prefix /docs --key $OWNER)
RO=$($BH key create notes --principal auditor --read-only --key $OWNER)
echo "== agent-7 writes inside its prefix"
echo "agent seven was here" > a.txt
$BH put a.txt notes:/docs/a.txt --key $A7 --intent "agent-7 rewrite a"
echo "agent seven new file" > c.txt
$BH put c.txt notes:/docs/c.txt --key $A7 --intent "agent-7 add c"
$BH rm notes:/docs/b.txt --key $A7 --intent "agent-7 delete b"
echo "== agent-7 outside prefix must be denied"
if $BH put a.txt notes:/big.bin --key $A7 2>/dev/null; then echo "FAIL: prefix not enforced"; exit 1; else echo "denied as expected"; fi
echo "== read-only key must be denied on write"
if $BH put a.txt notes:/docs/z.txt --key $RO 2>/dev/null; then echo "FAIL: read-only not enforced"; exit 1; else echo "denied as expected"; fi
echo "== read-only key can read"
$BH cat notes:/docs/a.txt --key $RO
echo "== owner edits b concurrently-ish (recreates b) and edits big.bin"
printf 'owner line\n' > b.txt
$BH put b.txt notes:/docs/b.txt --key $OWNER --intent "owner recreates b"
echo "== history of /docs/a.txt"
$BH history notes:/docs/a.txt --key $OWNER
echo "== log"
$BH log notes --key $OWNER
echo "== log --by agent-7"
$BH log notes --by agent-7 --key $OWNER
echo "== as-of read of a.txt at #1"
$BH cat notes:/docs/a.txt --at '#1' --key $OWNER
echo "== fork"
FK=$($BH fork notes notes-agent-9 --key $OWNER | tee /dev/stderr | sed 's/.*admin key: //')
echo "== fork sees parent content; fork key cannot read the parent; agent-7's parent key cannot read the fork; owner admin key can review the fork"
$BH ls notes-agent-9:/docs --key $FK
if $BH ls notes:/docs --key $FK 2>/dev/null; then echo "FAIL: fork key read the parent"; exit 1; else echo "denied as expected"; fi
if $BH ls notes-agent-9:/docs --key $A7 2>/dev/null; then echo "FAIL: non-admin parent key read the fork"; exit 1; else echo "denied as expected"; fi
$BH ls notes-agent-9:/docs --key $OWNER > /dev/null && echo "owner can review the fork"
echo "== write in fork, delete inherited file in fork; parent unchanged"
echo "fork only" > d.txt
$BH put d.txt notes-agent-9:/docs/d.txt --key $FK
$BH rm notes-agent-9:/docs/c.txt --key $FK
echo "-- fork /docs:"; $BH ls notes-agent-9:/docs --key $FK
echo "-- parent /docs:"; $BH ls notes:/docs --key $OWNER
echo "== rename dir in parent"
$BH mv notes:/docs notes:/documents --key $OWNER --intent "rename docs"
$BH ls notes:/ --key $OWNER
$BH history notes:/documents/a.txt --key $OWNER
echo "== undo agent-7 since 1h (b.txt was also touched by owner -> conflict; a.txt and c.txt revert)"
$BH undo notes --by agent-7 --since 1h --key $OWNER
echo "-- /documents after undo:"; $BH ls notes:/documents --key $OWNER
$BH cat notes:/documents/a.txt --key $OWNER
echo "== restore /documents to #1 state (path moved, so restore by old path at #1 recreates /docs)"
$BH restore notes:/docs --at '#1' --key $OWNER
$BH ls notes:/docs --key $OWNER
echo "== undo must not wipe other actors' files inside a directory the undone actor created"
A8=$($BH key create notes --principal agent-8 --prefix /documents --key $OWNER)
echo x > x.txt; $BH put x.txt notes:/documents/shared/x.txt --key $A8 --intent "agent-8 creates shared dir"
echo o > o.txt; $BH put o.txt notes:/documents/shared/owner.txt --key $OWNER --intent "owner adds a file in it"
$BH undo notes --by agent-8 --since 1h --key $OWNER
$BH ls notes:/documents/shared --key $OWNER
$BH ls notes:/documents/shared --key $OWNER | grep -q owner.txt || { echo "FAIL: owner.txt wiped by undo"; exit 1; }
$BH ls notes:/documents/shared --key $OWNER | grep -q x.txt && { echo "FAIL: x.txt survived undo"; exit 1; }
echo "owner.txt kept, x.txt removed"
echo "== range read of big.bin: 100 bytes at offset 2_500_000 fetches 1 shard"
$BH cat notes:/big.bin --range 2500000-2500100 --key $OWNER > /dev/null
echo "== get and verify big.bin"
$BH get notes:/big.bin out.bin --key $OWNER
cmp src/big.bin out.bin && echo "big.bin round-trips"
echo "== rename over an existing file keeps the target's identity and history (editor save pattern)"
echo "draft 1" > d1.txt; $BH put d1.txt notes:/documents/report.md --key $OWNER --intent "draft 1" > /dev/null
echo "draft 2" > d2.txt; $BH put d2.txt notes:/documents/report.tmp --key $OWNER --intent "editor temp" > /dev/null
$BH mv notes:/documents/report.tmp notes:/documents/report.md --key $OWNER --intent "editor atomic save" > /dev/null
$BH cat notes:/documents/report.md --key $OWNER 2>/dev/null
$BH history notes:/documents/report.md --key $OWNER | grep -c 'write' | awk '{print "versions of report.md:", $1}'
$BH ls notes:/documents --key $OWNER | grep -c 'report.tmp' | awk '{print "report.tmp entries left:", $1}'
echo "== verify the hash chain"
$BH verify notes --key $OWNER | tail -1
$BH verify notes-agent-9 --key $FK | tail -1
echo "== tamper with the log directly in sqlite: verify must fail"
sqlite3 $BLACKHOLE_ROOT/authority.sqlite "UPDATE \"commit\" SET intent='forged' WHERE drive_id=1 AND seq=2"
if $BH verify notes --key $OWNER > verify.out 2>&1; then echo "FAIL: tamper not detected"; exit 1; fi
grep -E '^#2' verify.out; grep -E '^#3' verify.out
sqlite3 $BLACKHOLE_ROOT/authority.sqlite "UPDATE \"commit\" SET intent='agent-7 rewrite a' WHERE drive_id=1 AND seq=2"
$BH verify notes --key $OWNER | tail -1
echo "== tamper with a version row: verify must fail on that commit"
sqlite3 $BLACKHOLE_ROOT/authority.sqlite "UPDATE node_version SET size=size+1 WHERE drive_id=1 AND from_seq=3"
$BH verify notes --key $OWNER 2>/dev/null | grep -E '^#3'
sqlite3 $BLACKHOLE_ROOT/authority.sqlite "UPDATE node_version SET size=size-1 WHERE drive_id=1 AND from_seq=3"
echo "== attest"
$BH attest notes --key $OWNER
echo "== legal hold blocks purge"
$BH drive hold notes --on --key $OWNER
if $BH purge notes:/documents/c.txt --reason test --key $OWNER 2>/dev/null; then echo "FAIL: purge under hold"; exit 1; else echo "refused as expected"; fi
$BH drive hold notes --off --key $OWNER
echo "== purge a deleted file's history (content gone, record kept, shard deleted)"
BEFORE=$($BH stats | grep -o 'shards [0-9]*')
$BH purge notes:/documents/c.txt --reason "erasure request 1234" --key $OWNER
AFTER=$($BH stats | grep -o 'shards [0-9]*')
echo "$BEFORE -> $AFTER"
$BH history notes:/documents/c.txt --key $OWNER | grep purged
$BH cat notes:/documents/c.txt --at '#3' --key $OWNER 2>&1 | tail -1
$BH verify notes --key $OWNER | grep -E 'purged|bad|commit\(s\)'
echo "== gc with retention: 25 versions of one file, keep 3; a fork pins the version it sees; hold refuses"
for i in $(seq 1 25); do echo "rev $i" > rev.txt; $BH put rev.txt notes:/documents/rev.txt --key $OWNER --intent "rev $i" > /dev/null; done
$BH fork notes notes-pin --key $OWNER > /dev/null
echo "rev 26" > rev.txt; $BH put rev.txt notes:/documents/rev.txt --key $OWNER > /dev/null
$BH drive hold notes --on --key $OWNER > /dev/null
if $BH gc notes --keep-versions 3 --key $OWNER 2>/dev/null; then echo "FAIL: gc under hold"; exit 1; else echo "gc refused under hold"; fi
$BH drive hold notes --off --key $OWNER > /dev/null
BEFORE=$($BH stats | grep -o 'shards [0-9]*')
$BH gc notes --keep-versions 3 --key $OWNER
AFTER=$($BH stats | grep -o 'shards [0-9]*')
echo "$BEFORE -> $AFTER"
$BH cat notes/documents/rev.txt --key $OWNER 2>/dev/null || $BH cat notes:/documents/rev.txt --key $OWNER 2>/dev/null
$BH cat notes-pin:/documents/rev.txt --key $($BH drive list >/dev/null; sqlite3 $BLACKHOLE_ROOT/authority.sqlite "select id from access_key where drive_id=(select id from drive where alias='notes-pin') limit 1") 2>/dev/null | head -1
$BH history notes:/documents/rev.txt --key $OWNER | grep -c 'purged' | awk '{print "expired versions in history:", $1}'
$BH verify notes --key $OWNER | tail -1
echo "== stats"
$BH stats
echo "workspace: $W"
