#!/bin/zsh
# Blackhole fleet demo: eight workers, one bad run, one undo, one fork.
# Usage: demo/fleet.sh [--auto] [--no-mount]
# Shards go wherever BLACKHOLE_SHARDS points (s3://bucket with BLACKHOLE_S3_*); unset, a local file:// store is used.
set -e
AUTO=0; MOUNT=1
for a in "$@"; do case $a in --auto) AUTO=1;; --no-mount) MOUNT=0;; esac; done
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
BH=$BIN/blackhole; SV=$BIN/blackhole-server; BM=$BIN/blackhole-mount
PORT=${PORT:-9910}; U=http://127.0.0.1:$PORT
W=$(mktemp -d /tmp/blackhole-demo.XXXXXX)
export BLACKHOLE_ROOT=$W/root
[ -n "$BLACKHOLE_SHARDS" ] || export BLACKHOLE_SHARDS=file://$W/bucket
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1} AWS_EC2_METADATA_DISABLED=true
B=$'\e[1m'; D=$'\e[2m'; G=$'\e[32m'; R=$'\e[31m'; N=$'\e[0m'
scene() { echo; echo "${B}$1${N}"; echo "${D}$2${N}"; if [ $AUTO = 0 ]; then printf "${D}[enter]${N}"; read -r; fi; }
run() { local shown="$*"; shown=${shown//$BIN\//}; shown=${shown//$OWNER/<operator-key>}; echo "${D}\$ $shown${N}"; "$@"; }
cleanup() { set +e; pkill -INT -f "blackhole-mount corpus-v2 $W/mnt" 2>/dev/null; sleep 1; umount $W/mnt 2>/dev/null; pkill -f "blackhole-server --addr 127.0.0.1:$PORT" 2>/dev/null; kill $FEEDPID 2>/dev/null; true; }
trap cleanup EXIT

$BH init > /dev/null
OWNER=$($BH drive create corpus --owner operator 2>/dev/null | sed 's/.*: //')
if curl -s $U/healthz > /dev/null 2>&1; then echo "port $PORT is already in use; set PORT to a free one"; exit 1; fi
$SV --addr 127.0.0.1:$PORT > $W/server.log 2>&1 &
SVPID=$!
for i in $(seq 1 40); do curl -s $U/healthz > /dev/null 2>&1 && break; sleep 0.2; done
mkdir -p $W/seed/pages $W/seed/raw
python3 - $W/seed <<'PY'
import os, sys
d = sys.argv[1]
for i in range(40):
    open(f"{d}/pages/{i:04d}.parquet", "wb").write(os.urandom(512*1024) + bytes(512*1024))
    open(f"{d}/raw/{i:04d}.html", "w").write(f"<html><body>source page {i}</body></html>\n")
PY
$BH put $W/seed corpus:/ --key $OWNER --intent "corpus as of last night" > /dev/null 2>&1
echo "${B}Blackhole fleet demo${N}  ${D}server $U, shards $BLACKHOLE_SHARDS, drive corpus: 40 partitions + 40 raw inputs${N}"

scene "Scene 1. Eight workers, one key each" "The operator mints a key per run with the key API: scoped to a prefix, read only or not, with an expiry. The key is the author of every write."
IDS=(); SECRETS=()
for i in $(seq 1 8); do
  J=$(curl -s -X POST $U/api/keys -H "Authorization: Bearer $OWNER" -H 'content-type: application/json' -d "{\"drive\":\"corpus\",\"principal\":\"run-$i\",\"prefix\":\"/\",\"ttl_seconds\":28800}")
  IDS+=($(echo "$J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')); SECRETS+=($(echo "$J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["secret"])'))
done
echo "minted keys for run-1 .. run-8 (ids ${IDS[1]} .. ${IDS[8]})"
OPJ=$($BH key create corpus --principal operator --admin --key $OWNER --json 2>/dev/null)
export AWS_ACCESS_KEY_ID=$(echo "$OPJ" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])') AWS_SECRET_ACCESS_KEY=$(echo "$OPJ" | python3 -c 'import sys,json; print(json.load(sys.stdin)["secret"])')
curl -s -N "$U/feed/corpus?after=0" -H "Authorization: Bearer $OWNER" > $W/feed.txt 2>&1 &
FEEDPID=$!

scene "Scene 2. The fleet works" "Each worker is nothing but aws s3 cp in a loop with its own key. Every object write becomes a commit under that run with an intent."
PIDS=()
for i in $(seq 1 8); do
  ( for j in $(seq 1 5); do p=$(( (i-1)*5 + j - 1 )); head -c 1048576 /dev/urandom > $W/w$i.bin
      AWS_ACCESS_KEY_ID=${IDS[$i]} AWS_SECRET_ACCESS_KEY=${SECRETS[$i]} aws --endpoint-url $U s3 cp $W/w$i.bin s3://corpus/pages/$(printf %04d $p).parquet --metadata intent="clean partition $p" --no-progress > /dev/null; done ) &
  PIDS+=($!)
done
wait $PIDS
echo "40 partitions rewritten by 8 runs"
run $BH log corpus --key $OWNER --since 1h | tail -4

scene "Scene 3. 02:00: run-42 goes bad" "A model update makes one worker write garbage over partitions and delete inputs it judges duplicates. Nothing stops it. Everything is recorded."
J=$(curl -s -X POST $U/api/keys -H "Authorization: Bearer $OWNER" -H 'content-type: application/json' -d '{"drive":"corpus","principal":"run-42","prefix":"/"}')
BID=$(echo "$J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])'); BSEC=$(echo "$J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["secret"])')
BAD_SINCE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 1
head -c 4096 /dev/zero > $W/garbage.bin
for p in 3 7 11 15 19 23; do AWS_ACCESS_KEY_ID=$BID AWS_SECRET_ACCESS_KEY=$BSEC aws --endpoint-url $U s3 cp $W/garbage.bin s3://corpus/pages/$(printf %04d $p).parquet --metadata intent="clean partition $p" --no-progress > /dev/null; done
for p in 2 5 8; do AWS_ACCESS_KEY_ID=$BID AWS_SECRET_ACCESS_KEY=$BSEC aws --endpoint-url $U s3 rm s3://corpus/raw/$(printf %04d $p).html > /dev/null; done   # deletes carry no intent over S3
echo "${R}run-42 overwrote 6 partitions with 4 KB of zeros and deleted 3 raw inputs${N}"
echo "meanwhile run-1 keeps working:"
head -c 1048576 /dev/urandom > $W/w1.bin
AWS_ACCESS_KEY_ID=${IDS[1]} AWS_SECRET_ACCESS_KEY=${SECRETS[1]} aws --endpoint-url $U s3 cp $W/w1.bin s3://corpus/pages/0000.parquet --metadata intent="clean partition 0 pass 2" --no-progress > /dev/null
AWS_ACCESS_KEY_ID=${IDS[1]} AWS_SECRET_ACCESS_KEY=${SECRETS[1]} aws --endpoint-url $U s3 cp $W/w1.bin s3://corpus/pages/0003.parquet --metadata intent="clean partition 3 pass 2" --no-progress > /dev/null
echo "run-1 rewrote partitions 0 and 3 after run-42 touched 3"
run aws --endpoint-url $U s3 ls s3://corpus/pages/ --human-readable | head -5
echo "${D}...${N}"

scene "Scene 4. Morning. Which run, since when?" "No timestamps to correlate. The key is the author, so the log answers directly."
run $BH log corpus --by run-42 --since 1h --key $OWNER
run $BH history corpus:/pages/0007.parquet --key $OWNER

scene "Scene 5. Undo run-42. Only run-42." "Every object run-42 touched goes back to the version before it. Objects another run also changed since are listed, not guessed. The undo is a new commit; nothing is rewritten."
run $BH undo corpus --by run-42 --since "$BAD_SINCE" --key $OWNER
run aws --endpoint-url $U s3 ls s3://corpus/pages/0007.parquet --human-readable
run aws --endpoint-url $U s3 ls s3://corpus/raw/ | wc -l
run $BH verify corpus --key $OWNER | tail -1

scene "Scene 6. Try a different strategy on the same inputs: fork" "A fork is one row and zero bytes. A second pipeline writes into the fork; a training job reads from it over S3 range requests; the original keeps going."
echo "${D}\$ blackhole fork corpus corpus-v2 --key <operator-key>${N}"; $BH fork corpus corpus-v2 --key $OWNER 2>/dev/null | sed -E 's/admin key: [0-9a-f]+/admin key: <fork-key>/'
FK=$($BH key create corpus-v2 --principal run-88 --key $OWNER --json 2>/dev/null)
FID=$(echo "$FK" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])'); FSEC=$(echo "$FK" | python3 -c 'import sys,json; print(json.load(sys.stdin)["secret"])')
head -c 1048576 /dev/urandom > $W/v2.bin
AWS_ACCESS_KEY_ID=$FID AWS_SECRET_ACCESS_KEY=$FSEC aws --endpoint-url $U s3 cp $W/v2.bin s3://corpus-v2/pages/0007.parquet --metadata intent="strategy B partition 7" --no-progress > /dev/null
echo "run-88 wrote partition 7 in the fork; the original:"
run aws --endpoint-url $U s3api get-object --bucket corpus --key pages/0007.parquet --range bytes=0-99 $W/r.bin --query ContentLength --output text
run $BH diff corpus-v2 --key $OWNER 2>/dev/null || $BH log corpus-v2 --key $OWNER
if [ $MOUNT = 1 ] && [ "$(uname)" = Darwin ]; then
  echo "and the bucket as it was before run-42, in Finder:"
  mkdir -p $W/mnt
  $BM corpus-v2 $W/mnt --key $OWNER --at "$BAD_SINCE" --port $((PORT+1)) > $W/mount.log 2>&1 &
  for i in $(seq 1 75); do mount | grep -q " $W/mnt " && break; sleep 0.2; done
  grep mounted $W/mount.log || tail -2 $W/mount.log
  run ls $W/mnt/pages | head -3
  open $W/mnt 2>/dev/null || true
fi

scene "Scene 7. The record stays, and it is bounded" "A hash chain over every commit; legal hold; erasure that leaves a verifiable tombstone; retention."
run $BH attest corpus --key $OWNER
run $BH drive hold corpus --on --key $OWNER
$BH purge corpus:/raw/0001.html --reason "erasure request" --key $OWNER 2>&1 | tail -1
run $BH drive hold corpus --off --key $OWNER
run $BH purge corpus:/raw/0001.html --reason "erasure request 1234" --key $OWNER
run $BH gc corpus --keep-versions 1 --key $OWNER
run $BH verify corpus --key $OWNER | tail -1

scene "Scene 8. What the fleet saw" "The change feed the workers could have followed: every commit with its run, intent and paths."
python3 - $W/feed.txt <<'PY'
import sys, json
rows = [json.loads(l[5:]) for l in open(sys.argv[1]) if l.startswith("data:") and l[5:].strip()]
for d in rows[-12:]:
    print(f"  #{d['seq']:<4} {d['actor']:<9} {d['intent'][:40]:<40} {len(d['paths'])} path(s)")
PY
echo
echo "${G}done.${N} ${D}root $W${N}"
