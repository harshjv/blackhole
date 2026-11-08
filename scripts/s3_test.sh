#!/bin/zsh
set -e
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
BH=$BIN/blackhole
S3=$BIN/blackhole-s3
W=$(mktemp -d /tmp/bh-s3.XXXXXX)
export BLACKHOLE_ROOT=$W/root
cd $W
$BH init > /dev/null
OWNER=$($BH drive create data | sed 's/.*: //')
A1J=$($BH key create data --principal agent-1 --prefix / --key $OWNER --json)
A1=$(echo "$A1J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["secret"])')
A1ID=$(echo "$A1J" | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')
$S3 --addr 127.0.0.1:9901 > $W/s3.log 2>&1 &
S3PID=$!
trap "kill $S3PID 2>/dev/null" EXIT
sleep 0.5
export AWS_ACCESS_KEY_ID=$A1ID AWS_SECRET_ACCESS_KEY=$A1 AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
EP=--endpoint-url=http://127.0.0.1:9901
echo "== list buckets as agent-1"
aws $EP s3 ls
echo "== upload a tree with aws s3 sync (one commit per object)"
mkdir -p tree/sub; echo hello > tree/a.txt; echo world > tree/sub/b.txt; head -c 5000000 /dev/urandom > tree/big.bin
aws $EP s3 sync tree s3://data/proj --no-progress
echo "== ls with delimiter and recursive"
aws $EP s3 ls s3://data/proj/
aws $EP s3 ls s3://data/proj/ --recursive
echo "== head, range get (bytes 4000000-4000099), full get and verify"
aws $EP s3api head-object --bucket data --key proj/big.bin --query '[ContentLength,ETag]' --output text
aws $EP s3api get-object --bucket data --key proj/big.bin --range bytes=4000000-4000099 part.bin --query ContentLength --output text
cmp <(dd if=tree/big.bin bs=1 skip=4000000 count=100 2>/dev/null) part.bin && echo "range bytes match"
aws $EP s3 cp s3://data/proj/big.bin got.bin --no-progress
cmp tree/big.bin got.bin && echo "full object matches"
echo "== overwrite with intent metadata, then read old version by versionId (commit number)"
echo hello-v2 > tree/a.txt
aws $EP s3 cp tree/a.txt s3://data/proj/a.txt --metadata intent="agent-1 bumps a" --no-progress
aws $EP s3api get-object --bucket data --key proj/a.txt --version-id 2 old.txt > /dev/null; echo "version 2 content: $(cat old.txt)"
aws $EP s3 cp s3://data/proj/a.txt -
echo "== server-side copy is zero-copy (same manifest)"
aws $EP s3 cp s3://data/proj/big.bin s3://data/proj/big-copy.bin --no-progress
$BH stats
echo "== delete, then history shows who did what via S3"
aws $EP s3 rm s3://data/proj/sub/b.txt
$BH history data:/proj/a.txt --key $OWNER
$BH log data --key $OWNER
echo "== rclone against the same endpoint"
rclone --config /dev/null --s3-provider Other --s3-endpoint http://127.0.0.1:9901 --s3-access-key-id $A1ID --s3-secret-access-key $A1 ls :s3:data/proj
echo "== create-only put (If-None-Match: *) twice: second must fail with 412"
echo one > lock.txt
aws $EP s3api put-object --bucket data --key proj/lock.txt --body lock.txt --if-none-match '*' --query ETag --output text
aws $EP s3api put-object --bucket data --key proj/lock.txt --body lock.txt --if-none-match '*' 2>&1 | tail -1
echo "== 40 MB multipart upload via aws s3 cp (threshold 8 MB), verify, then re-upload unchanged: shards deduplicated"
head -c 41943040 /dev/urandom > huge.bin
aws $EP s3 cp huge.bin s3://data/proj/huge.bin --no-progress
aws $EP s3 cp s3://data/proj/huge.bin huge-back.bin --no-progress
cmp huge.bin huge-back.bin && echo "40 MB multipart round-trips"
$BH stats | tail -1
aws $EP s3 cp huge.bin s3://data/proj/huge2.bin --no-progress
$BH stats | tail -1
echo "== range past EOF is 416; deleting a directory key is a no-op unless empty with trailing slash"
aws $EP s3api get-object --bucket data --key proj/a.txt --range bytes=100-200 /dev/null 2>&1 | tail -1
aws $EP s3 rm s3://data/proj/sub 2>&1 | tail -1; aws $EP s3 ls s3://data/proj/ --recursive | wc -l | awk '{print "objects after rm of dir key without slash:", $1}'
aws $EP s3api delete-object --bucket data --key proj/sub/ && aws $EP s3 ls s3://data/proj/ --recursive | wc -l | awk '{print "objects after delete of non-empty dir/ key:", $1}'
echo "== wrong secret for a real id is refused"
AWS_ACCESS_KEY_ID=$A1ID AWS_SECRET_ACCESS_KEY=notthesecret aws $EP s3 ls s3://data/ 2>&1 | tail -1
echo "== paginated listing: 1,200 keys with max-keys 500 over three pages, v2 and v1"
mkdir -p many; python3 -c "
import os
for i in range(1200): open(f'many/k{i:04d}.txt','w').write('x')
"
aws $EP s3 sync many s3://data/many --no-progress --only-show-errors
aws $EP s3api list-objects-v2 --bucket data --prefix many/ --max-keys 500 --query '[KeyCount,IsTruncated,NextContinuationToken!=null]' --output text
TOK=$(aws $EP s3api list-objects-v2 --bucket data --prefix many/ --max-keys 500 --query NextContinuationToken --output text)
aws $EP s3api list-objects-v2 --bucket data --prefix many/ --max-keys 500 --continuation-token "$TOK" --query '[KeyCount,IsTruncated,Contents[0].Key]' --output text
aws $EP s3 ls s3://data/many/ | wc -l | awk '{print "total keys via paging:", $1}'
aws $EP s3api list-objects --bucket data --prefix many/ --max-keys 500 --query '[length(Contents),IsTruncated,NextMarker]' --output text
echo "== streaming put: a 120 MB object through the endpoint while the server stays small"
head -c 125829120 /dev/urandom > stream.bin
PSS=$(ps -o rss= -p $S3PID); aws $EP s3 cp stream.bin s3://data/stream.bin --no-progress --expected-size 125829120 2>&1 | tail -1; echo "server rss before: $((PSS/1024)) MB, after: $(( $(ps -o rss= -p $S3PID)/1024 )) MB"
aws $EP s3 cp s3://data/stream.bin stream-back.bin --no-progress > /dev/null && cmp stream.bin stream-back.bin && echo "120 MB streams round-trip"
echo "== unsigned request is refused"
curl -s -o /dev/null -w "anonymous GET -> HTTP %{http_code}\n" http://127.0.0.1:9901/data/proj/a.txt
echo "== wrong key is refused"
AWS_ACCESS_KEY_ID=BHKdeadbeef AWS_SECRET_ACCESS_KEY=deadbeef aws $EP s3 ls s3://data/ 2>&1 | tail -1
echo "workspace: $W"
