#!/bin/zsh
set -e
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
BH=$BIN/blackhole
BM=$BIN/blackhole-mount
W=$(mktemp -d /tmp/bh-mount.XXXXXX)
export BLACKHOLE_ROOT=$W/root
cd $W
$BH init > /dev/null
OWNER=$($BH drive create notes | sed 's/.*: //')
mkdir -p seed/docs
echo "hello from blackhole" > seed/docs/a.txt
printf 'line1\nline2\n' > seed/docs/b.txt
head -c 3000000 /dev/urandom > seed/big.bin
$BH put seed notes:/ --key $OWNER --intent seed > /dev/null
M=$W/mnt
cleanup() { pkill -INT -f "blackhole-mount notes $M" 2>/dev/null; sleep 1; umount $M 2>/dev/null; umount $W/mnt-old 2>/dev/null; pkill -INT -f "blackhole-mount" 2>/dev/null; }
trap cleanup EXIT
echo "== mount (read write) as the owner key"
$BM notes $M --key $OWNER --port 11199 > $W/mount.log 2>&1 &
for i in $(seq 1 30); do grep -q mounted $W/mount.log 2>/dev/null && break; sleep 0.2; done
cat $W/mount.log
mount | grep "$M" | sed 's/ (.*//'
echo "== read"
ls -la $M $M/docs
cat $M/docs/a.txt
cmp $M/big.bin seed/big.bin && echo "big.bin matches through the mount"
time (cat $M/big.bin > /dev/null)
echo "== write: new file, append, mkdir, mv, rm, cp of 5 MB"
echo "written through the mount" > $M/docs/c.txt
echo "line3" >> $M/docs/b.txt
mkdir $M/newdir
mv $M/docs/c.txt $M/newdir/c.txt
rm $M/docs/a.txt
head -c 5000000 /dev/urandom > five.bin
cp five.bin $M/newdir/five.bin
sleep 1.5
cat $M/docs/b.txt
cat $M/newdir/c.txt
cmp five.bin $M/newdir/five.bin && echo "5 MB copy matches"
echo "-- drive log (commits made through the mount):"
$BH log notes --key $OWNER | tail -8
$BH cat notes:/newdir/c.txt --key $OWNER 2>/dev/null
echo "== 20 s soak: atomic-rename rewrites while another process reads"
( end=$((SECONDS+20)); n=0; while [ $SECONDS -lt $end ]; do echo "v$n" > $M/docs/tmp.$$; mv -f $M/docs/tmp.$$ $M/docs/swap.txt; n=$((n+1)); done; echo "writer: $n rewrites" ) &
WPID=$!
( end=$((SECONDS+20)); r=0; while [ $SECONDS -lt $end ]; do cat $M/docs/swap.txt > /dev/null 2>&1 || true; ls $M/docs > /dev/null; r=$((r+1)); done; echo "reader: $r reads" ) &
RPID=$!
wait $WPID $RPID
sleep 1.5
echo "swap.txt now: $(cat $M/docs/swap.txt)"
$BH history notes:/docs/swap.txt --key $OWNER | wc -l | awk '{print "history events for swap.txt:", $1}'
echo "== read only mount as of #1 (before any mount writes)"
$BM notes $W/mnt-old --key $OWNER --at '#1' --port 11200 > $W/mount-old.log 2>&1 &
for i in $(seq 1 30); do grep -q mounted $W/mount-old.log 2>/dev/null && break; sleep 0.2; done
cat $W/mount-old.log
ls $W/mnt-old $W/mnt-old/docs
cat $W/mnt-old/docs/a.txt
if echo x > $W/mnt-old/docs/z.txt 2>/dev/null; then echo "FAIL: wrote to a read-only view"; else echo "write refused on the --at view"; fi
echo "== unmount on Ctrl-C"
pkill -INT -f "blackhole-mount notes $M" || true
pkill -INT -f "blackhole-mount notes $W/mnt-old" || true
sleep 1.5
mount | grep -c "$W" | awk '{print "mounts left:", $1}'
tail -2 $W/mount.log
echo "workspace: $W"
