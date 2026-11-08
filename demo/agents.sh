#!/bin/zsh
set -e
AUTO=0
for a in "$@"; do case $a in --auto) AUTO=1;; esac; done
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
BH=$BIN/blackhole
W=$(mktemp -d /tmp/blackhole-record.XXXXXX)
export BLACKHOLE_ROOT=$W/root
blackhole() { $BH "$@"; }
trap 'rm -rf $W' EXIT
cd $W

B=$'\e[1m'; D=$'\e[2m'; O=$'\e[38;2;255;90;31m'; R=$'\e[38;2;255;77;77m'; G=$'\e[38;2;134;239;172m'; N=$'\e[0m'
typeset -A COLOR
COLOR=(you $'\e[1m' agent-1 $'\e[38;2;255;184;107m' agent-2 $'\e[38;2;125;211;252m' agent-3 $'\e[38;2;167;139;250m')

pause() { if [ $AUTO = 0 ]; then printf "  ${D}▸ enter${N}"; read -rs; printf '\r\e[K'; else sleep 2; fi; }
title() { echo; echo "  ${O}${B}$1${N}"; echo; }
who() { printf '%s%-8s%s' "${COLOR[$1]}" "$1" "$N"; }

write() {
  printf '%s\n' "$3" > $W/f
  $BH put $W/f team:/launch/$2 --key "${KEY[$1]}" --intent "$4" > /dev/null 2>&1
  echo "  $(who $1) wrote ${B}$2${N}  ${D}\"$4\"${N}"; sleep 0.6
}
trash() {
  printf '%s\n' "$3" > $W/f
  $BH put $W/f team:/launch/$2 --key "${KEY[$1]}" --intent "$4" > /dev/null 2>&1
  echo "  $(who $1) ${R}overwrote $2${N}  ${D}\"$4\"${N}"; sleep 0.6
}
remove() {
  $BH rm team:/launch/$2 --key "${KEY[$1]}" --intent "$3" > /dev/null 2>&1
  echo "  $(who $1) ${R}deleted $2${N}  ${D}\"$3\"${N}"; sleep 0.6
}
folder() {
  echo "  ${D}launch/${N}"
  for f in plan.md pricing.md notes.md; do
    if $BH cat team:/launch/$f > $W/c 2>/dev/null; then
      printf "    %-12s %s\n" "$f" "$(head -1 $W/c)"
    else
      printf "    ${R}%-12s missing${N}\n" "$f"
    fi
  done
  echo
}
cmd() { echo "  ${D}\$${N} ${B}$1${N}"; sleep 0.5; }

$BH init > /dev/null
OWNER=$($BH drive create team --owner you 2>/dev/null | head -1 | sed 's/.*: //')
export BLACKHOLE_KEY=$OWNER
typeset -A KEY
KEY[you]=$OWNER
printf 'Launch Tuesday.\n' > $W/f
$BH put $W/f team:/launch/notes.md --intent "start notes" > /dev/null 2>&1
for a in agent-1 agent-2 agent-3; do KEY[$a]=$($BH key create team --principal $a --prefix /launch 2>/dev/null); done
clear

title "You and three AI agents share one launch folder."
pause
write agent-1 plan.md "Tue post, Wed email, Thu demo day." "draft the launch plan"
write agent-2 pricing.md "Price at \$49. Competitors charge \$39 to \$79." "research pricing"
echo
folder
pause

title "You ask agent-3 to tidy up the folder. It goes too far."
pause
trash agent-3 plan.md "TODO" "shorten the plan"
remove agent-3 pricing.md "remove duplicate"
write you notes.md "Launch Tuesday. Book the demo room." "add demo room"
echo
folder
pause

title "Normally you restore last night's backup and lose your own edit too."
echo "  ${B}Blackhole knows who did what.${N}"
echo
pause
cmd "blackhole log team --by agent-3"
$BH log team --by agent-3 2>/dev/null | awk -F'\t' '{ split($2, t, "T"); printf "    %s  %s  %s\n", substr(t[2], 1, 8), $3, $5 }'
echo
pause

title "Take back agent-3. Only agent-3."
pause
cmd "blackhole undo team --by agent-3 --since 10m"
$BH undo team --by agent-3 --since 10m 2>/dev/null | sed "s/^/    /; s/restored/${G}restored${N}/"
echo
pause

title "The plan and the pricing are back. Your edit stays."
folder
pause

title "blackhole"
echo "  ${D}a filesystem for AI agents · every write has a name · undo any agent${N}"
echo "  ${D}useblackhole.com${N}"
echo
