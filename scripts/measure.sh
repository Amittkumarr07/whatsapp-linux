#!/usr/bin/env bash
# Idle footprint of the running processes. Let them sit idle for a few minutes first.
for p in whatsapp-daemon whatsapp-ui; do
  pid=$(pgrep -nx "$p") || { echo "$p: not running"; continue; }
  ps -o rss=,pcpu=,etimes=,nlwp= -p "$pid" |
    awk -v n="$p" '{printf "%-16s RSS %.1f MiB | CPU %s%% (lifetime avg) | up %ss | %s threads\n", n, $1/1024, $2, $3, $4}'
done
D="${WA_DATA:-${XDG_DATA_HOME:-$HOME/.local/share}/whatsapp-linux-client}"
du -sh "$D"/*.db "$D"/media 2>/dev/null
