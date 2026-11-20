#!/usr/bin/env bash
# Run with the daemon up. Checks permissions, stale files, DB integrity and the IPC edge cases.
# Usage: scripts/check.sh   (override locations with WA_DATA / WA_SOCK)
set -u
D="${WA_DATA:-${XDG_DATA_HOME:-$HOME/.local/share}/whatsapp-linux-client}"
S="${WA_SOCK:-${XDG_RUNTIME_DIR:-/nonexistent}/whatsapp/daemon.sock}"
fail=0
ok()  { echo "ok    $*"; }
bad() { echo "FAIL  $*"; fail=1; }
mode() { stat -c %a "$1"; }
chk() { [ "$(mode "$1")" = "$2" ] && ok "$1 is $2" || bad "$1 is $(mode "$1"), want $2"; }
ask() { printf '%s\n' "$1" | timeout 3 nc -U -N "$S" 2>/dev/null; }

echo "== permissions"
for p in "$D" "$D/media" "$(dirname "$S")"; do [ -d "$p" ] && chk "$p" 700 || bad "missing dir $p"; done
for p in "$D/whatsapp.db" "$D/cache.db" "$S" "$D"/*.db-wal "$D"/*.db-shm "$D"/media/*; do
  [ -e "$p" ] && chk "$p" 600
done

echo "== leftovers / integrity"
ls "$D"/media/*.part >/dev/null 2>&1 && bad "stale .part files in media/" || ok "no stale .part files"
if command -v sqlite3 >/dev/null; then
  [ "$(sqlite3 "$D/cache.db" 'PRAGMA integrity_check')" = ok ] && ok "cache.db integrity_check" || bad "cache.db integrity_check"
else echo "skip  sqlite3 not installed"; fi

echo "== ipc"
command -v nc >/dev/null || { echo "skip  nc not installed"; exit $fail; }
ask '{"v":1,"id":1,"type":"health"}' | grep -q '"type":"health"'   && ok health          || bad health
ask '{"v":1,"id":2,"type":"chats"}'  | grep -q '"type":"chats"'    && ok chats           || bad chats
ask '{"v":9,"id":3,"type":"health"}' | grep -q version_mismatch    && ok version_mismatch || bad version_mismatch
ask 'garbage'                        | grep -q bad_request         && ok bad_request      || bad bad_request
ask '{"v":1,"id":4,"type":"history","chat":""}' | grep -q bad_request && ok empty_chat    || bad empty_chat
head -c 70000 /dev/zero | tr '\0' a | timeout 3 nc -U -N "$S" 2>/dev/null | grep -q frame_too_large \
  && ok frame_too_large || bad frame_too_large
exit $fail
