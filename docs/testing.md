# Test plan (Milestone 7)

Nothing here has been run yet. Do these in order; stop at the first failure.
Scripts: run with `bash scripts/check.sh` and `bash scripts/measure.sh` (the zip drops exec bits).
Use a throwaway WhatsApp account.

## 0. Build
```bash
cargo test --workspace     # storage dedup/pagination, IPC parsing, response shape
cargo build --release
```

## 1. Automated checks (daemon running)
`bash scripts/check.sh` verifies modes (0700/0600), stale `.part` files, `PRAGMA integrity_check`,
and IPC edge cases (health, chats, version mismatch, garbage, empty chat, 70 KB frame).
It was validated against a fake server speaking the documented protocol, not the real daemon.

## 2. Reliability scenarios
| Scenario | Do | Expect |
|---|---|---|
| Network loss | `nmcli networking off`, wait 30 s, `nmcli networking on` | UI shows daemon `starting`, then `connected`. **Check the log for backoff**: reconnect attempts must be spaced out, not a tight loop. The library's backoff policy is unverified. |
| Suspend/resume | `systemctl suspend`, wake | Reconnects without re-pairing; no duplicate messages (`SELECT chat,id,count(*) FROM messages GROUP BY 1,2 HAVING count(*)>1` returns nothing; the primary key makes this structural). |
| Daemon crash | `kill -9 $(pgrep -x whatsapp-daemon)` | systemd restarts it within ~5 s; UI reconnects by itself; stale socket is replaced; no stale `.part` files remain. |
| UI closed | Quit the UI, send yourself a message from the phone | Daemon keeps running, message is stored and notified. Reopen UI: message is in history. |
| Daemon down | Stop the daemon, start the UI | UI says "Daemon not running" and recovers when it starts. No hang (requests time out at 5 s). |
| Second daemon | Start a second instance | Refuses with "daemon already running"; first is unaffected. |
| Flood | Have a contact send ~50 images quickly | At most 3 downloads at once; UI stays responsive; memory returns to baseline. |
| Document send | Open a chat, click `+`, pick `small.pdf` (or drag it onto the window), then press Send in the "Send ...?" bar. Also check Cancel and switching chat clear the bar | Phone shows the PDF with its name; one line in the UI with an Open file button; no duplicate row. |
| Document receive | From the phone, send a PDF, a .docx and a file named `x.desktop` | PDF/docx download and open with Open file; `x.desktop` is stored as `.bin`. Check `ls -l media/` is all 0600. |
| Oversized document | Send a >25 MB file both ways | Outgoing: UI error `file is larger than 25 MB`. Incoming: shown by name only, no file in `media/`. |
| Backfill sync | Phone online, daemon connected. Note `SELECT count(*) FROM messages` in `cache.db`, press "Sync this chat" in a busy chat | Notice shows batches received; the count rises and older messages appear in the UI; ends with "Sync finished". Repeating it adds nothing (idempotent, no duplicates). |
| Backfill, phone offline | Turn the phone's network off, press "Sync this chat" | Finishes with 0 batches after ~15 s; no error loop, no crash. |
| Sync all | Press "Sync all chats" | Runs one chat at a time (about 2 s apart in the log), stops by itself; a second press while running says "a sync is already running". |
| Oversized media | Send an image > 10 MB | Stored as `[image]`, not downloaded. |
| D-Bus down | `DBUS_SESSION_BUS_ADDRESS=unix:path=/nonexistent` for the daemon | One warning in the log; messages still stored. |
| Crash mid-download | `kill -9` during a large image fetch | No `.part` left after restart. The message itself is lost (see gaps). |

## 3. Desktop environments
GNOME (Wayland), KDE Plasma (Wayland), and one X11 session: notifications appear, UI renders,
QR scans from the phone. Record which fail.

## 4. Footprint
Idle 10 minutes, then `bash scripts/measure.sh`. Record RSS and CPU for daemon and UI, startup time
(`systemctl --user restart whatsapp-daemon; journalctl --user -u whatsapp-daemon` until `health connected`),
and DB/media sizes. No targets were set by the README, so these become the baseline.

## 5. Security review
- Re-run `scripts/check.sh` after pairing, after receiving an image, and after a restart.
- `grep -ri -e "qr" -e "$(sqlite3 ... some message text)" <(journalctl --user -u whatsapp-daemon)`:
  neither the pairing QR payload nor message text may appear in the journal.
- Confirm `RUST_LOG=trace` still logs no message bodies or QR payloads.
- `cargo audit` and `cargo deny check` (not configured yet).

## Known gaps this plan cannot fix
- Messages queued in memory at shutdown, or mid-download at a crash, are lost after the library has acked them.
- Session keys are plaintext in `whatsapp.db` (see `security.md`).
- Media and cache growth are unbounded; there is no eviction.
- History sync is not ingested; only live messages are cached.

## Sending (added after the first test plan)
| Scenario | Do | Expect |
|---|---|---|
| Send text | Open a chat, type, press Send/Enter | Message appears in the UI once, arrives on the phone; `from_me` row in `cache.db`. |
| Offline send | Network off, send | Error shown, draft kept, nothing stored. |
| Slow answer | Send while the link is flapping | UI waits up to 20 s; on "may not have been sent" check the phone before retrying (a retry can duplicate). |
| Empty / huge text | Send "   ", then > 8000 bytes | Rejected with `bad_request`, nothing sent. |
| Echo dedup | Send a message | Exactly one row, even if the library also echoes it back. |

## Contacts (added later)
| Scenario | Do | Expect |
|---|---|---|
| Fresh pairing | Pair a new session, wait ~1 min | `sqlite3 cache.db 'select count(*) from contacts'` > 0; UI list shows names, including contacts you never messaged. |
| Name vs id | Message from a known contact | Chat shows the name, not `...@lid` / `...@s.whatsapp.net`. |
| Rename on phone | Rename a contact on the phone | The name updates (upsert), no duplicate row. |
| Already-paired session | Restart without re-pairing | No new import (the sync is mostly a one-time burst); existing names remain. |

## History sync (added with the history-sync hardening)
| Scenario | Do | Expect |
|---|---|---|
| Fresh pairing import | Pair a new session, watch `RUST_LOG=info` | Log lines `history sync chunk received conversations=N messages=M`; chat list fills in; **no burst of desktop notifications**; UI reloads a few times, not once per message. |
| Idempotent | Restart the daemon / import the same chunk again | Row counts in `messages` unchanged (`select count(*) from messages`). |
| Deleted chat stays deleted | Delete a chat in the UI, then run an on-demand fetch or re-pair | The chat does not reappear until a *live* message arrives in it. |
| Ask phone for older | Open a chat, scroll to the top, press **Ask phone for older messages** (phone online) | Notice shows "Asking your phone..."; after a few seconds older messages appear (press Load older if needed). |
| Phone offline | Same, with the phone off / no network | Request returns OK or times out; nothing breaks; try again when the phone is online. |
| Empty chat | Press the button on a contact you never messaged | Error "no cached message to start from"; nothing sent to the phone. |
| Volume | Import a large account | Memory stays flat, no UI freeze; `bash scripts/measure.sh` before/after. |

## Notifications (name and content)
| Scenario | Do | Expect |
|---|---|---|
| 1:1 from a contact | Have a saved contact message you | Title = their name, body = the text, their avatar as the icon when one was downloaded. |
| Group | Message in a group | Title = group name (or "Group" if unknown), body = `Sender: text`. |
| Unknown sender | Message from a number not in your contacts | Title = `+<number>` (or "Unknown contact" for an `@lid` id). **Never a long internal id.** |
| Photo / voice note | Receive an image / audio | Body `📷 Photo` / `🎤 Audio` (with the caption after the icon when there is one). |
| Quiet modes | Run with `WA_NOTIFY_PREVIEW=0`, then `WA_NOTIFY_CHANNELS=0` | Body "New message"; channel posts silent. |
| Never notify | Post a Status, send from the phone, import history | No notification. |
