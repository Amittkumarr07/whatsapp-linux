# Security and local data

## What is stored, and where
Default directory: `$XDG_DATA_HOME/whatsapp-linux-client` (or `$WA_DATA`; the systemd unit uses
`%S/whatsapp`). Directory mode 0700, files 0600.

| Path | Contents |
|---|---|
| `whatsapp.db` | Protocol session: identity keys, Signal sessions, app state. Managed by `whatsapp-rust`. **Plaintext SQLite.** |
| `cache.db` | Message cache (chat id, message id, sender, timestamp, text, media kind/path) and your **address-book names** (jid + name). **Plaintext SQLite.** |
| `media/` | Downloaded images (<= 10 MB, named by content hash) and documents (<= 25 MB, named `<sha>-<message id>.<ext>`). Never executable (0600). |
| `$XDG_RUNTIME_DIR/whatsapp/daemon.sock` | Local IPC. Dir 0700, socket 0600. |

## Known gaps (not solved)
- **Session keys are not in the Secret Service.** `whatsapp-rust` persists them itself in
  `whatsapp.db`. Moving them needs either a custom `Backend` implementation backed by the keyring
  or an encrypted SQLite (SQLCipher). Until then protection is filesystem permissions only; use
  full-disk encryption. This does NOT meet the README definition of done for credentials.
- IPC has no peer-UID check; the 0700 directory is the only gate.
- Images from senders are decoded by the UI's image library. Treat a decoder bug as a risk.
- Documents from anyone (including strangers) are downloaded automatically and there is no eviction. "Open file" hands them to `xdg-open`, i.e. whatever app your desktop associates with that type: open files from unknown senders as carefully as an email attachment.
- `send_file` reads any path the daemon's user can read; the 0600 socket is the only gate (same as every other IPC request).
- Notifications show the contact or group name and the message text, like WhatsApp's own. Anything that can read your
  notification history (and anyone who can see the screen) sees it. Set `WA_NOTIFY_PREVIEW=0` to show only "New message",
  and `WA_NOTIFY_CHANNELS=0` to silence channel posts. Status posts never notify.

## Logout / delete everything
```bash
systemctl --user stop whatsapp-daemon
rm -rf "$XDG_DATA_HOME/whatsapp-linux-client"      # or %S/whatsapp for the unit
```
Then remove the linked device on your phone (Settings > Linked devices). Deleting local files
alone does not unlink the session on WhatsApp's side.
