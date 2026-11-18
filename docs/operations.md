# Backup, restore, recovery

`cache.db` is a cache of messages seen live; `whatsapp.db` is the linked-device session.
Data directory: `$XDG_DATA_HOME/whatsapp-linux-client` (or `%S/whatsapp` under the systemd unit).

## Backup
```bash
sqlite3 "$D/cache.db" ".backup '$HOME/cache-backup.db'"     # safe while the daemon runs
```
`whatsapp.db` holds session secrets. Back it up only to encrypted storage, and stop the daemon first:
```bash
systemctl --user stop whatsapp-daemon && cp -p "$D/whatsapp.db" /secure/location/ && systemctl --user start whatsapp-daemon
```
Copy only this file to a second machine and both would claim the same linked device. Don't.

## Restore
Stop the daemon, copy the backup over the file, `chmod 600`, start the daemon, run `bash scripts/check.sh`.

## Corruption
- `cache.db`: `sqlite3 cache.db 'PRAGMA integrity_check'`. If bad, stop the daemon and delete `cache.db*`. It is recreated empty
  (history before that point is gone; media files remain).
- `whatsapp.db`: if the session is bad, delete it and re-pair from the phone (Linked devices). Remove the old entry on the phone.

## Full removal
See `security.md`.
