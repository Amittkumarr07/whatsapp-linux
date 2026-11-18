# CLAUDE.md: project memory for the unofficial WhatsApp Linux client

Saved from a long working session so a future session can continue without re-deriving anything.
This is a summary of decisions and state, not a transcript. Original spec: `README.md`.

## How to work with this user
- User preference: act as a **lazy senior developer**. Ladder before writing code: (1) does it need
  to exist, (2) stdlib, (3) platform feature, (4) already-installed dependency, (5) one-liner,
  (6) minimum code. Never cut validation, error handling, security or accessibility.
- End every reply with one or two lines on what was skipped/unchecked and any risk.
- **Claude's sandbox has no Rust toolchain and no network.** Nothing Rust was ever compiled by Claude.
  The user builds locally (`~/whatsapp-linux`, Linux, recent Rust) and pastes `cargo` output;
  fix errors in order (rustc reports type errors first, borrow errors after).
- **Needs nightly Rust**: `wacore-binary` 0.5 enables `feature(portable_simd)` via its default `simd` feature; stable fails with E0554. `rust-toolchain.toml` pins nightly. Check with `cargo tree -e features -i wacore-binary` if this changes.
- Be honest about verified vs. predicted. Don't claim something works until the user has run it.

## What this is
Unofficial WhatsApp Web (linked device) client for Linux. Daemon + local IPC + thin UI.
Use a **throwaway number**: unofficial clients can be logged out or banned.

## Layout
```
Cargo.toml                       workspace (resolver 3, edition 2024)
crates/whatsapp-daemon/          protocol, storage, IPC, notifications
  src/protocol.rs                ONLY file that touches whatsapp-rust (adapter + trait Protocol)
  src/storage.rs                 diesel + embedded migrations, message cache (cache.db)
  src/ipc.rs                     Unix socket, newline-delimited JSON, versioned (v=1)
  src/notify.rs                  notify-rust desktop notifications
  src/main.rs                    wiring, XDG dirs, perms, writer thread, signals
  migrations/0001_messages, 0002_media
crates/whatsapp-ui/src/main.rs   Iced 0.14 client; speaks JSON only, no shared crate
packaging/systemd/whatsapp-daemon.service
scripts/check.sh, measure.sh     run with `bash` (exec bit is lost in the zip)
docs/security.md testing.md operations.md
.github/workflows/ci.yml audit.yml
```

## Key decisions (and why)
- **Use the `whatsapp-rust` crate, pinned to 0.5.x** (0.7.0 exists; its message types changed to
  `buffa::MessageField`, so the 0.5-shaped code would not compile). Siblings: `-tokio-transport`,
  `-ureq-http-client`, `wacore`, `waproto`, all 0.5.
- **diesel, not sqlx**: the protocol crate's SQLite storage already uses diesel + `libsqlite3-sys`;
  a second SQLite binding would clash (`links` conflict).
- **Iced over GPUI**: on crates.io, documented. Not benchmarked; GPUI untested.
- **UI is a pure JSON client**: no shared types crate. Short one-shot connections for requests, one
  long-lived connection for events; reconnects forever.
- **Persist, then broadcast**: one writer thread inserts (INSERT OR IGNORE on `(chat,id)`), only new
  rows are broadcast/notified. Bounded queues everywhere.
- QR payload travels in `State::Pairing{qr}`; `Debug` for `State` is redacted; QR printed to stdout
  only when stdout is a terminal (never into the journal).
- Only **images** are auto-downloaded (<=10 MB, 3 concurrent, content-hash filename, 0600, atomic
  rename). Other media recorded by kind only.
- Messages with no text and no media (internal protocol traffic) are skipped (`protocol.rs`).
- Files 0600 / dirs 0700; DB files pre-created 0600; socket 0600 in a 0700 dir under `$XDG_RUNTIME_DIR`.

## IPC (v1)
Requests: `health`, `subscribe`, `chats`, `history{chat,before?,limit?}`, `send_text{chat,text}`, `delete_chat{chat}`, `fetch_older{chat}`.
Frames <=64 KB, <=8 clients, text <=8000 bytes, 5 s write timeout. Errors use fixed codes
(`bad_request`, `version_mismatch`, `frame_too_large`, `lagged`, `busy`, `send_failed`, `timeout`).
Subscribe first, then fetch history, dedup by message id.

## Status
| Item | State |
|---|---|
| Builds, 3 unit tests pass (IPC parsing, response shape, storage dedup/paging) | **verified by user** |
| Pairing, connected state, chat list, UI rendering, composer showing | **verified by user (screenshot)** |
| Migrations + SQL | verified on real SQLite in Claude's sandbox |
| `scripts/check.sh` | verified only against a fake server |
| Contact import (`Event::ContactUpdate` -> `contacts` table, names shown in the chat list; contacts with no chat also listed) | **verified by user**: 407 contacts imported after pairing; field names compiled and work |
| Sending text to a new chat started with a full international number (delivered to the phone) | **verified by user**; root cause of earlier "sent but not delivered" was a number without country code |
| Notifications, image download, reconnect, suspend/crash scenarios | **untested** |
| Session was revoked (`LoggedOut`) on a later run | recovered by re-pairing; cause unknown |

## Documents (added after the session above; NOT compiled, NOT run)
- Receive: `Event::Message` with a `document_message` is downloaded off the event path (<= 25 MiB, same 3-slot
  semaphore as images), stored 0600 as `<sha>-<msgid>.<safe ext>` in `media/`. The sender's file name is display only
  (control chars stripped, 120 chars) and never part of a path; launchable extensions (.desktop, .sh, .exe ...) are
  stored as `.bin`. `text` = file name (+ caption), `media_kind` = `document`, `media_path` = local copy.
- Send: IPC `send_file{chat,path}` (absolute path; daemon checks regular file and <= 25 MiB, answers within 120 s)
  -> `Outgoing::File` -> `client.upload(bytes, MediaType::Document)` -> `DocumentMessage` -> `send_message`.
  UI: `+` button (native dialog via `zenity`, else `kdialog`, run as a subprocess: no crate) or a file dropped on the
  window (`iced::event::listen_with` -> `window::Event::FileDropped`) stages the file; a bar asks "Send <name>?" and only
  Send uploads it. One staged file at a time; it is cleared when you switch chat. Winit on Wayland may not deliver
  drops (unverified here): the `+` button is the reliable path. `Open file` button runs `xdg-open`.
- **Unverified library assumptions** (fix from compiler output): `wacore::download::MediaType::Document`,
  `Client::upload` and the `UploadResponse` field names/types (`url, direct_path, media_key, file_enc_sha256,
  file_sha256, file_length`), `DocumentMessage` implementing `Downloadable`, `DocumentMessage.caption`, that
  `send_message` returns the id as `String`.
- Known gaps: one file at a time; no picker if neither zenity nor kdialog is installed; documents from history sync are names only (no download); no media
  eviction, so auto-downloaded documents accumulate; an upload blocks the send loop (text waits behind it).

## Backfill sync (added after the session above; NOT compiled, NOT run)
- `src/sync.rs`: IPC `sync{chat|null}` starts ONE background job, `sync_status` reports `{running,rounds,done,total}`.
  Per chat it repeats: anchor = oldest cached message -> `Outgoing::Older` (50) -> wait up to 12 s for the oldest cached
  message to become older (progress is measured in the DB, not by library events) -> pause 2 s. Stops for a chat when
  nothing older arrives. One chat: up to 10 rounds. All chats: the 100 most recent chats with cached messages, 3 rounds
  each, and the job stops after 10 chats in a row that returned nothing (phone offline). Requires state `connected`.
- UI: "Sync all chats" (left), "Sync this chat" (next to Delete chat), progress shown in the notice line, polled every 2 s.
- It is deliberately slow and manual: a burst of history requests is the kind of traffic that can get an unofficial
  client flagged. It never runs by itself.
- **Reported: "0 batches received"** (first real run). Sync now counts `asked` (requests handed to the phone) separately
  from `rounds` (answers that made the oldest cached message older) and the UI says which stage failed; it waits 30 s
  per answer (was 12) and logs `sync: ...` lines at info. Also fixed: history messages whose key has no `remote_jid`
  were skipped; they now use the conversation id. Still unknown whether 0.5.0 delivers the on-demand answer as
  `Event::HistorySync`: diagnose with `RUST_LOG=debug` (event NAMES are logged) right after pressing Sync.
- Limits: a chat whose older messages have no text and no media looks "finished"; the phone must be online; whether
  0.5.0 answers `fetch_message_history` at all is still unverified (see History sync above).

## Logout recovery (updated)
`LoggedOut` -> `protocol.rs` deletes `whatsapp.db*` and returns `protocol::REVOKED` -> `main.rs` `restart_to_pair`
re-execs the daemon after 3 s (env `WA_REPAIR_COUNT`, max 3 in a row) so a new QR shows without re-running. User
report that triggered this: after unlinking the device on the phone, the first start printed the revoked error and
exited. NOT compiled/run. Re-exec abandons open SQLite connections like a crash would (WAL is recovered on open).

## Open issue at the end of this session
Daemon compiled and started, then logged: `Got LoggedOut connect failure ... Auto-reconnect disabled,
shutting down`. WhatsApp rejected the saved session (device unlinked on the phone, or revoked).
My daemon then exited silently with success: fixed so it now deletes the dead session
(`whatsapp.db*`), logs an error, and exits non-zero (systemd restarts it, UI shows a fresh QR).
**To recover manually:** `rm ~/.local/share/whatsapp-linux-client/whatsapp.db*`, run the daemon,
scan the QR, and remove the stale entry under phone > Linked devices. Cause of the logout unknown.

## History sync ("all chats on every device")
- The uploaded project already had an `Event::HistorySync` handler (conversations, messages, push names,
  status posts). This session hardened it and added on-demand fetch.
- **Initial history** = the one-time bundle the phone pushes at pairing; a limited recent window (the library's
  default asks for about 30 days; unverified for 0.5.0). Re-pairing is the only way to trigger it again.
- **Imports are `historical`** (`Incoming.historical`): written in batches of <=500 per transaction, idempotent
  (PK `(chat,id)`), no desktop notifications, no per-message live events (one `contacts_changed` ping per batch,
  which makes the UI reload), no image downloads (old media stays a `[image]` placeholder), and they never
  un-delete a chat the user deleted (checked through the `contacts.pn` alias). A live message does un-delete.
- **Older messages without re-pairing**: UI button "Ask phone for older messages" -> IPC `fetch_older{chat}` ->
  `Client::fetch_message_history(chat_jid, oldest_id, from_me, ts_ms, 50)` -> the phone (must be online) answers with
  an on-demand history sync -> the same import path. Needs at least one cached message in the chat as the anchor.
- **Not synced**: media from history, read/archive/pin/mute state, deletions made on the phone, anything the phone
  will not provide. Chats are stored in plaintext in `cache.db` (see docs/security.md).
- **Unverified**: the 0.5.0 signature of `fetch_message_history` (read from 0.4.2), that the answer arrives as
  `Event::HistorySync` in 0.5, and how many messages one request returns.

## Features added in the uploaded project (by another session, found on import)
Migrations 0005 `deleted_chats` and 0006 `contact_avatar`; push-name and profile-picture handling; chat deletion
(`delete_chat`); Chats/Status/Channels tabs; `.deb` packaging in `package-folder/`. `upsert_contact` is
"last write wins" except it never replaces a name with the raw jid, so a push name can overwrite an address-book name.

## Known gaps / not done
- **Session keys are plaintext in `whatsapp.db`** (library-managed). Secret Service/keyring needs a
  custom `Backend` or SQLCipher. Does NOT meet the README definition of done. See docs/security.md.
- Contact names come only from the phone's address-book sync (`ContactUpdate`), mostly delivered once right after pairing. Push names (what strangers call themselves) are NOT handled, so non-contacts show raw ids (`...@lid`). No timestamps in the UI.
- Contacts arrive under two ids (phone number and @lid). `contacts.pn` links a @lid id to its phone-number id (migration 0004, backfilled by unique-name heuristic); chat list and history merge them. Own self-chat is a bare @lid with no name.
- Chat/contact list is capped at 2000 entries and rendered as one plain list (no search).
- No system tray, no packaging (AppImage/Flatpak/.deb/.desktop), no media/cache eviction.
- Shutdown can drop queued messages; a crash mid-download loses that message.
- New chat = type a full international number in the UI (digits only, 8-15; no on-WhatsApp check, no default country code). A number missing its country code "sends" but is never delivered.
- Only plain text and documents can be sent (no replies, reactions, images/video/voice).
- Library reconnect backoff/jitter unverified. IPC has no peer-UID check.

## Unverified library assumptions (fix from compiler/runtime output)
`info.timestamp.timestamp()` (compiled), `msg.image_message.as_deref().cloned()` (compiled),
`device_sent_message.message` shape, `Event::LoggedOut(_)`, that `client` survives reconnects,
`MessageExt::get_base_message()` as a cleaner alternative to `text_of` if it exists in 0.5.

## Timeline of the session (condensed)
1. Read README spec; chose library over custom protocol; Milestone 1 PoC from the crate quick start.
2. M2 daemon foundation (tracing, `Protocol` trait, health, SIGTERM, systemd unit).
3. M3 storage/sync: found `libsqlite3-sys` clash -> diesel; verified SQL in Python's sqlite3.
4. M4 IPC (JSON lines, versioned, bounded). M5 Iced UI + workspace restructure + `chats` request.
5. M6 notifications, image download, XDG data dir + permissions, security doc; Secret Service not doable.
6. M7 review fixes (.part sweep, UI timeouts, unit hardening), check/measure scripts, test/ops docs.
7. Added send_text + composer + CI. User compiled: one error (`Box<dyn Error>` conversion in main.rs),
   fixed with `.map_err(|e| e.to_string())`; tests passed.
8. First live run: paired and connected, but UI showed a wall of `[non-text message]` from own account
   (internal protocol traffic + no history sync) -> skip contentless messages, extract extended/device-sent text.
9. Later run hit `LoggedOut`; added logout handling and this file.

## Next steps
1. Re-pair, then test with real incoming messages and a reply from the composer.
2. Confirm sends arrive on the phone and no duplicate rows appear.
3. Run `docs/testing.md` scenarios and `bash scripts/check.sh`; push to GitHub for CI.
4. Then: history sync, contact names/timestamps, Secret Service/SQLCipher, packaging.

## UI refresh and current conversation (2026-11-18)
User request: “go ~/whatsapp-linux and read the model.md file you should also write what you have done in that file and the our converstation in the same way written previously. You have to update the ui just like the whatsapp ui add every thing missing in this ui remove the massage showing on the top and add a option like show log to view these things.” The existing project memory is named `MODEl.md` (mixed case), and it uses dated/condensed session notes.

Changes made in `crates/whatsapp-ui/src/main.rs`:
- Reworked the main window toward WhatsApp Desktop's two-pane shape: fixed-width conversation sidebar, avatars/labels, category tabs and chat search on the left; selected chat header, message history, message composer, and file/send actions on the right.
- Added a neutral empty-chat prompt and more WhatsApp-like muted chat background, outgoing/incoming message bubbles, and connection indicator.
- Removed the persistent top status/error/notice row. The header now has a `Show log` button; it opens an activity view with current connection state, current notice/error, and recent event/request entries. The in-memory log is capped at 500 entries and can be cleared.
- Existing capabilities remain exposed: Chats/Status/Channels, search, new chat, file attach/drop, send text, open document, load/ask older messages, sync, and delete chat.

Conversation summary: I told the user I would inspect project notes and source first, then update the WhatsApp-like UI and move the status message into a log view. I found and read `MODEl.md` (not lowercase `model.md`) and inspected the Iced UI implementation. I identified that the project already had several core actions and implemented a layout refresh plus the log view as requested. No compile or runtime verification has been performed in this session; visual parity and the full feature set of WhatsApp remain incomplete. In particular, this UI still supports only text and documents for sending, and does not implement voice/video calls, reactions, replies, read/archive/pin/mute state, or tray integration.

Verification update for the UI refresh: `rustfmt --edition 2024 crates/whatsapp-ui/src/main.rs` completed, and `cargo check -p whatsapp-ui` succeeded after formatting. This verifies compilation only; the window has not been launched for visual or interaction review.

## Reply action follow-up (2026-11-18)
User asked: “Add a option to replay the massage just like the whatsapp” (interpreted as reply to a message). Added a **Reply** action under each rendered message, a quoted target preview and cancel action above the composer, and sends the selected target in the version-1 JSON request. The daemon builds WhatsApp `extended_text_message` + `ContextInfo` with the quoted message and stanza id; own-message replies omit participant, incoming-message replies include its sender. Existing requests remain compatible because `reply_to` is optional.

Conversation summary: The user requested a reply option after the UI refresh. I checked IPC and `waproto` message types, found `ContextInfo.quoted_message` support, then implemented UI selection/composer state and daemon serialization of the quoted reply. `rustfmt` completed and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded. No runtime send/receive test was done. The cached message schema still does not persist reply metadata, so historical UI bubbles don't render existing quoted previews, and the newly sent local echo only shows its text; the actual outgoing payload contains the quote.

## Dark sidebar and account display follow-up (2026-11-18)
User asked to keep the dark theme, move category controls from the top down to the bottom, show the account used to start conversations, and hide contacts until searched. Restored Iced's dark theme and dark message colors; moved Chats/Status/Channels into a vertical group at the bottom of the sidebar; the chat list now hides contacts with no message history until the search field is non-empty; updated the search hint accordingly. The daemon now includes the linked account's push name in connected state, falling back to its linked JID, and the sidebar shows that account above the search/chat list. `rustfmt` completed and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded. This checks compilation only; no runtime UI review was performed.

## Double-click reply and dark chat panel (2026-11-18)
User requested replying by double-clicking a chat message like WhatsApp and applying the dark theme to the chat window. Added a 500 ms same-message double-click detector on message bubbles that sets the quoted reply target; the existing explicit Reply button remains available. Adjusted message timestamp and chat-header secondary text contrast for dark mode. The chat pane and message bubbles already had explicit dark backgrounds from the prior theme request and remain dark. `rustfmt` and `cargo check -p whatsapp-ui` succeeded. No desktop interaction test was performed.

## Minimal chat action menus (2026-11-18)
User asked to remove the always-visible Reply control and place actions behind three-dot menus like WhatsApp, citing the attached chat-header screenshot. Replaced the per-message Reply button with a compact `⋮` menu that reveals Reply (and document Open file when relevant); double-click reply remains. Replaced the always-visible Delete chat / Sync this chat buttons with a chat-header `⋮` menu. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no visual runtime review was done.

## Combined search and horizontal categories (2026-11-18)
User requested replacing the two sidebar search/new-chat inputs with one field that does both jobs and laying the category buttons horizontally (image showed the stacked category controls). Removed the separate new-chat input/state. The unified search field filters existing chats and contacts; for a valid 8-15 digit international number, it offers `Start chat with +...` and Enter starts that chat. Moved Chats/Status/Channels into a horizontal row. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no live UI review was done.

## Per-message menu chevron (2026-11-18)
User asked to replace the per-message triple-dot button shown below bubbles in attached screenshots with a WhatsApp-like down arrow. Replaced it with a subtle down chevron beside each message bubble; it opens the same Reply/Open file menu. Chat-header menu is unchanged. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no visual runtime review was done.

## Older-history action in chat menu (2026-11-18)
User pointed out the remaining standalone “Ask phone for older messages” button and asked to move it into the chat triple-dot menu. Removed that in-conversation button and added the action to the chat menu when no cached older page is available; “Load older” remains in the conversation when cached pages exist. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no live UI review was done.

## Window title cleanup (2026-11-18)
User asked to remove “unofficial” from the title shown in the attached screenshot. Changed the desktop window title to “WhatsApp”. Not separately compiled for this string-only edit.

## Open inline images (2026-11-18)
User reported that clicking an image in chat did nothing. Wrapped inline image previews in a click target that invokes the existing `xdg-open` handler, opening the local image in the desktop's default viewer. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no runtime click was tested.

## Duplicate contact rows with matching avatars (2026-11-18)
User screenshot showed the same apparent contact twice: one raw `@lid` identifier and one named alias. The backend already merges aliases when WhatsApp supplies the phone-number link; the screenshot pair lacked that link. UI now hashes downloaded avatar bytes and collapses list rows only when a raw-name identity and a friendly-name identity have identical image bytes. It labels the survivor with the friendly name and keeps the alias with the latest activity as the open target. `rustfmt` and `cargo check -p whatsapp-ui` succeeded. This is a visual list merge; messages stored under a different unlinked JID are not merged into the open history.

## Visual polish pass (2026-11-18)
User asked to make the interface more aesthetic. Updated the dark palette toward WhatsApp charcoal/teal, introduced consistent subdued and green primary button styles, softened panels with rounded corners and spacing, clarified contact/conversation sublabels, and styled profile, chat header, composer, reply preview, and sidebar rows as cohesive surfaces. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no live window screenshot review was performed.

## Reference theme pass (2026-11-18)
User supplied a WhatsApp desktop screenshot and asked for the UI theme to match it. Reworked the sidebar hierarchy with a WhatsApp brand/header row, quick menu and new-chat action, rounded unified search, horizontal active category chips near the top, and account card at the bottom. Increased chat-list width and styled rows with message previews. Added the latest message text to the chats IPC response; one-to-one incoming bubbles no longer show the redundant “Contact:” prefix (group messages retain sender labels). The main area keeps the charcoal background with green outgoing bubbles and accent actions. `rustfmt` and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded; no live screenshot review was performed.

## Screenshot-matched layout correction (2026-11-18)
User said the preceding theme pass was still far from the supplied WhatsApp screenshot and asked for all controls, chat rows, and profile to match. Replaced the teal-filled chat cards with flat dark rows and hover states, added a narrow left navigation rail with functional Chats/Status/Channels icons and account initial, moved the account identity into a flat bottom profile row, moved the app brand/actions to the top of the chat sidebar, and aligned category chips below the unified rounded search field. Unified buttons around charcoal surfaces with green primary actions; rounded the search/composer fields and changed the conversation canvas to neutral charcoal. `rustfmt` and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded; no running-window visual review was performed.

## Match the reference inside conversations (2026-11-18)
User attached screenshots of the current sidebar and reference WhatsApp chat panes and asked to apply the same appearance inside the chat window. Reduced conversation spacing, changed message text to use content sizing, capped bubbles at 500 px, and retained left/right alignment; this prevents each bubble from stretching across the whole chat pane. The dark charcoal conversation surface, green outgoing bubbles, dark incoming bubbles, header, and rounded composer remain consistent with the reference. `rustfmt` and `cargo check -p whatsapp-ui` succeeded; no runtime screenshot review was performed.

## Move account profile and sync-all menu (2026-11-18)
User said the profile was duplicated (initial on the rail plus account card at the bottom), the photo did not match, and requested the account in the sidebar and Sync all chats under the three-dot menu. Removed the bottom account card; account name now sits beneath WhatsApp in the chat sidebar. The rail bottom uses the linked account's actual WhatsApp profile image: daemon reads the linked JID, asynchronously fetches the profile picture, saves it with existing avatar handling, and publishes its local path in connected state; initial remains the fallback when no picture is available. Moved Sync all chats and Show log into the sidebar `⋮` menu. `rustfmt` and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded. The avatar fetch and visual placement have not been runtime-tested.

## Circular avatars and remove search category chips (2026-11-18)
User clarified that profile photos must be circular like WhatsApp and the horizontal category chips under search should be removed. Added a cached circular PNG renderer (center crop + transparent circular mask) for the linked account avatar, contact rows, and selected-chat header. The account photo uses the daemon's asynchronously fetched profile image, with account initial fallback. Removed the horizontal category strip under search; Chats/Status/Channels remain reachable from the left rail. The duplicate account card remains removed; the account label stays under WhatsApp in the sidebar. `rustfmt` and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded; no runtime visual check was performed.

## Header initials and rail account label (2026-11-18)
User screenshot showed a green dot in a selected-chat header, account name still under WhatsApp, and requested the account name at the rail bottom with its photo. Replaced the chat-header dot fallback with the same circular contact initial used by chat-list rows; real circular photos are used when present. Removed the account name under the WhatsApp title and added it below the rail-bottom account avatar. The daemon now retains both own PN and LID and tries both when fetching the own profile photo; UI also searches both aliases for a cached avatar fallback. Category chips under search were removed in the prior pass; only rail navigation remains. `rustfmt` and `cargo check -p whatsapp-ui -p whatsapp-daemon` succeeded; account-photo runtime fetch has not been visually confirmed.

## Slim rail and account popover (2026-11-18)
User asked to remove the always-visible account name, reveal it in a small floating panel when the rail profile is tapped, replace the A fallback with the account image, and make the rail slimmer. Removed the name label from the rail; tapping the round profile opens a shadowed floating account-name card. Reduced the rail from 76 px to 52 px (plus outer padding) and retained the circular account image, using the account photo/fallback lookup established earlier. UI/daemon compile check succeeded with `cargo check -p whatsapp-ui -p whatsapp-daemon`; runtime profile-photo availability and popover positioning have not been visually checked.
