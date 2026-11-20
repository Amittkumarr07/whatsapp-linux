# Audio calls (built into whatsapp-daemon)

Calls run inside the chat daemon: one process, one WhatsApp login. The daemon is on the
`whatsapp-rust` 0.7 line (the first with VoIP); the `voip-mlow` feature brings the pure-Rust codec.

## System packages
    sudo apt install libasound2-dev pkg-config

## Use (no UI yet; the daemon must be running)
    whatsapp-daemon ctl call 919876543210     # country code + number, no "+"
    whatsapp-daemon ctl answer | reject | hangup | mute | unmute | status
    # or: cargo run -p whatsapp-daemon -- ctl status

Incoming calls raise a desktop notification and ring on the phone too.

## Socket protocol (for the UI)
Requests: `call_start {to}`, `call_answer`, `call_reject`, `call_hangup`, `call_mute {on}`,
`call_status`. Replies: `call_ok {message}` or `call_failed {message}`.
After `subscribe`, the daemon pushes `{"type":"call","call":{"state":"ringing"|"dialing"|"active"|"ended", ...}}`.

## Limits
1:1 audio only. Group calls are rejected, video offers are answered audio-only, no echo
cancellation (use headphones). Unofficial client: use a spare number first.

## Upgrading from 0.5
The 0.7 storage layer may not read a 0.5 session. Back up the data directory first
(`~/.local/share/whatsapp-linux-client/`); if the daemon asks for a QR, scan it again.

## Chat controls (terminal, until the UI has buttons)
    whatsapp-daemon ctl typing <chat> composing|recording|paused
    whatsapp-daemon ctl edit <chat> <message-id> <new text>
    whatsapp-daemon ctl react <chat> <message-id> <emoji> [mine]
    whatsapp-daemon ctl archive|unarchive|pin|unpin|mute|unmute <chat>
    whatsapp-daemon ctl block|unblock <chat>
`<chat>` is the id the daemon stores, e.g. `919876543210@s.whatsapp.net`. Socket requests:
`typing`, `edit`, `react`, `chat_action`, `block`; replies are `op_ok` / `op_failed`.
    whatsapp-daemon ctl star|unstar <chat> <message-id> [mine]
    whatsapp-daemon ctl read|unread <chat>
