//! Local control socket: newline-delimited JSON, versioned envelope, bounded frames.
//! Request : {"v":1,"id":7,"type":"health" | "subscribe" | "history", ...}
//! Response: {"v":1,"id":7,"type":"...", ...}   Events: same shape with "id":null.
//! To avoid a gap, subscribe first, then fetch history, and dedup by message id.
use crate::protocol::{Incoming, Outgoing, State};
use crate::storage::{self, ChatRow, Row};
use diesel::SqliteConnection;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, broadcast, mpsc, oneshot, watch};
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

pub const VERSION: u32 = 1;
const MAX_FRAME: usize = 64 * 1024;
const MAX_CLIENTS: usize = 32;
const MAX_TEXT: usize = 8000; // bytes; well under the frame limit
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct Envelope {
    #[allow(dead_code)] // version is checked in parse() before this is built
    v: u32,
    id: u64,
    #[serde(flatten)]
    req: Request,
}

#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    Health,
    Subscribe,
    Chats,
    DeleteChat {
        chat: String,
    },
    FetchOlder {
        chat: String,
    },
    /// Backfill older messages from the phone: one chat, or every chat with cached messages.
    Sync {
        chat: Option<String>,
    },
    SyncStatus,
    SendText {
        chat: String,
        text: String,
        #[serde(default)]
        reply_to: Option<crate::protocol::ReplyContext>,
    },
    SendFile {
        chat: String,
        path: String,
    },
    History {
        chat: String,
        before: Option<(i64, i64)>,
        limit: Option<i64>,
    },
    CallStart {
        to: String,
    },
    CallAnswer,
    CallReject,
    CallHangup,
    CallMute {
        on: bool,
    },
    CallStatus,
    Typing {
        chat: String,
        state: String,
    },
    Edit {
        chat: String,
        id: String,
        text: String,
    },
    React {
        chat: String,
        id: String,
        emoji: String,
        #[serde(default)]
        from_me: bool,
        #[serde(default)]
        participant: Option<String>,
    },
    ChatAction {
        chat: String,
        action: String,
    },
    Block {
        chat: String,
        on: bool,
    },
    Star {
        chat: String,
        id: String,
        #[serde(default)]
        from_me: bool,
        #[serde(default)]
        participant: Option<String>,
        on: bool,
    },
    MarkRead {
        chat: String,
        read: bool,
    },
    DownloadMedia {
        chat: String,
        msg_id: String,
    },
}

#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Out {
    Health {
        state: State,
    },
    Subscribed,
    History {
        messages: Vec<Row>,
    },
    Chats {
        chats: Vec<ChatRow>,
    },
    Sent {
        id: String,
    },
    Downloaded {
        chat: String,
        id: String,
        media_path: String,
    },
    Deleted,
    Requested,
    SyncStatus {
        running: bool,
        asked: u32,
        rounds: u32,
        done: u32,
        total: u32,
    },
    ContactsChanged,
    /// Pushed to subscribed clients when a call rings, connects or ends.
    Call {
        call: crate::calls::CallEvent,
    },
    CallOk {
        message: String,
    },
    CallFailed {
        message: String,
    },
    OpOk {
        message: String,
    },
    OpFailed {
        message: String,
    },
    Message(Incoming),
    State {
        state: State,
    },
    Error {
        code: &'static str,
        message: &'static str,
    },
}

#[derive(Serialize)]
struct Frame {
    v: u32,
    id: Option<u64>,
    #[serde(flatten)]
    body: Out,
}

/// Runs `f` with the connected client and wraps the outcome as op_ok / op_failed.
async fn op<F, Fut>(ctx: &Ctx, f: F) -> Out
where
    F: FnOnce(Arc<whatsapp_rust::client::Client>) -> Fut,
    Fut: std::future::Future<Output = Result<String, String>>,
{
    match ctx.calls.client() {
        Ok(client) => match f(client).await {
            Ok(message) => Out::OpOk { message },
            Err(message) => Out::OpFailed { message },
        },
        Err(message) => Out::OpFailed { message },
    }
}

fn call_reply(r: Result<String, String>) -> Out {
    match r {
        Ok(message) => Out::CallOk { message },
        Err(message) => Out::CallFailed { message },
    }
}

fn err(code: &'static str, message: &'static str) -> Out {
    Out::Error { code, message }
}

fn parse(line: &str) -> Result<(u64, Request), Out> {
    // Check the version first so a newer client gets version_mismatch, not bad_request.
    #[derive(Deserialize)]
    struct V {
        v: u32,
    }
    let v = serde_json::from_str::<V>(line)
        .map_err(|_| err("bad_request", "invalid JSON frame"))?
        .v;
    if v != VERSION {
        return Err(err("version_mismatch", "unsupported protocol version"));
    }
    let e: Envelope =
        serde_json::from_str(line).map_err(|_| err("bad_request", "invalid request"))?;
    if let Request::History { chat, .. } = &e.req {
        if chat.is_empty() || chat.len() > 256 {
            return Err(err("bad_request", "invalid chat"));
        }
    }
    if let Request::DeleteChat { chat } = &e.req {
        if chat.is_empty() || chat.len() > 256 {
            return Err(err("bad_request", "invalid chat"));
        }
    }
    if let Request::FetchOlder { chat } = &e.req {
        if chat.is_empty() || chat.len() > 256 {
            return Err(err("bad_request", "invalid chat"));
        }
    }
    if let Request::Sync { chat: Some(chat) } = &e.req {
        if chat.is_empty() || chat.len() > 256 {
            return Err(err("bad_request", "invalid chat"));
        }
    }
    if let Request::SendText { chat, text, .. } = &e.req {
        if chat.is_empty() || chat.len() > 256 || text.trim().is_empty() || text.len() > MAX_TEXT {
            return Err(err("bad_request", "invalid message"));
        }
    }
    if let Request::CallStart { to } = &e.req {
        if to.trim().is_empty() || to.len() > 64 {
            return Err(err("bad_request", "invalid number"));
        }
    }
    let chat_ok = |c: &str| !c.is_empty() && c.len() <= 256;
    match &e.req {
        Request::Typing { chat, state } if !chat_ok(chat) || state.len() > 16 => {
            return Err(err("bad_request", "invalid typing request"));
        }
        Request::Edit { chat, id, text } if !chat_ok(chat) || id.is_empty() || id.len() > 128 || text.trim().is_empty() || text.len() > MAX_TEXT => {
            return Err(err("bad_request", "invalid edit"));
        }
        Request::React { chat, id, emoji, participant, .. }
            if !chat_ok(chat) || id.is_empty() || id.len() > 128 || emoji.len() > 32 || participant.as_ref().is_some_and(|p| p.len() > 256) =>
        {
            return Err(err("bad_request", "invalid reaction"));
        }
        Request::ChatAction { chat, action } if !chat_ok(chat) || action.len() > 16 => {
            return Err(err("bad_request", "invalid chat action"));
        }
        Request::Star { chat, id, participant, .. }
            if !chat_ok(chat) || id.is_empty() || id.len() > 128 || participant.as_ref().is_some_and(|p| p.len() > 256) =>
        {
            return Err(err("bad_request", "invalid star request"));
        }
        Request::MarkRead { chat, .. } if !chat_ok(chat) => {
            return Err(err("bad_request", "invalid chat"));
        }
        Request::Block { chat, .. } if !chat_ok(chat) => {
            return Err(err("bad_request", "invalid chat"));
        }
        _ => {}
    }
    if let Request::DownloadMedia { chat, msg_id } = &e.req {
        if !chat_ok(chat) || msg_id.is_empty() || msg_id.len() > 128 {
            return Err(err("bad_request", "invalid download request"));
        }
    }
    if let Request::SendFile { chat, path } = &e.req {
        // Absolute only: the daemon's working directory means nothing to the UI.
        if chat.is_empty()
            || chat.len() > 256
            || path.len() > 4096
            || !path.starts_with('/')
            || path.contains('\0')
        {
            return Err(err("bad_request", "invalid file request"));
        }
    }
    Ok((e.id, e.req))
}

#[derive(Clone)]
pub struct Ctx {
    pub state: watch::Receiver<State>,
    pub bus: broadcast::Sender<Incoming>,
    pub db: Arc<Mutex<SqliteConnection>>,
    pub cmds: mpsc::Sender<Outgoing>,
    pub contact_bus: broadcast::Sender<()>,
    pub sync: Arc<crate::sync::SyncState>,
    pub calls: Arc<crate::calls::CallHub>,
}

/// Directory 0700, socket 0600. Refuses to start if a live daemon already owns the socket.
pub async fn bind(path: &Path) -> std::io::Result<UnixListener> {
    if let Some(dir) = path.parent() {
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    if UnixStream::connect(path).await.is_ok() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "daemon already running",
        ));
    }
    let _ = std::fs::remove_file(path); // stale socket from a crash
    let l = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(l)
}

pub async fn serve(l: UnixListener, ctx: Ctx) {
    let slots = Arc::new(Semaphore::new(MAX_CLIENTS));
    loop {
        let s = match l.accept().await {
            Ok((s, _)) => s,
            Err(e) => {
                tracing::error!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let permit = match tokio::time::timeout(Duration::from_secs(5), slots.clone().acquire_owned()).await {
            Ok(Ok(p)) => p,
            _ => {
                tracing::warn!("client limit reached; dropping connection");
                continue;
            }
        };
        let ctx = ctx.clone();
        tokio::spawn(async move {
            client(s, ctx).await;
            drop(permit);
        });
    }
}

type Conn = Framed<UnixStream, LinesCodec>;

async fn send(f: &mut Conn, id: Option<u64>, body: Out) -> bool {
    let Ok(line) = serde_json::to_string(&Frame {
        v: VERSION,
        id,
        body,
    }) else {
        return false;
    };
    matches!(
        tokio::time::timeout(WRITE_TIMEOUT, f.send(line)).await,
        Ok(Ok(()))
    )
}

async fn client(s: UnixStream, ctx: Ctx) {
    let mut f = Framed::new(s, LinesCodec::new_with_max_length(MAX_FRAME));
    let mut bus: Option<broadcast::Receiver<Incoming>> = None;
    let mut contact_bus: Option<broadcast::Receiver<()>> = None;
    let mut call_bus: Option<broadcast::Receiver<crate::calls::CallEvent>> = None;
    let mut state = ctx.state.clone();
    loop {
        let (id, out) = tokio::select! {
            line = f.next() => match line {
                None | Some(Err(LinesCodecError::Io(_))) => return,
                Some(Err(_)) => {
                    send(&mut f, None, err("frame_too_large", "frame exceeds limit")).await;
                    return;
                }
                Some(Ok(l)) => match parse(&l) {
                    Err(e) => (None, e),
                    Ok((id, req)) => (Some(id), handle(req, &ctx, &mut bus, &mut contact_bus, &mut call_bus, &mut state).await),
                },
            },
            r = async { bus.as_mut().unwrap().recv().await }, if bus.is_some() => match r {
                Ok(m) => (None, Out::Message(m)),
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    (None, err("lagged", "events dropped; re-sync with history"))
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
            r = async { contact_bus.as_mut().unwrap().recv().await }, if contact_bus.is_some() => match r {
                Ok(()) | Err(broadcast::error::RecvError::Lagged(_)) => (None, Out::ContactsChanged),
                Err(broadcast::error::RecvError::Closed) => return,
            },
            r = async { call_bus.as_mut().unwrap().recv().await }, if call_bus.is_some() => match r {
                Ok(ev) => (None, Out::Call { call: ev }),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            },
            r = state.changed(), if bus.is_some() => {
                if r.is_err() { return }
                (None, Out::State { state: state.borrow_and_update().clone() })
            }
        };
        if !send(&mut f, id, out).await {
            return;
        }
    }
}

async fn handle(
    req: Request,
    ctx: &Ctx,
    bus: &mut Option<broadcast::Receiver<Incoming>>,
    contact_bus: &mut Option<broadcast::Receiver<()>>,
    call_bus: &mut Option<broadcast::Receiver<crate::calls::CallEvent>>,
    state: &mut watch::Receiver<State>,
) -> Out {
    match req {
        Request::Health => Out::Health {
            state: ctx.state.borrow().clone(),
        },
        Request::CallStart { to } => call_reply(ctx.calls.place(&to).await),
        Request::CallAnswer => call_reply(ctx.calls.answer().await),
        Request::CallReject => call_reply(ctx.calls.reject().await),
        Request::CallHangup => call_reply(ctx.calls.hangup().await),
        Request::CallMute { on } => call_reply(ctx.calls.mute(on)),
        Request::CallStatus => Out::CallOk { message: ctx.calls.status() },
        Request::Typing { chat, state } => {
            op(ctx, move |c| async move { crate::chatops::typing(&c, &chat, &state).await }).await
        }
        Request::Edit { chat, id, text } => {
            op(ctx, move |c| async move { crate::chatops::edit(&c, &chat, &id, &text).await }).await
        }
        Request::React { chat, id, emoji, from_me, participant } => {
            op(ctx, move |c| async move { crate::chatops::react(&c, &chat, &id, &emoji, from_me, participant).await }).await
        }
        Request::ChatAction { chat, action } => {
            op(ctx, move |c| async move { crate::chatops::chat_action(&c, &chat, &action).await }).await
        }
        Request::Star { chat, id, from_me, participant, on } => {
            op(ctx, move |c| async move { crate::chatops::star(&c, &chat, &id, from_me, participant, on).await }).await
        }
        Request::MarkRead { chat, read } => {
            op(ctx, move |c| async move { crate::chatops::mark_read(&c, &chat, read).await }).await
        }
        Request::Block { chat, on } => {
            op(ctx, move |c| async move { crate::chatops::block(&c, &chat, on).await }).await
        }
        Request::Subscribe => {
            *bus = Some(ctx.bus.subscribe());
            *contact_bus = Some(ctx.contact_bus.subscribe());
            *call_bus = Some(ctx.calls.events.subscribe());
            state.borrow_and_update();
            Out::Subscribed
        }
        Request::SendText {
            chat,
            text,
            reply_to,
        } => {
            let (reply, answer) = oneshot::channel();
            if ctx
                .cmds
                .try_send(Outgoing::Text {
                    chat,
                    text,
                    reply_to,
                    reply,
                })
                .is_err()
            {
                return err("busy", "send queue full");
            }
            match tokio::time::timeout(Duration::from_secs(15), answer).await {
                Ok(Ok(Ok(id))) => Out::Sent { id },
                Ok(Ok(Err(_))) => err("send_failed", "message not sent"),
                _ => err("timeout", "no answer; the message may not have been sent"),
            }
        }
        Request::Sync { chat } => {
            if !matches!(*ctx.state.borrow(), State::Connected { .. }) {
                return err("not_connected", "not connected to WhatsApp yet");
            }
            if crate::sync::start(ctx.clone(), chat) {
                Out::Requested
            } else {
                err("busy", "a sync is already running")
            }
        }
        Request::SyncStatus => {
            use std::sync::atomic::Ordering::SeqCst;
            let s = &ctx.sync;
            Out::SyncStatus {
                running: s.running.load(SeqCst),
                asked: s.asked.load(SeqCst),
                rounds: s.rounds.load(SeqCst),
                done: s.done.load(SeqCst),
                total: s.total.load(SeqCst),
            }
        }
        Request::SendFile { chat, path } => {
            let path = std::path::PathBuf::from(path);
            match tokio::fs::metadata(&path).await {
                Ok(m) if m.is_file() && m.len() <= crate::protocol::MAX_DOC_BYTES => {}
                Ok(m) if m.is_file() => return err("file_too_large", "file is larger than 25 MB"),
                _ => return err("not_a_file", "path is not a readable file"),
            }
            let (reply, answer) = oneshot::channel();
            if ctx
                .cmds
                .try_send(Outgoing::File { chat, path, reply })
                .is_err()
            {
                return err("busy", "send queue full");
            }
            // Uploads are slow; the text-send timeout would give up long before a big file is done.
            match tokio::time::timeout(Duration::from_secs(120), answer).await {
                Ok(Ok(Ok(id))) => Out::Sent { id },
                Ok(Ok(Err(_))) => err("send_failed", "file not sent"),
                _ => err("timeout", "no answer; the file may not have been sent"),
            }
        }
        Request::DownloadMedia { chat, msg_id } => {
            let chat2 = chat.clone();
            let id2 = msg_id.clone();
            let params = match query(ctx, move |c| storage::get_media_params(c, &chat2, &id2)).await {
                Some(Some(p)) => p,
                Some(None) => return err("not_found", "no download parameters saved for this message"),
                None => return err("internal", "query failed"),
            };
            let (reply, answer) = oneshot::channel();
            if ctx
                .cmds
                .try_send(Outgoing::DownloadMedia { chat: chat.clone(), id: msg_id.clone(), params, reply })
                .is_err()
            {
                return err("busy", "download queue full");
            }
            match tokio::time::timeout(Duration::from_secs(120), answer).await {
                Ok(Ok(Ok(path))) => {
                    let chat3 = chat.clone();
                    let id3 = msg_id.clone();
                    let path2 = path.clone();
                    let _ = query(ctx, move |c| {
                        storage::update_media_path(c, &chat3, &id3, &path2)
                    }).await;
                    let _ = ctx.bus.send(Incoming {
                        id: msg_id.clone(),
                        chat: chat.clone(),
                        sender: String::new(),
                        from_me: false,
                        ts: 0,
                        text: None,
                        media_kind: None,
                        media_path: Some(path.clone()),
                        historical: false,
                        media_params: None,
                    });
                    Out::Downloaded { chat, id: msg_id, media_path: path }
                }
                Ok(Ok(Err(_))) => err("download_failed", "media download failed"),
                _ => err("timeout", "download timed out"),
            }
        }
        Request::FetchOlder { chat } => {
            let anchor = match query(ctx, move |c| storage::oldest(c, &chat)).await {
                Some(Some(a)) => a,
                Some(None) => {
                    return err(
                        "no_messages",
                        "no cached message to start from; send or receive one first",
                    );
                }
                None => return err("internal", "query failed"),
            };
            let (reply, answer) = oneshot::channel();
            let cmd = Outgoing::Older {
                chat: anchor.chat,
                oldest_id: anchor.id,
                oldest_from_me: anchor.from_me,
                oldest_ts_ms: anchor.ts.saturating_mul(1000),
                count: 50,
                reply,
            };
            if ctx.cmds.try_send(cmd).is_err() {
                return err("busy", "request queue full");
            }
            match tokio::time::timeout(Duration::from_secs(15), answer).await {
                Ok(Ok(Ok(_))) => Out::Requested,
                Ok(Ok(Err(_))) => err("request_failed", "the phone could not be asked"),
                _ => err("timeout", "no answer from the protocol task"),
            }
        }
        Request::Chats => match query(ctx, storage::chats).await {
            Some(chats) => Out::Chats { chats },
            None => err("internal", "query failed"),
        },
        Request::DeleteChat { chat } => {
            match query(ctx, move |c| storage::delete_chat(c, &chat)).await {
                Some(()) => Out::Deleted,
                None => err("internal", "delete failed"),
            }
        }
        Request::History {
            chat,
            before,
            limit,
        } => {
            let before = before.unwrap_or((i64::MAX, i64::MAX));
            let limit = limit.unwrap_or(50).clamp(1, 200);
            match query(ctx, move |c| storage::page(c, &chat, before, limit)).await {
                Some(messages) => Out::History { messages },
                None => err("internal", "query failed"),
            }
        }
    }
}

/// Runs a read on the shared connection off the async threads.
pub(crate) async fn query<T: Send + 'static>(
    ctx: &Ctx,
    f: impl FnOnce(&mut SqliteConnection) -> diesel::QueryResult<T> + Send + 'static,
) -> Option<T> {
    let db = ctx.db.clone();
    let r = tokio::task::spawn_blocking(move || {
        let mut c = db.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut c)
    })
    .await;
    match r {
        Ok(Ok(v)) => Some(v),
        _ => {
            tracing::error!("db query failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(r: Result<(u64, Request), Out>) -> &'static str {
        match r {
            Err(Out::Error { code, .. }) => code,
            _ => "ok",
        }
    }

    #[test]
    fn parsing_and_versioning() {
        assert_eq!(
            parse(r#"{"v":1,"id":7,"type":"health"}"#).unwrap(),
            (7, Request::Health)
        );
        assert_eq!(
            code(parse(r#"{"v":2,"id":1,"type":"health"}"#)),
            "version_mismatch"
        );
        assert_eq!(
            code(parse(r#"{"v":2,"id":1,"type":"from_the_future"}"#)),
            "version_mismatch"
        );
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"nope"}"#)),
            "bad_request"
        );
        assert_eq!(code(parse("garbage")), "bad_request");
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"history","chat":""}"#)),
            "bad_request"
        );
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"history","chat":"a"}"#)),
            "ok"
        );
        assert_eq!(
            code(parse(
                r#"{"v":1,"id":1,"type":"send_text","chat":"a","text":"hi"}"#
            )),
            "ok"
        );
        assert_eq!(
            code(parse(
                r#"{"v":1,"id":1,"type":"send_text","chat":"a","text":"  "}"#
            )),
            "bad_request"
        );
        assert_eq!(
            code(parse(
                r#"{"v":1,"id":1,"type":"send_text","chat":"","text":"hi"}"#
            )),
            "bad_request"
        );
        let big = format!(
            r#"{{"v":1,"id":1,"type":"send_text","chat":"a","text":"{}"}}"#,
            "x".repeat(MAX_TEXT + 1)
        );
        assert_eq!(code(parse(&big)), "bad_request");
        assert_eq!(
            code(parse(
                r#"{"v":1,"id":1,"type":"send_file","chat":"a","path":"/tmp/x.pdf"}"#
            )),
            "ok"
        );
        assert_eq!(
            code(parse(
                r#"{"v":1,"id":1,"type":"send_file","chat":"a","path":"x.pdf"}"#
            )),
            "bad_request"
        );
        assert_eq!(
            code(parse(
                r#"{"v":1,"id":1,"type":"send_file","chat":"","path":"/tmp/x.pdf"}"#
            )),
            "bad_request"
        );
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"sync","chat":null}"#)),
            "ok"
        );
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"sync","chat":"a"}"#)),
            "ok"
        );
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"sync","chat":""}"#)),
            "bad_request"
        );
        assert_eq!(code(parse(r#"{"v":1,"id":1,"type":"sync_status"}"#)), "ok");
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"fetch_older","chat":"a"}"#)),
            "ok"
        );
        assert_eq!(
            code(parse(r#"{"v":1,"id":1,"type":"fetch_older","chat":""}"#)),
            "bad_request"
        );
    }

    #[test]
    fn response_shape() {
        let f = Frame {
            v: VERSION,
            id: Some(3),
            body: Out::Health {
                state: State::Starting,
            },
        };
        assert_eq!(
            serde_json::to_string(&f).unwrap(),
            r#"{"v":1,"id":3,"type":"health","state":{"kind":"starting"}}"#
        );
    }
}
