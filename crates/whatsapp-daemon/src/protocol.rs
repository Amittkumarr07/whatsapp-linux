//! The only file that knows about whatsapp-rust. Swap the impl, nothing else changes.
use serde::Serialize;
use std::hash::{Hash, Hasher};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use wacore::download::MediaType;
use wacore::types::events::Event;
use whatsapp_rust::{TokioRuntime, bot::Bot, store::SqliteStore};
use whatsapp_rust_tokio_transport::TokioWebSocketTransportFactory;
use whatsapp_rust_ureq_http_client::UreqHttpClient;

const MAX_MEDIA_BYTES: u64 = 10 * 1024 * 1024;
const MAX_AVATAR_BYTES: u64 = 2 * 1024 * 1024;
const MAX_DOWNLOADS: usize = 3;
/// Documents are held in memory while (de)crypting, so this is a cap on RAM as well as on disk.
/// The error `run` returns after WhatsApp revoked the session and its files were deleted. main() matches it.
pub const REVOKED: &str =
    "WhatsApp revoked this session; the local session was deleted. Start again to pair.";
pub const MAX_DOC_BYTES: u64 = 25 * 1024 * 1024;

/// A sender-supplied file name is for display only and is never used as a path: control characters
/// are dropped and the length is capped.
fn clean_name(n: &str) -> String {
    n.chars().filter(|c| !c.is_control()).take(120).collect()
}

/// Extension for the stored copy: short lowercase alphanumerics only, so a hostile name cannot steer
/// the path. Extensions that a desktop may treat as launchable are stored as `.bin` (no handler).
fn safe_ext(name: &str) -> String {
    const RISKY: &[&str] = &[
        "desktop", "sh", "run", "appimage", "jar", "exe", "bat", "cmd", "scr", "msi", "com", "vbs",
        "js",
    ];
    name.rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .filter(|e| !e.is_empty() && e.len() <= 8 && e.bytes().all(|b| b.is_ascii_alphanumeric()))
        .filter(|e| !RISKY.contains(&e.as_str()))
        .unwrap_or_else(|| "bin".into())
}

/// Extension for a received document: from its file name, else from its MIME type, so a PDF whose
/// name has no ".pdf" is still stored as `.pdf` and the desktop knows which viewer to start.
fn ext_for_doc(name: &str, mime: Option<&str>) -> String {
    let ext = safe_ext(name);
    if ext != "bin" {
        return ext;
    }
    match mime.unwrap_or("") {
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        "text/csv" => "csv",
        "application/zip" => "zip",
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "application/msword" => "doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "application/vnd.ms-excel" => "xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xlsx",
        "application/vnd.ms-powerpoint" => "ppt",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation" => "pptx",
        _ => "bin",
    }
    .to_string()
}

fn mime_for(ext: &str) -> &'static str {
    match ext {
        "pdf" => "application/pdf",
        "txt" => "text/plain",
        "csv" => "text/csv",
        "zip" => "application/zip",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        _ => "application/octet-stream",
    }
}

/// What the chat shows for a document: its file name, then the caption if there is one.
fn doc_label(d: &waproto::whatsapp::message::DocumentMessage) -> Option<String> {
    let name = d
        .file_name
        .as_deref()
        .map(clean_name)
        .filter(|n| !n.trim().is_empty())?;
    Some(
        match d.caption.as_deref().filter(|c| !c.trim().is_empty()) {
            Some(c) => format!("{name}\n{c}"),
            None => name,
        },
    )
}

async fn download_document(
    client: &Arc<whatsapp_rust::client::Client>,
    downloads: &Arc<Semaphore>,
    dir: &Path,
    msg_id: &str,
    doc: &waproto::whatsapp::message::DocumentMessage,
) -> Option<String> {
    let Some(sha) = doc.file_sha256.as_deref().filter(|s| s.len() == 32) else {
        tracing::warn!(msg_id, "document has no usable checksum; not downloaded");
        return None;
    };
    // A missing length is not a reason to refuse: the real size is checked after the download.
    if doc.file_length.is_some_and(|l| l > MAX_DOC_BYTES) {
        tracing::warn!(msg_id, "document is larger than the {MAX_DOC_BYTES} byte cap; not downloaded");
        return None;
    }
    let _slot = downloads.acquire().await.ok()?;
    let bytes = match client.download(doc).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(msg_id, error = %e, "document download failed");
            return None;
        }
    };
    if bytes.len() as u64 > MAX_DOC_BYTES {
        tracing::warn!(msg_id, "downloaded document exceeds the size cap; dropped");
        return None;
    }
    // The hash is the sender's claim, so the message id is part of the name: a sender cannot make
    // their bytes land under the name another file would get.
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    let tag: String = msg_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(40)
        .collect();
    match store_file(
        dir,
        &format!("{hex}-{tag}"),
        &ext_for_doc(doc.file_name.as_deref().unwrap_or(""), doc.mimetype.as_deref()),
        &bytes,
    ) {
        Ok(p) => Some(p),
        Err(e) => {
            tracing::warn!(msg_id, error = %e, "could not save document");
            None
        }
    }
}

/// Reads, uploads and sends one file. Errors are generic strings: the cause goes to the log only.
async fn send_file(
    client: &Arc<whatsapp_rust::client::Client>,
    chat: &str,
    path: &Path,
) -> Result<(String, String), String> {
    use tokio::io::AsyncReadExt;
    let to = chat.parse().map_err(|_| "bad chat id".to_string())?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .map(clean_name)
        .filter(|n| !n.trim().is_empty())
        .ok_or_else(|| "bad file name".to_string())?;
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| "cannot open file".to_string())?;
    let meta = file
        .metadata()
        .await
        .map_err(|_| "cannot read file".to_string())?;
    if !meta.is_file() || meta.len() > MAX_DOC_BYTES {
        return Err("not a regular file, or too large".into());
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    // Read one byte past the cap: the file may have grown since the size check.
    file.take(MAX_DOC_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "cannot read file".to_string())?;
    if bytes.len() as u64 > MAX_DOC_BYTES {
        return Err("file too large".into());
    }
    let up = client
        .upload(bytes, MediaType::Document)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "upload failed");
            "upload failed".to_string()
        })?;
    let doc = waproto::whatsapp::message::DocumentMessage {
        url: Some(up.url),
        direct_path: Some(up.direct_path),
        media_key: Some(up.media_key),
        file_enc_sha256: Some(up.file_enc_sha256),
        file_sha256: Some(up.file_sha256),
        file_length: Some(up.file_length),
        mimetype: Some(mime_for(&safe_ext(&name)).to_string()),
        file_name: Some(name.clone()),
        ..Default::default()
    };
    let msg = waproto::whatsapp::Message {
        document_message: Some(Box::new(doc)),
        ..Default::default()
    };
    let id = client.send_message(to, msg).await.map_err(|e| {
        tracing::warn!(error = %e, "send failed");
        "send failed".to_string()
    })?;
    Ok((id, name))
}

/// WhatsApp wraps many messages in an envelope: a document that has a caption, a message sent from
/// your own phone, a disappearing-chat message. The payload (e.g. `document_message`) lives inside
/// the envelope, so looking only at the top level misses it and the file is never downloaded.
/// `Borrow` lets this work whether the generated field is boxed or not.
fn unwrap_message(m: &waproto::whatsapp::Message) -> &waproto::whatsapp::Message {
    use std::borrow::Borrow;
    fn inner<B: Borrow<waproto::whatsapp::Message>>(o: &Option<B>) -> Option<&waproto::whatsapp::Message> {
        o.as_ref().map(|b| b.borrow())
    }
    let mut cur = m;
    for _ in 0..4 {
        let next = cur
            .document_with_caption_message
            .as_ref()
            .and_then(|f| inner(&f.message))
            .or_else(|| cur.ephemeral_message.as_ref().and_then(|f| inner(&f.message)))
            .or_else(|| cur.view_once_message.as_ref().and_then(|f| inner(&f.message)))
            .or_else(|| cur.device_sent_message.as_ref().and_then(|d| inner(&d.message)));
        match next {
            Some(n) => cur = n,
            None => break,
        }
    }
    cur
}

fn media_kind(m: &waproto::whatsapp::Message) -> Option<&'static str> {
    if m.image_message.is_some() {
        Some("image")
    } else if m.video_message.is_some() {
        Some("video")
    } else if m.audio_message.is_some() {
        Some("audio")
    } else if m.document_message.is_some() {
        Some("document")
    } else if m.sticker_message.is_some() {
        Some("sticker")
    } else {
        None
    }
}

async fn download_image(
    client: &Arc<whatsapp_rust::client::Client>,
    downloads: &Arc<Semaphore>,
    dir: &Path,
    img: &waproto::whatsapp::message::ImageMessage,
) -> Option<String> {
    let sha = img.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    if img.file_length.unwrap_or(u64::MAX) > MAX_MEDIA_BYTES {
        return None;
    }
    let _slot = downloads.acquire().await.ok()?;
    let bytes = client.download(img).await.ok()?;
    if bytes.len() as u64 > MAX_MEDIA_BYTES {
        return None;
    }
    store_media(dir, sha, img.mimetype.as_deref(), &bytes).ok()
}

#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum State {
    Starting,
    Pairing {
        qr: String,
    },
    Connected {
        account: Option<String>,
        account_jid: Option<String>,
        account_pn: Option<String>,
        account_lid: Option<String>,
        account_avatar_path: Option<String>,
    },
}

impl State {
    pub fn name(&self) -> &'static str {
        match self {
            State::Starting => "starting",
            State::Pairing { .. } => "pairing",
            State::Connected { .. } => "connected",
        }
    }
}

// Redacted on purpose: the QR payload is a pairing secret and must never reach a log.
impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Protocol-neutral message. Text and image captions only; other media is recorded by kind.
#[derive(Clone, Debug, Serialize)]
pub struct Incoming {
    pub id: String,
    pub chat: String,
    pub sender: String,
    pub from_me: bool,
    pub ts: i64,
    pub text: Option<String>,
    pub media_kind: Option<String>,
    pub media_path: Option<String>, // set only for images that were downloaded
    /// Imported from history sync: stored quietly (no notification, no live event, no image download).
    #[serde(skip)]
    pub historical: bool,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct ReplyContext {
    pub id: String,
    pub sender: String,
    pub from_me: bool,
    pub text: Option<String>,
    pub media_kind: Option<String>,
}

/// A name from the phone's address book. Stored locally in cache.db.
#[derive(Clone)]
pub struct Contact {
    pub jid: String,
    pub name: String,
    /// For a @lid id: the phone-number id of the same person, so the two show up as one chat.
    pub pn: Option<String>,
    pub avatar_path: Option<String>,
}

/// Work for the protocol task. The result is a message id, or a generic failure.
pub enum Outgoing {
    Text {
        chat: String,
        text: String,
        reply_to: Option<ReplyContext>,
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Upload a local file and send it as a document.
    File {
        chat: String,
        path: PathBuf,
        reply: oneshot::Sender<Result<String, String>>,
    },
    /// Ask the phone for messages older than the anchor message. The phone answers later through a
    /// history-sync event, which is imported like any other.
    Older {
        chat: String,
        oldest_id: String,
        oldest_from_me: bool,
        oldest_ts_ms: i64,
        count: i32,
        reply: oneshot::Sender<Result<String, String>>,
    },
}

pub type Res = Result<(), Box<dyn std::error::Error>>;

pub trait Protocol {
    /// Runs until the connection ends; reports health via `state`, messages via `out`.
    async fn run(
        self,
        state: watch::Sender<State>,
        out: mpsc::Sender<Incoming>,
        cmds: mpsc::Receiver<Outgoing>,
        contacts: mpsc::Sender<Contact>,
    ) -> Res;
}

pub struct WhatsApp {
    pub db: PathBuf,
    pub media_dir: PathBuf,
    pub cached_contacts: Vec<Contact>,
}

fn fetch_avatar(
    client: Arc<whatsapp_rust::client::Client>,
    contacts: mpsc::Sender<Contact>,
    slots: Arc<Semaphore>,
    dir: PathBuf,
    jid: String,
    name: String,
    pn: Option<String>,
) {
    tokio::spawn(async move {
        let Ok(_slot) = slots.acquire_owned().await else {
            return;
        };
        let Ok(parsed) = jid.parse() else { return };
        let picture = match client.contacts().get_profile_picture(&parsed, true).await {
            Ok(Some(p)) => p,
            Ok(None) => return,
            Err(e) => {
                tracing::debug!(error = %e, "profile picture lookup failed");
                return;
            }
        };
        let url = picture.url;
        let save_dir = dir.clone();
        let key = jid.clone();
        let path = tokio::task::spawn_blocking(move || download_avatar(&url, &save_dir, &key))
            .await
            .ok()
            .flatten();
        if let Some(avatar_path) = path {
            let _ = contacts
                .send(Contact {
                    jid,
                    name,
                    pn,
                    avatar_path: Some(avatar_path),
                })
                .await;
        }
    });
}

fn download_avatar(url: &str, dir: &Path, jid: &str) -> Option<String> {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(12)))
        .build();
    let agent = ureq::Agent::new_with_config(config);
    let mut response = agent.get(url).call().ok()?;
    let bytes = response
        .body_mut()
        .with_config()
        .limit(MAX_AVATAR_BYTES)
        .read_to_vec()
        .ok()?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_AVATAR_BYTES {
        return None;
    }
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    jid.hash(&mut hash);
    let path = dir.join(format!("avatar-{:016x}.jpg", hash.finish()));
    let mut part = path.clone();
    part.set_extension("jpg.part");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&part)
        .ok()?;
    use std::io::Write;
    if file.write_all(&bytes).is_err() {
        let _ = std::fs::remove_file(&part);
        return None;
    }
    drop(file);
    std::fs::rename(&part, &path).ok()?;
    path.to_str().map(str::to_string)
}

impl Protocol for WhatsApp {
    async fn run(
        self,
        state: watch::Sender<State>,
        out: mpsc::Sender<Incoming>,
        cmds: mpsc::Receiver<Outgoing>,
        contacts: mpsc::Sender<Contact>,
    ) -> Res {
        // Taken once, on the first Connected event, by the send loop below.
        let cmds = Arc::new(Mutex::new(Some(cmds)));
        let backend =
            Arc::new(SqliteStore::new(self.db.to_str().ok_or("non-UTF-8 data path")?).await?);
        let downloads = Arc::new(Semaphore::new(MAX_DOWNLOADS)); // a chat can't flood us with fetches
        let media_dir = self.media_dir;
        let cached_contacts = self.cached_contacts;
        let avatar_downloads = Arc::new(Semaphore::new(3));
        let avatar_bootstrap_done = Arc::new(AtomicBool::new(false));
        let logged_out = Arc::new(AtomicBool::new(false));
        let logged_out_handler = logged_out.clone();

        let mut bot = Bot::builder()
            .with_backend(backend)
            .with_transport_factory(TokioWebSocketTransportFactory::new())
            .with_http_client(UreqHttpClient::new())
            .with_runtime(TokioRuntime)
            .on_event(move |event, client| {
                let (state, out) = (state.clone(), out.clone());
                let (downloads, dir) = (downloads.clone(), media_dir.clone());
                let avatar_downloads = avatar_downloads.clone();
                let cached_contacts = cached_contacts.clone();
                let avatar_bootstrap_done = avatar_bootstrap_done.clone();
                let cmds = cmds.clone();
                let logged_out = logged_out_handler.clone();
                let contacts = contacts.clone();
                async move {
                    // Event names only (never contents), to see what the library actually delivers.
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        let dbg = format!("{event:?}");
                        let kind = dbg.split(|c: char| c == '(' || c == ' ' || c == '{').next().unwrap_or("?");
                        tracing::debug!(kind, "event");
                    }
                    match event {
                        Event::PairingQrCode { code, .. } => {
                            // Terminal only: under systemd stdout is the journal, i.e. a log.
                            if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
                                println!("QR:\n{code}");
                            }
                            state.send_replace(State::Pairing { qr: code.to_string() });
                        }
                        Event::Connected(_) => {
                            let device = client.persistence_manager().get_device_snapshot().await;
                            let account_pn = device.pn.as_ref().map(ToString::to_string);
                            let account_lid = device.lid.as_ref().map(ToString::to_string);
                            let account_jid = account_pn.clone().or_else(|| account_lid.clone());
                            let account = if !device.push_name.trim().is_empty() {
                                Some(device.push_name.clone())
                            } else {
                                account_jid.clone()
                            };
                            state.send_replace(State::Connected {
                                account: account.clone(),
                                account_jid: account_jid.clone(),
                                account_pn: account_pn.clone(),
                                account_lid: account_lid.clone(),
                                account_avatar_path: None,
                            });
                            if account_jid.is_some() {
                                let (client, state, dir) = (client.clone(), state.clone(), dir.clone());
                                let (account_pn, account_lid) = (account_pn.clone(), account_lid.clone());
                                tokio::spawn(async move {
                                    let mut found = None;
                                    for jid in [account_pn.clone(), account_lid.clone()].into_iter().flatten() {
                                        let Ok(parsed) = jid.parse() else { continue };
                                        let Ok(Some(picture)) = client.contacts().get_profile_picture(&parsed, true).await else { continue };
                                        let url = picture.url;
                                        let jid_for_file = jid.clone();
                                        let save_dir = dir.clone();
                                        let path = tokio::task::spawn_blocking(move || download_avatar(&url, &save_dir, &jid_for_file))
                                            .await.ok().flatten();
                                        if path.is_some() { found = path; break; }
                                    }
                                    let path = found;
                                    if let Some(account_avatar_path) = path {
                                        state.send_replace(State::Connected {
                                            account,
                                            account_jid,
                                            account_pn,
                                            account_lid,
                                            account_avatar_path: Some(account_avatar_path),
                                        });
                                    }
                                });
                            }
                            if !avatar_bootstrap_done.swap(true, Ordering::SeqCst) {
                                for contact in cached_contacts {
                                    fetch_avatar(client.clone(), contacts.clone(), avatar_downloads.clone(), dir.clone(),
                                        contact.jid, contact.name, contact.pn);
                                }
                            }
                            let rx = cmds.lock().unwrap_or_else(|p| p.into_inner()).take();
                            if let Some(mut rx) = rx {
                                // Send loop. It lives with the process; while offline, send_message
                                // fails and that failure goes back to the caller.
                                let (client, out) = (client.clone(), out.clone());
                                tokio::spawn(async move {
                                    while let Some(cmd) = rx.recv().await {
                                        let (chat, text, reply_to, reply) = match cmd {
                                            Outgoing::Text { chat, text, reply_to, reply } => (chat, text, reply_to, reply),
                                            Outgoing::File { chat, path, reply } => {
                                                let r = match send_file(&client, &chat, &path).await {
                                                    Ok((id, name)) => {
                                                        let ts = std::time::SystemTime::now()
                                                            .duration_since(std::time::UNIX_EPOCH)
                                                            .map_or(0, |d| d.as_secs() as i64);
                                                        let _ = out.send(Incoming {
                                                            id: id.clone(), chat: chat.clone(), sender: "me".into(), from_me: true, ts,
                                                            text: Some(name), media_kind: Some("document".into()),
                                                            media_path: path.to_str().map(str::to_string), historical: false,
                                                        }).await;
                                                        Ok(id)
                                                    }
                                                    Err(e) => Err(e),
                                                };
                                                let _ = reply.send(r);
                                                continue;
                                            }
                                            Outgoing::Older { chat, oldest_id, oldest_from_me, oldest_ts_ms, count, reply } => {
                                                let r = async {
                                                    let jid = chat.parse().map_err(|_| "bad chat id".to_string())?;
                                                    client
                                                        .fetch_message_history(&jid, &oldest_id, oldest_from_me, oldest_ts_ms, count)
                                                        .await
                                                        .map_err(|e| {
                                                            tracing::warn!(error = %e, "history request failed");
                                                            "request failed".to_string()
                                                        })
                                                }
                                                .await;
                                                let _ = reply.send(r);
                                                continue;
                                            }
                                        };
                                        let r = async {
                                            let to = chat.parse().map_err(|_| "bad chat id".to_string())?;
                                            let msg = if let Some(q) = reply_to {
                                                let quoted_text = q.text.clone().or_else(|| q.media_kind.clone().map(|k| format!("[{k}]"))).unwrap_or_default();
                                                let quoted = waproto::whatsapp::Message { conversation: Some(quoted_text), ..Default::default() };
                                                let context = waproto::whatsapp::ContextInfo {
                                                    stanza_id: Some(q.id), participant: (!q.from_me).then_some(q.sender),
                                                    quoted_message: Some(Box::new(quoted)), remote_jid: Some(chat.clone()),
                                                    ..Default::default()
                                                };
                                                waproto::whatsapp::Message {
                                                    extended_text_message: Some(Box::new(waproto::whatsapp::message::ExtendedTextMessage {
                                                        text: Some(text.clone()), context_info: Some(Box::new(context)), ..Default::default()
                                                    })), ..Default::default()
                                                }
                                            } else { waproto::whatsapp::Message { conversation: Some(text.clone()), ..Default::default() } };
                                            client.send_message(to, msg).await.map_err(|e| {
                                                tracing::warn!(error = %e, "send failed");
                                                "send failed".to_string()
                                            })
                                        }
                                        .await;
                                        if let Ok(id) = &r {
                                            // Persist our own message so the UI sees it; a library echo
                                            // of the same id is deduplicated by the primary key.
                                            let ts = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .map_or(0, |d| d.as_secs() as i64);
                                            let m = Incoming {
                                                id: id.clone(),
                                                chat: chat.clone(),
                                                sender: "me".into(),
                                                from_me: true,
                                                ts,
                                                text: Some(text.clone()),
                                                media_kind: None,
                                                media_path: None,
                                                historical: false,
                                            };
                                            let _ = out.send(m).await;
                                        }
                                        let _ = reply.send(r);
                                    }
                                });
                            }
                        }
                        Event::ContactUpdate(c) => {
                            let name = c
                                .action
                                .full_name
                                .clone()
                                .or_else(|| c.action.first_name.clone())
                                .or_else(|| c.action.username.clone())
                                .filter(|n| !n.trim().is_empty());
                            // Keep contact actions even without a saved display name, and retain
                            // both phone-number and LID identifiers when WhatsApp supplies them.
                            let id = c.jid.to_string();
                            let pn = c.action.pn_jid.clone().or_else(|| id.ends_with("@s.whatsapp.net").then(|| id.clone()));
                            let name = name.unwrap_or_else(|| id.clone());
                            let ids = [Some((id, pn.clone())), c.action.lid_jid.clone().map(|l| (l, pn.clone())), c.action.pn_jid.clone().map(|p| (p, None))];
                            for (jid, pn) in ids.into_iter().flatten() {
                                if contacts.send(Contact { jid: jid.clone(), name: name.clone(), pn: pn.clone(), avatar_path: None }).await.is_err() {
                                    tracing::error!("contact writer is gone");
                                    break;
                                }
                            }
                            let id = c.jid.to_string();
                            fetch_avatar(client.clone(), contacts.clone(), avatar_downloads, dir.clone(), id, name, c.action.pn_jid.clone());
                        }
                        Event::PushNameUpdate(update) => {
                            let jid = update.jid.to_string();
                            let pn = jid.ends_with("@s.whatsapp.net").then(|| jid.clone());
                            if !update.new_push_name.trim().is_empty() {
                                let _ = contacts.send(Contact { jid: jid.clone(), name: update.new_push_name.clone(), pn: pn.clone(), avatar_path: None }).await;
                                fetch_avatar(client.clone(), contacts.clone(), avatar_downloads, dir.clone(), jid, update.new_push_name, pn);
                            }
                        }
                        Event::PictureUpdate(update) if !update.removed => {
                            let jid = update.jid.to_string();
                            let pn = jid.ends_with("@s.whatsapp.net").then(|| jid.clone());
                            fetch_avatar(client.clone(), contacts.clone(), avatar_downloads, dir.clone(), jid.clone(), jid, pn);
                        }
                        Event::HistorySync(sync) => {
                            // WhatsApp's linked-device bootstrap, and the answers to on-demand requests for
                            // older messages, arrive in chunks. They go through the same idempotent writer as
                            // live messages but are marked historical: stored quietly, never notified, and
                            // images are not downloaded (a chat can hold thousands of old ones).
                            let mut imported = 0usize;
                            let conversations = sync.conversations.len();
                            for info in sync.status_v3_messages {
                                let key = info.key;
                                let (Some(id), Some(chat), Some(msg)) = (key.id, key.remote_jid, info.message) else { continue };
                                let msg = unwrap_message(&msg);
                                let kind = media_kind(msg);
                                let mut body = text_of(msg);
                                if body.is_none() && kind.is_none() { continue; }
                                if let Some(img) = msg.image_message.as_deref() { body = body.or_else(|| img.caption.clone()); }
                                if let Some(d) = msg.document_message.as_deref() { body = body.or_else(|| doc_label(d)); }
                                let m = Incoming {
                                    id, chat: "status@broadcast".to_string(),
                                    sender: key.participant.or(info.participant).unwrap_or(chat),
                                    from_me: key.from_me.unwrap_or(false),
                                    ts: info.message_timestamp.unwrap_or(0) as i64,
                                    text: body, media_kind: kind.map(str::to_string), media_path: None, historical: true,
                                };
                                if out.send(m).await.is_err() { tracing::error!("history writer is gone"); return; }
                                imported += 1;
                            }
                            for conversation in sync.conversations {
                                let conversation_id = conversation.id.clone();
                                let conversation_name = conversation.name.clone().filter(|n| !n.trim().is_empty()).unwrap_or_else(|| conversation_id.clone());
                                let conversation_pn = conversation_id.ends_with("@s.whatsapp.net").then(|| conversation_id.clone());
                                let _ = contacts.send(Contact { jid: conversation_id.clone(), name: conversation_name.clone(), pn: conversation_pn.clone(), avatar_path: None }).await;
                                fetch_avatar(client.clone(), contacts.clone(), avatar_downloads.clone(), dir.clone(), conversation_id.clone(), conversation_name, conversation_pn);
                                for entry in conversation.messages {
                                    let Some(info) = entry.message else { continue };
                                    let key = info.key;
                                    let Some(id) = key.id else { continue };
                                    // The conversation id is the chat even when the key does not repeat it.
                                    let chat = key.remote_jid.unwrap_or_else(|| conversation_id.clone());
                                    let from_me = key.from_me.unwrap_or(false);
                                    let sender = if from_me { "me".to_string() } else {
                                        key.participant.clone().or_else(|| info.participant.clone()).unwrap_or_else(|| chat.clone())
                                    };
                                    if !from_me && let Some(name) = info.push_name.as_deref().filter(|n| !n.trim().is_empty()) {
                                        let pn = if sender.ends_with("@s.whatsapp.net") { Some(sender.clone()) }
                                            else if chat.ends_with("@s.whatsapp.net") { Some(chat.clone()) } else { None };
                                        let _ = contacts.send(Contact { jid: sender.clone(), name: name.to_string(), pn: pn.clone(), avatar_path: None }).await;
                                        if let Some(pn) = pn.filter(|p| p != &sender) {
                                            let _ = contacts.send(Contact { jid: pn, name: name.to_string(), pn: None, avatar_path: None }).await;
                                        }
                                    }
                                    let Some(msg) = info.message else { continue };
                                    let msg = unwrap_message(&msg);
                                    let kind = media_kind(msg);
                                    let mut body = text_of(msg);
                                    if body.is_none() && kind.is_none() { continue; }
                                    if let Some(img) = msg.image_message.as_deref() { body = body.or_else(|| img.caption.clone()); }
                                if let Some(d) = msg.document_message.as_deref() { body = body.or_else(|| doc_label(d)); }
                                    let m = Incoming {
                                        id, chat, sender, from_me,
                                        ts: info.message_timestamp.unwrap_or(0) as i64,
                                        text: body, media_kind: kind.map(str::to_string), media_path: None, historical: true,
                                    };
                                    if out.send(m).await.is_err() { tracing::error!("history writer is gone"); return; }
                                    imported += 1;
                                }
                            }
                            tracing::info!(conversations, messages = imported, "history sync chunk received");
                        }
                        Event::LoggedOut(_) => {
                            tracing::error!("WhatsApp revoked this session (logged out)");
                            logged_out.store(true, Ordering::SeqCst);
                            state.send_replace(State::Starting);
                        }
                        Event::Disconnected(_) => {
                            state.send_replace(State::Starting);
                        }
                        Event::Message(msg, info) => {
                            if !info.source.is_from_me && !info.push_name.trim().is_empty() {
                                let sender = info.source.sender.to_string();
                                let pn = if sender.ends_with("@s.whatsapp.net") { Some(sender.clone()) }
                                    else { info.source.sender_alt.as_ref().map(ToString::to_string).filter(|p| p.ends_with("@s.whatsapp.net")) };
                                let _ = contacts.send(Contact { jid: sender.clone(), name: info.push_name.clone(), pn: pn.clone(), avatar_path: None }).await;
                                if let Some(pn) = pn.filter(|p| p != &sender) {
                                    let _ = contacts.send(Contact { jid: pn, name: info.push_name.clone(), pn: None, avatar_path: None }).await;
                                }
                            }
                            let msg = unwrap_message(&msg);
                            let img = msg.image_message.as_deref().cloned();
                            let doc = msg.document_message.as_deref().cloned();
                            let kind = media_kind(msg);
                            let mut m = Incoming {
                                id: info.id.clone(),
                                chat: info.source.chat.to_string(),
                                sender: info.source.sender.to_string(),
                                from_me: info.source.is_from_me,
                                ts: info.timestamp.timestamp(),
                                text: text_of(msg),
                                media_kind: kind.map(str::to_string),
                                media_path: None,
                                historical: false,
                            };
                            tracing::info!(
                                from_me = m.from_me,
                                has_text = m.text.is_some(),
                                media = ?m.media_kind,
                                domain = m.chat.rsplit('@').next().unwrap_or("?"),
                                "message event"
                            );
                            // Internal protocol traffic (key shares, sync notices) has no text and no media.
                            // Storing it shows up as a wall of "[non-text message]" lines.
                            if m.text.is_none() && m.media_kind.is_none() {
                                tracing::debug!("skipping message with no user-visible content");
                                return;
                            }
                            if let Some(doc) = doc {
                                m.text = m.text.or_else(|| doc_label(&doc));
                                // Off the event path, like images: a slow download never stalls other events.
                                tokio::spawn(async move {
                                    let path = download_document(&client, &downloads, &dir, &m.id, &doc).await;
                                    m.media_path = path;
                                    if out.send(m).await.is_err() {
                                        tracing::error!("message writer is gone");
                                    }
                                });
                                return;
                            }
                            let Some(img) = img else {
                                if out.send(m).await.is_err() {
                                    tracing::error!("message writer is gone");
                                }
                                return;
                            };
                            m.text = m.text.or_else(|| img.caption.clone());
                            // Fetch off the event path so a slow download never stalls other events.
                            tokio::spawn(async move {
                                m.media_path = download_image(&client, &downloads, &dir, &img).await;
                                if out.send(m).await.is_err() {
                                    tracing::error!("message writer is gone");
                                }
                            });
                        }
                        _ => {}
                    }
                }
            })
            .build()
            .await?;

        let run_error = match bot.run().await {
            Ok(handle) => handle.await.err().map(|e| e.to_string()),
            Err(e) => Some(e.to_string()),
        };
        // The loop only ends when the connection is dead for good, so never report that as success:
        // exiting non-zero lets a service manager restart us (and show a fresh pairing QR).
        if logged_out.load(Ordering::SeqCst) {
            for ext in ["", "-wal", "-shm"] {
                let mut p = self.db.clone().into_os_string();
                p.push(ext);
                let _ = std::fs::remove_file(p); // the revoked session is useless; don't keep its secrets
            }
            return Err(REVOKED.into());
        }
        if let Some(error) = run_error {
            return Err(error.into());
        }
        Err("WhatsApp connection loop ended unexpectedly".into())
    }
}

/// Content-addressed (hex of the 32-byte sha256, so the name is path-safe), written 0600 via
/// a .part file and an atomic rename, so an interrupted download never leaves a half file.
fn store_media(
    dir: &Path,
    sha: &[u8],
    mime: Option<&str>,
    bytes: &[u8],
) -> std::io::Result<String> {
    let ext = match mime {
        Some("image/jpeg") => "jpg",
        Some("image/png") => "png",
        Some("image/webp") => "webp",
        Some("image/gif") => "gif",
        _ => "bin",
    };
    let name: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    store_file(dir, &name, ext, bytes)
}

/// `name` and `ext` must already be path-safe. Written 0600 through a .part file.
fn store_file(dir: &Path, name: &str, ext: &str, bytes: &[u8]) -> std::io::Result<String> {
    use std::io::Write;
    let path = dir.join(format!("{name}.{ext}"));
    if path.exists() {
        return Ok(path.to_string_lossy().into_owned());
    }
    let tmp = dir.join(format!("{name}.part"));
    let r = (|| {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        std::fs::rename(&tmp, &path)
    })();
    if r.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    r.map(|_| path.to_string_lossy().into_owned())
}

/// Plain text, reply/link text, or text wrapped in the envelope a linked device receives for
/// messages that your own phone sent.
fn text_of(m: &waproto::whatsapp::Message) -> Option<String> {
    m.conversation
        .clone()
        .or_else(|| {
            m.extended_text_message
                .as_deref()
                .and_then(|e| e.text.clone())
        })
        .or_else(|| {
            m.device_sent_message
                .as_deref()
                .and_then(|d| d.message.as_deref())
                .and_then(text_of)
        })
}
