//! The only file that knows about whatsapp-rust. Swap the impl, nothing else changes.
use serde::Serialize;
use base64::prelude::*;
use std::hash::{Hash, Hasher};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use wacore::download::MediaType;
use wacore::types::events::Event;
use whatsapp_rust::prelude::{Bot, SqliteStore};

const MAX_MEDIA_BYTES: u64 = 10 * 1024 * 1024;
const MAX_AVATAR_BYTES: u64 = 2 * 1024 * 1024;
const MAX_DOWNLOADS: usize = 3;
pub const REVOKED: &str =
    "WhatsApp revoked this session; the local session was deleted. Start again to pair.";
pub const MAX_DOC_BYTES: u64 = 25 * 1024 * 1024;

fn bare_jid(jid: &str) -> String {
    match jid.split_once('@') {
        Some((user, server)) => format!("{}@{server}", user.split(':').next().unwrap_or(user)),
        None => jid.to_string(),
    }
}

fn clean_name(n: &str) -> String {
    n.chars().filter(|c| !c.is_control()).take(120).collect()
}

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
        "webp" => "image/webp",
        "gif" => "image/gif",
        "mp4" => "video/mp4",
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        "3gp" => "video/3gpp",
        "ogg" => "audio/ogg",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "m4a" => "audio/mp4",
        "doc" => "application/msword",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls" => "application/vnd.ms-excel",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt" => "application/vnd.ms-powerpoint",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        _ => "application/octet-stream",
    }
}

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

fn sent_id(r: &whatsapp_rust::SendResult) -> String {
    r.message_id.clone()
}

async fn send_file(
    client: &Arc<whatsapp_rust::client::Client>,
    chat: &str,
    path: &Path,
) -> Result<(String, String), String> {
    use tokio::io::AsyncReadExt;
    let to = chat.parse::<whatsapp_rust::Jid>().map_err(|_| "bad chat id".to_string())?;
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
    file.take(MAX_DOC_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "cannot read file".to_string())?;
    if bytes.len() as u64 > MAX_DOC_BYTES {
        return Err("file too large".into());
    }
    let up = client
        .upload(bytes, MediaType::Document, whatsapp_rust::UploadOptions::default())
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "upload failed");
            "upload failed".to_string()
        })?;
    let doc = waproto::whatsapp::message::DocumentMessage {
        url: Some(up.url),
        direct_path: Some(up.direct_path),
        media_key: Some(up.media_key.to_vec()),
        file_enc_sha256: Some(up.file_enc_sha256.to_vec()),
        file_sha256: Some(up.file_sha256.to_vec()),
        file_length: Some(up.file_length),
        mimetype: Some(mime_for(&safe_ext(&name)).to_string()),
        file_name: Some(name.clone()),
        ..Default::default()
    };
    let msg = waproto::whatsapp::Message {
        document_message: buffa::MessageField::some(doc),
        ..Default::default()
    };
    let id = client.send_message(to, msg).await.map(|r| sent_id(&r)).map_err(|e| {
        tracing::warn!(error = %e, "send failed");
        "send failed".to_string()
    })?;
    Ok((id, name))
}

fn unwrap_message(m: &waproto::whatsapp::Message) -> &waproto::whatsapp::Message {
    let mut cur = m;
    for _ in 0..4 {
        let next = cur
            .document_with_caption_message
            .as_option()
            .and_then(|f| f.message.as_option())
            .or_else(|| cur.ephemeral_message.as_option().and_then(|f| f.message.as_option()))
            .or_else(|| cur.view_once_message.as_option().and_then(|f| f.message.as_option()))
            .or_else(|| cur.device_sent_message.as_option().and_then(|d| d.message.as_option()));
        match next {
            Some(n) => cur = n,
            None => break,
        }
    }
    cur
}

fn media_kind(m: &waproto::whatsapp::Message) -> Option<&'static str> {
    if m.image_message.as_option().is_some() {
        Some("image")
    } else if m.video_message.as_option().is_some() {
        Some("video")
    } else if m.audio_message.as_option().is_some() {
        Some("audio")
    } else if m.document_message.as_option().is_some() {
        Some("document")
    } else if m.sticker_message.as_option().is_some() {
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

async fn download_video(
    client: &Arc<whatsapp_rust::client::Client>,
    downloads: &Arc<Semaphore>,
    dir: &Path,
    vid: &waproto::whatsapp::message::VideoMessage,
) -> Option<String> {
    const MAX_VIDEO_BYTES: u64 = 100 * 1024 * 1024;
    let sha = vid.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    if vid.file_length.unwrap_or(u64::MAX) > MAX_VIDEO_BYTES {
        tracing::warn!("video exceeds 100MB limit; skipped");
        return None;
    }
    let _slot = downloads.acquire().await.ok()?;
    let bytes = client.download(vid).await.ok()?;
    if bytes.len() as u64 > MAX_VIDEO_BYTES {
        return None;
    }
    store_media(dir, sha, vid.mimetype.as_deref().or(Some("video/mp4")), &bytes).ok()
}

async fn download_audio(
    client: &Arc<whatsapp_rust::client::Client>,
    downloads: &Arc<Semaphore>,
    dir: &Path,
    aud: &waproto::whatsapp::message::AudioMessage,
) -> Option<String> {
    const MAX_AUDIO_BYTES: u64 = 50 * 1024 * 1024;
    let sha = aud.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    if aud.file_length.unwrap_or(u64::MAX) > MAX_AUDIO_BYTES {
        tracing::warn!("audio exceeds 50MB limit; skipped");
        return None;
    }
    let _slot = downloads.acquire().await.ok()?;
    let bytes = client.download(aud).await.ok()?;
    if bytes.len() as u64 > MAX_AUDIO_BYTES {
        return None;
    }
    store_media(dir, sha, aud.mimetype.as_deref().or(Some("audio/ogg")), &bytes).ok()
}

pub fn auto_download_enabled() -> bool {
    std::env::var("WA_AUTO_DOWNLOAD").map_or(false, |v| v == "1" || v.eq_ignore_ascii_case("true"))
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StoredMediaParams {
    pub direct_path: String,
    pub media_key: Option<String>,
    pub file_sha256: String,
    pub file_enc_sha256: Option<String>,
    pub file_length: u64,
    pub media_type: String, // "image", "video", "audio", "document"
    pub mimetype: Option<String>,
    pub file_name: Option<String>,
}

pub fn extract_image_params(img: &waproto::whatsapp::message::ImageMessage) -> Option<String> {
    let direct_path = img.direct_path.clone()?;
    let file_sha256 = img.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    let file_length = img.file_length.unwrap_or(0);
    let params = StoredMediaParams {
        direct_path,
        media_key: img.media_key.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_sha256: BASE64_STANDARD.encode(file_sha256),
        file_enc_sha256: img.file_enc_sha256.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_length,
        media_type: "image".to_string(),
        mimetype: img.mimetype.clone(),
        file_name: None,
    };
    serde_json::to_string(&params).ok()
}

pub fn extract_video_params(vid: &waproto::whatsapp::message::VideoMessage) -> Option<String> {
    let direct_path = vid.direct_path.clone()?;
    let file_sha256 = vid.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    let file_length = vid.file_length.unwrap_or(0);
    let params = StoredMediaParams {
        direct_path,
        media_key: vid.media_key.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_sha256: BASE64_STANDARD.encode(file_sha256),
        file_enc_sha256: vid.file_enc_sha256.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_length,
        media_type: "video".to_string(),
        mimetype: vid.mimetype.clone().or_else(|| Some("video/mp4".to_string())),
        file_name: None,
    };
    serde_json::to_string(&params).ok()
}

pub fn extract_audio_params(aud: &waproto::whatsapp::message::AudioMessage) -> Option<String> {
    let direct_path = aud.direct_path.clone()?;
    let file_sha256 = aud.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    let file_length = aud.file_length.unwrap_or(0);
    let params = StoredMediaParams {
        direct_path,
        media_key: aud.media_key.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_sha256: BASE64_STANDARD.encode(file_sha256),
        file_enc_sha256: aud.file_enc_sha256.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_length,
        media_type: "audio".to_string(),
        mimetype: aud.mimetype.clone().or_else(|| Some("audio/ogg".to_string())),
        file_name: None,
    };
    serde_json::to_string(&params).ok()
}

pub fn extract_document_params(doc: &waproto::whatsapp::message::DocumentMessage) -> Option<String> {
    let direct_path = doc.direct_path.clone()?;
    let file_sha256 = doc.file_sha256.as_deref().filter(|s| s.len() == 32)?;
    let file_length = doc.file_length.unwrap_or(0);
    let params = StoredMediaParams {
        direct_path,
        media_key: doc.media_key.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_sha256: BASE64_STANDARD.encode(file_sha256),
        file_enc_sha256: doc.file_enc_sha256.as_deref().map(|k| BASE64_STANDARD.encode(k)),
        file_length,
        media_type: "document".to_string(),
        mimetype: doc.mimetype.clone(),
        file_name: doc.file_name.clone(),
    };
    serde_json::to_string(&params).ok()
}

async fn perform_download(
    client: &Arc<whatsapp_rust::client::Client>,
    downloads: &Arc<Semaphore>,
    dir: &Path,
    msg_id: &str,
    params_json: &str,
) -> Result<String, String> {
    let p: StoredMediaParams = serde_json::from_str(params_json)
        .map_err(|e| format!("invalid media params: {e}"))?;
    let sha = BASE64_STANDARD.decode(&p.file_sha256)
        .map_err(|e| format!("bad sha256: {e}"))?;
    let key = p.media_key.as_ref()
        .and_then(|k| BASE64_STANDARD.decode(k).ok());
    let enc_sha = p.file_enc_sha256.as_ref()
        .and_then(|k| BASE64_STANDARD.decode(k).ok());
    let m_type = match p.media_type.as_str() {
        "image" => wacore::download::MediaType::Image,
        "video" => wacore::download::MediaType::Video,
        "audio" => wacore::download::MediaType::Audio,
        "document" => wacore::download::MediaType::Document,
        _ => wacore::download::MediaType::Image,
    };
    let dl_params = whatsapp_rust::download::DownloadParams {
        direct_path: p.direct_path,
        media_key: key,
        file_sha256: sha.clone(),
        file_enc_sha256: enc_sha,
        file_length: p.file_length,
        media_type: m_type,
    };
    let _slot = downloads.acquire().await.map_err(|_| "download limit reached".to_string())?;
    let bytes = client.download(&dl_params).await.map_err(|e| format!("download failed: {e}"))?;
    match p.media_type.as_str() {
        "document" => {
            let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
            let tag: String = msg_id.chars().filter(char::is_ascii_alphanumeric).take(40).collect();
            let ext = ext_for_doc(p.file_name.as_deref().unwrap_or(""), p.mimetype.as_deref());
            store_file(dir, &format!("{hex}-{tag}"), &ext, &bytes)
                .map_err(|e| format!("cannot save document: {e}"))
        }
        "video" => {
            store_media(dir, &sha, p.mimetype.as_deref().or(Some("video/mp4")), &bytes)
                .map_err(|e| format!("cannot save video: {e}"))
        }
        "audio" => {
            store_media(dir, &sha, p.mimetype.as_deref().or(Some("audio/ogg")), &bytes)
                .map_err(|e| format!("cannot save audio: {e}"))
        }
        _ => {
            store_media(dir, &sha, p.mimetype.as_deref(), &bytes)
                .map_err(|e| format!("cannot save image: {e}"))
        }
    }
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

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Incoming {
    pub id: String,
    pub chat: String,
    pub sender: String,
    pub from_me: bool,
    pub ts: i64,
    pub text: Option<String>,
    pub media_kind: Option<String>,
    pub media_path: Option<String>,
    #[serde(skip)]
    pub historical: bool,
    #[serde(skip)]
    pub media_params: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize)]
pub struct ReplyContext {
    pub id: String,
    pub sender: String,
    pub from_me: bool,
    pub text: Option<String>,
    pub media_kind: Option<String>,
}

#[derive(Clone)]
pub struct Contact {
    pub jid: String,
    pub name: String,
    pub pn: Option<String>,
    pub avatar_path: Option<String>,
    pub rank: i64,
}

pub enum Outgoing {
    Text {
        chat: String,
        text: String,
        reply_to: Option<ReplyContext>,
        reply: oneshot::Sender<Result<String, String>>,
    },
    File {
        chat: String,
        path: PathBuf,
        reply: oneshot::Sender<Result<String, String>>,
    },
    Older {
        chat: String,
        oldest_id: String,
        oldest_from_me: bool,
        oldest_ts_ms: i64,
        count: i32,
        reply: oneshot::Sender<Result<String, String>>,
    },
    DownloadMedia {
        #[allow(dead_code)]
        chat: String,
        id: String,
        params: String,
        reply: oneshot::Sender<Result<String, String>>,
    },
}

pub type Res = Result<(), Box<dyn std::error::Error>>;

pub trait Protocol {
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
    pub calls: Arc<crate::calls::CallHub>,
}

static HIST_CONVS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static HIST_MSGS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

async fn import_history(
    sync: waproto::whatsapp::HistorySync,
    out: &mpsc::Sender<Incoming>,
    contacts: &mpsc::Sender<Contact>,
    client: &Arc<whatsapp_rust::client::Client>,
    avatar_downloads: &Arc<Semaphore>,
    dir: &PathBuf,
) {
    let mut imported = 0usize;
    let sync_type = format!("{:?}", sync.sync_type);
    let conversations = sync.conversations.len();
    for info in sync.status_v3_messages {
        let key = info.key.as_option().cloned().unwrap_or_default();
        let (Some(id), Some(chat), Some(msg)) = (key.id.clone(), key.remote_jid.clone(), info.message.as_option().cloned()) else { continue };
        let msg = unwrap_message(&msg);
        let kind = media_kind(msg);
        let mut body = text_of(msg);
        if body.is_none() && kind.is_none() { continue; }
        let mut params = None;
        if let Some(img) = msg.image_message.as_option() {
            body = body.or_else(|| img.caption.clone());
            params = extract_image_params(img);
        }
        if let Some(v) = msg.video_message.as_option() {
            body = body.or_else(|| v.caption.clone());
            params = extract_video_params(v);
        }
        if let Some(d) = msg.document_message.as_option() {
            body = body.or_else(|| doc_label(d));
            params = extract_document_params(d);
        }
        if let Some(a) = msg.audio_message.as_option() {
            params = extract_audio_params(a);
        }
        let m = Incoming {
            id, chat: "status@broadcast".to_string(),
            sender: key.participant.or(info.participant).unwrap_or(chat),
            from_me: key.from_me.unwrap_or(false),
            ts: info.message_timestamp.unwrap_or(0) as i64,
            text: body, media_kind: kind.map(str::to_string), media_path: None, historical: true,
            media_params: params,
        };
        if out.send(m).await.is_err() { tracing::error!("history writer is gone"); return; }
        imported += 1;
    }
    for conversation in sync.conversations {
        let conversation_id = conversation.id.clone();
        let conversation_name = conversation
            .name
            .clone()
            .filter(|n| !n.trim().is_empty())
            .or_else(|| conversation.display_name.clone().filter(|n| !n.trim().is_empty()))
            .unwrap_or_else(|| conversation_id.clone());
        let conversation_pn = conversation_id.ends_with("@s.whatsapp.net").then(|| conversation_id.clone());
        let _ = contacts.send(Contact {
            jid: conversation_id.clone(),
            name: conversation_name.clone(),
            pn: conversation_pn.clone(),
            avatar_path: None,
            rank: if conversation_name != conversation_id { 2 } else { 0 },
        }).await;
        if conversation_id.ends_with("@g.us") && conversation_name == conversation_id {
            fetch_group_name(client.clone(), contacts.clone(), avatar_downloads.clone(), dir.clone(), conversation_id.clone());
        } else {
            fetch_avatar(client.clone(), contacts.clone(), avatar_downloads.clone(), dir.clone(), conversation_id.clone(), conversation_name, conversation_pn);
        }
        for entry in conversation.messages {
            let Some(info) = entry.message.as_option().cloned() else { continue };
            let key = info.key.as_option().cloned().unwrap_or_default();
            let Some(id) = key.id.clone() else { continue };
            let chat = key.remote_jid.clone().unwrap_or_else(|| conversation_id.clone());
            let from_me = key.from_me.unwrap_or(false);
            let sender = if from_me { "me".to_string() } else {
                key.participant.clone().or_else(|| info.participant.clone()).unwrap_or_else(|| chat.clone())
            };
            if !from_me && let Some(name) = info.push_name.as_deref().filter(|n| !n.trim().is_empty()) {
                let pn = if sender.ends_with("@s.whatsapp.net") { Some(sender.clone()) }
                    else if chat.ends_with("@s.whatsapp.net") { Some(chat.clone()) } else { None };
                let _ = contacts.send(Contact { jid: sender.clone(), name: name.to_string(), pn: pn.clone(), avatar_path: None, rank: 1 }).await;
                if let Some(pn) = pn.filter(|p| p != &sender) {
                    let _ = contacts.send(Contact { jid: pn, name: name.to_string(), pn: None, avatar_path: None, rank: 1 }).await;
                }
            }
            let Some(msg) = info.message.as_option().cloned() else { continue };
            let msg = unwrap_message(&msg);
            let kind = media_kind(msg);
            let mut body = text_of(msg);
            if body.is_none() && kind.is_none() { continue; }
            let mut params = None;
            if let Some(img) = msg.image_message.as_option() {
                body = body.or_else(|| img.caption.clone());
                params = extract_image_params(img);
            }
            if let Some(v) = msg.video_message.as_option() {
                body = body.or_else(|| v.caption.clone());
                params = extract_video_params(v);
            }
            if let Some(d) = msg.document_message.as_option() {
                body = body.or_else(|| doc_label(d));
                params = extract_document_params(d);
            }
            if let Some(a) = msg.audio_message.as_option() {
                params = extract_audio_params(a);
            }
            let m = Incoming {
                id, chat, sender, from_me,
                ts: info.message_timestamp.unwrap_or(0) as i64,
                text: body, media_kind: kind.map(str::to_string), media_path: None, historical: true,
                media_params: params,
            };
            if out.send(m).await.is_err() { tracing::error!("history writer is gone"); return; }
            imported += 1;
        }
    }
    use std::sync::atomic::Ordering::Relaxed;
    let total_conversations = HIST_CONVS.fetch_add(conversations as u32, Relaxed) + conversations as u32;
    let total_messages = HIST_MSGS.fetch_add(imported as u32, Relaxed) + imported as u32;
    tracing::info!(sync_type, conversations, messages = imported, total_conversations, total_messages, "history sync chunk received");
}

static AV_OK: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static AV_NONE: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static AV_LOOKUP_ERR: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
static AV_DOWNLOAD_ERR: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn avatar_done(counter: &std::sync::atomic::AtomicU32, problem: Option<&str>) {
    use std::sync::atomic::Ordering::Relaxed;
    let n = counter.fetch_add(1, Relaxed) + 1;
    let (ok, none, lookup, download) = (
        AV_OK.load(Relaxed),
        AV_NONE.load(Relaxed),
        AV_LOOKUP_ERR.load(Relaxed),
        AV_DOWNLOAD_ERR.load(Relaxed),
    );
    if let Some(p) = problem {
        if n <= 5 {
            tracing::info!(error = p, "profile picture problem (first few shown)");
        }
    }
    if (ok + none + lookup + download) % 25 == 0 {
        tracing::info!(ok, no_picture = none, lookup_error = lookup, download_error = download, "profile pictures so far");
    }
}

fn event_name(e: &impl std::fmt::Debug) -> String {
    format!("{e:?}")
        .split(|ch: char| ch == '(' || ch == ' ' || ch == '{')
        .next()
        .unwrap_or("?")
        .to_string()
}

fn fetch_group_name(
    client: Arc<whatsapp_rust::client::Client>,
    contacts: mpsc::Sender<Contact>,
    slots: Arc<Semaphore>,
    dir: PathBuf,
    jid: String,
) {
    tokio::spawn(async move {
        let Ok(parsed) = jid.parse::<whatsapp_rust::Jid>() else { return };
        match client.groups().get_metadata(&parsed).await {
            Ok(meta) => {
                let subject = meta.subject.trim().to_string();
                tracing::info!(jid = %jid, subject = %subject, "fetched group metadata");
                if !subject.is_empty() {
                    let _ = contacts
                        .send(Contact {
                            jid: jid.clone(),
                            name: subject.clone(),
                            pn: None,
                            avatar_path: None,
                            rank: 2,
                        })
                        .await;
                    fetch_avatar(client, contacts, slots, dir, jid, subject, None);
                }
            }
            Err(e) => {
                tracing::warn!(jid = %jid, error = %e, "group metadata lookup failed");
            }
        }
    });
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
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let mut candidates = vec![jid.clone()];
        if let Some(p) = pn.clone().filter(|p| p != &jid) {
            candidates.push(p);
        }
        let mut picture = None;
        let mut last_error: Option<String> = None;
        for id in &candidates {
            let Ok(parsed) = id.parse() else { continue };
            for attempt in 0..2 {
                let mut result = client.contacts().get_profile_picture(&parsed, true).await;
                if matches!(result, Ok(None)) {
                    result = client.contacts().get_profile_picture(&parsed, false).await;
                }
                match result {
                    Ok(Some(p)) => {
                        picture = Some(p);
                        break;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        last_error = Some(e.to_string());
                        if attempt == 0 {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                    }
                }
            }
            if picture.is_some() {
                break;
            }
        }
        let Some(picture) = picture else {
            match last_error {
                Some(e) => avatar_done(&AV_LOOKUP_ERR, Some(e.as_str())),
                None => {
                    let note = format!("no picture returned for a @{} id", jid.rsplit('@').next().unwrap_or("?"));
                    avatar_done(&AV_NONE, Some(note.as_str()))
                }
            }
            return;
        };
        let url = picture.url;
        let save_dir = dir.clone();
        let key = jid.clone();
        let path = tokio::task::spawn_blocking(move || download_avatar(&url, &save_dir, &key))
            .await
            .ok()
            .flatten();
        if let Some(avatar_path) = path {
            avatar_done(&AV_OK, None);
            let _ = contacts
                .send(Contact {
                    jid,
                    name,
                    pn,
                    avatar_path: Some(avatar_path),
                    rank: 0,
                })
                .await;
        } else {
            avatar_done(&AV_DOWNLOAD_ERR, Some("download failed"));
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

async fn fetch_newsletters(
    client: Arc<whatsapp_rust::client::Client>,
    contacts: mpsc::Sender<Contact>,
    out: mpsc::Sender<Incoming>,
    dir: PathBuf,
) {
    let newsletters = match client.newsletter().list_subscribed().await {
        Ok(nl) => nl,
        Err(e) => {
            tracing::info!(error = ?e, "could not list subscribed newsletters");
            return;
        }
    };
    tracing::info!(count = newsletters.len(), "subscribed newsletters fetched");
    for nl in newsletters {
        let jid = nl.jid.to_string();
        let name = if nl.name.trim().is_empty() { jid.clone() } else { nl.name.clone() };
        let mut avatar_path = None;
        if let Some(pic_url) = nl.picture_url.or(nl.preview_url) {
            let jid_clone = jid.clone();
            let dir_clone = dir.clone();
            avatar_path = tokio::task::spawn_blocking(move || download_avatar(&pic_url, &dir_clone, &jid_clone))
                .await.ok().flatten();
        }
        let _ = contacts.send(Contact {
            jid: jid.clone(),
            name: name.clone(),
            pn: None,
            avatar_path: avatar_path.clone(),
            rank: 3,
        }).await;

        if let Ok(msgs) = client.newsletter().get_messages(nl.jid.clone(), 20, None).await {
            for entry in msgs {
                let id = if entry.message_id.is_empty() {
                    format!("nl_{}_{}", jid, entry.server_id)
                } else {
                    entry.message_id
                };
                let raw_msg = entry.message.as_ref();
                let msg = raw_msg.map(unwrap_message);
                let kind = msg.and_then(media_kind);
                let mut body = msg.and_then(text_of);
                let mut params = None;
                if let Some(m) = msg {
                    if let Some(img) = m.image_message.as_option() {
                        body = body.or_else(|| img.caption.clone());
                        params = extract_image_params(img);
                    }
                    if let Some(v) = m.video_message.as_option() {
                        body = body.or_else(|| v.caption.clone());
                        params = extract_video_params(v);
                    }
                    if let Some(d) = m.document_message.as_option() {
                        body = body.or_else(|| doc_label(d));
                        params = extract_document_params(d);
                    }
                    if let Some(a) = m.audio_message.as_option() {
                        params = extract_audio_params(a);
                    }
                }
                let m = Incoming {
                    id,
                    chat: jid.clone(),
                    sender: jid.clone(),
                    from_me: entry.is_sender,
                    ts: entry.timestamp as i64,
                    text: body,
                    media_kind: kind.map(str::to_string),
                    media_path: None,
                    historical: true,
                    media_params: params,
                };
                let _ = out.send(m).await;
            }
        }
    }
}

impl Protocol for WhatsApp {
    async fn run(
        self,
        state: watch::Sender<State>,
        out: mpsc::Sender<Incoming>,
        cmds: mpsc::Receiver<Outgoing>,
        contacts: mpsc::Sender<Contact>,
    ) -> Res {
        let cmds = Arc::new(Mutex::new(Some(cmds)));
        let backend = SqliteStore::new(self.db.to_str().ok_or("non-UTF-8 data path")?).await?;
        let calls = self.calls.clone();
        let downloads = Arc::new(Semaphore::new(MAX_DOWNLOADS));
        let media_dir = self.media_dir;
        let cached_contacts = self.cached_contacts;
        let avatar_downloads = Arc::new(Semaphore::new(3));
        let avatar_bootstrap_done = Arc::new(AtomicBool::new(false));
        let logged_out = Arc::new(AtomicBool::new(false));
        let logged_out_handler = logged_out.clone();

        let bot = Bot::builder()
            .with_backend(backend)
            .on_event(move |event, client| {
                let (state, out) = (state.clone(), out.clone());
                let (downloads, dir) = (downloads.clone(), media_dir.clone());
                let avatar_downloads = avatar_downloads.clone();
                let cached_contacts = cached_contacts.clone();
                let avatar_bootstrap_done = avatar_bootstrap_done.clone();
                let cmds = cmds.clone();
                let logged_out = logged_out_handler.clone();
                let contacts = contacts.clone();
                let calls = calls.clone();
                async move {
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        let dbg = format!("{event:?}");
                        let kind = dbg.split(|c: char| c == '(' || c == ' ' || c == '{').next().unwrap_or("?");
                        tracing::debug!(kind, "event");
                    }
                    let event = Arc::unwrap_or_clone(event);
                    let inbound_msgs: Vec<_> = event.messages().map(|m| (m.message.clone(), m.info.clone())).collect();
                    match event {
                        Event::IncomingCall(call) => {
                            calls.on_incoming(client.clone(), whatsapp_rust::wacore::types::call::IncomingCall::clone(&call)).await;
                        }
                        Event::PairingQrCode(qr) => {
                            let code = qr.code.clone();
                            if std::io::IsTerminal::is_terminal(&std::io::stdout()) {
                                println!("QR:\n{code}");
                            }
                            state.send_replace(State::Pairing { qr: code.to_string() });
                        }
                        Event::Connected(_) => {
                            calls.set_client(client.clone());
                            let device = client.persistence_manager().get_device_snapshot();
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
                            tracing::info!(has_pn = account_pn.is_some(), has_lid = account_lid.is_some(), "own account ids");
                            if let (Some(pn), Some(lid)) = (&account_pn, &account_lid) {
                                let (pn, lid) = (bare_jid(pn), bare_jid(lid));
                                for (jid, own_pn) in [(lid, pn.clone()), (pn.clone(), pn)] {
                                    let _ = contacts.send(Contact { name: jid.clone(), jid, pn: Some(own_pn), avatar_path: None, rank: 0 }).await;
                                }
                            }
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
                            let nl_client = client.clone();
                            let nl_contacts = contacts.clone();
                            let nl_out = out.clone();
                            let nl_dir = dir.clone();
                            tokio::spawn(async move {
                                fetch_newsletters(nl_client, nl_contacts, nl_out, nl_dir).await;
                            });
                            if !avatar_bootstrap_done.swap(true, Ordering::SeqCst) {
                                for contact in cached_contacts {
                                    if contact.jid.ends_with("@g.us")
                                        && (contact.name == contact.jid
                                            || contact.name == "Group"
                                            || contact.name.trim().is_empty())
                                    {
                                        fetch_group_name(
                                            client.clone(),
                                            contacts.clone(),
                                            avatar_downloads.clone(),
                                            dir.clone(),
                                            contact.jid.clone(),
                                        );
                                    }
                                    let fresh = contact.avatar_path.as_deref().is_some_and(|p| {
                                        std::fs::metadata(p).and_then(|m| m.modified()).ok()
                                            .and_then(|t| t.elapsed().ok())
                                            .is_some_and(|age| age < std::time::Duration::from_secs(7 * 24 * 3600))
                                    });
                                    if fresh { continue; }
                                    fetch_avatar(client.clone(), contacts.clone(), avatar_downloads.clone(), dir.clone(),
                                        contact.jid, contact.name, contact.pn);
                                }
                            }
                            let rx = cmds.lock().unwrap_or_else(|p| p.into_inner()).take();
                            if let Some(mut rx) = rx {
                                let (client, out, downloads, dir) =
                                    (client.clone(), out.clone(), downloads.clone(), dir.clone());
                                tokio::spawn(async move {
                                    while let Some(cmd) = rx.recv().await {
                                        let (chat, text, reply_to, reply) = match cmd {
                                            Outgoing::Text { chat, text, reply_to, reply } => (chat, text, reply_to, reply),
                                            Outgoing::DownloadMedia { chat: _, id, params, reply } => {
                                                let r = perform_download(&client, &downloads, &dir, &id, &params).await;
                                                let _ = reply.send(r);
                                                continue;
                                            }
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
                                                            media_params: None,
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
                                                    let jid = chat.parse::<whatsapp_rust::Jid>().map_err(|_| "bad chat id".to_string())?;
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
                                            let to = chat.parse::<whatsapp_rust::Jid>().map_err(|_| "bad chat id".to_string())?;
                                            let msg = if let Some(q) = reply_to {
                                                let quoted_text = q.text.clone().or_else(|| q.media_kind.clone().map(|k| format!("[{k}]"))).unwrap_or_default();
                                                let quoted = waproto::whatsapp::Message { conversation: Some(quoted_text), ..Default::default() };
                                                let context = waproto::whatsapp::ContextInfo {
                                                    stanza_id: Some(q.id), participant: (!q.from_me).then_some(q.sender),
                                                    quoted_message: buffa::MessageField::some(quoted), remote_jid: Some(chat.clone()),
                                                    ..Default::default()
                                                };
                                                waproto::whatsapp::Message {
                                                    extended_text_message: buffa::MessageField::some(waproto::whatsapp::message::ExtendedTextMessage {
                                                        text: Some(text.clone()), context_info: buffa::MessageField::some(context), ..Default::default()
                                                    }), ..Default::default()
                                                }
                                            } else { waproto::whatsapp::Message { conversation: Some(text.clone()), ..Default::default() } };
                                            client.send_message(to, msg).await.map(|r| sent_id(&r)).map_err(|e| {
                                                tracing::warn!(error = %e, "send failed");
                                                "send failed".to_string()
                                            })
                                        }
                                        .await;
                                        if let Ok(id) = &r {
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
                                                media_params: None,
                                            };
                                            let _ = out.send(m).await;
                                        }
                                        let _ = reply.send(r);
                                    }
                                });
                            }
                        }
                        Event::ContactUpdate(c) => {
                            let saved = c.action.full_name.clone()
                                .or_else(|| c.action.first_name.clone())
                                .filter(|n| !n.trim().is_empty());
                            let name = saved.clone()
                                .or_else(|| c.action.username.clone())
                                .filter(|n| !n.trim().is_empty());
                            let rank: i64 = if saved.is_some() { 2 } else if name.is_some() { 1 } else { 0 };
                            let id = c.jid.to_string();
                            let pn = c.action.pn_jid.clone().or_else(|| id.ends_with("@s.whatsapp.net").then(|| id.clone()));
                            let name = name.unwrap_or_else(|| id.clone());
                            let ids = [Some((id, pn.clone())), c.action.lid_jid.clone().map(|l| (l, pn.clone())), c.action.pn_jid.clone().map(|p| (p, None))];
                            for (jid, pn) in ids.into_iter().flatten() {
                                if contacts.send(Contact { jid: jid.clone(), name: name.clone(), pn: pn.clone(), avatar_path: None, rank }).await.is_err() {
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
                                let _ = contacts.send(Contact { jid: jid.clone(), name: update.new_push_name.clone(), pn: pn.clone(), avatar_path: None, rank: 1 }).await;
                                fetch_avatar(client.clone(), contacts.clone(), avatar_downloads, dir.clone(), jid, update.new_push_name, pn);
                            }
                        }
                        Event::PictureUpdate(update) if !update.removed => {
                            let jid = update.jid.to_string();
                            let pn = jid.ends_with("@s.whatsapp.net").then(|| jid.clone());
                            fetch_avatar(client.clone(), contacts.clone(), avatar_downloads, dir.clone(), jid.clone(), jid, pn);
                        }
                        Event::HistorySync(lazy) => {
                            let Some(sync) = lazy.get() else {
                                tracing::warn!("history sync chunk could not be decoded");
                                return;
                            };
                            import_history(sync.clone(), &out, &contacts, &client, &avatar_downloads, &dir).await;
                        }
                        Event::LoggedOut(_) => {
                            tracing::error!("WhatsApp revoked this session (logged out)");
                            logged_out.store(true, Ordering::SeqCst);
                            state.send_replace(State::Starting);
                        }
                        Event::Disconnected(_) => {
                            state.send_replace(State::Starting);
                        }
                        Event::Messages(_) => {
                            for (msg, info) in inbound_msgs {
                                let (client, out, downloads, dir, contacts, avatar_downloads) =
                                    (client.clone(), out.clone(), downloads.clone(), dir.clone(), contacts.clone(), avatar_downloads.clone());
                                async move {
                                    if !info.source.is_from_me && !info.push_name.trim().is_empty() {
                                        let sender = info.source.sender.to_string();
                                        let pn = if sender.ends_with("@s.whatsapp.net") { Some(sender.clone()) }
                                            else { info.source.sender_alt.as_ref().map(ToString::to_string).filter(|p| p.ends_with("@s.whatsapp.net")) };
                                        let _ = contacts.send(Contact { jid: sender.clone(), name: info.push_name.clone(), pn: pn.clone(), avatar_path: None, rank: 1 }).await;
                                        if let Some(pn) = pn.filter(|p| p != &sender) {
                                            let _ = contacts.send(Contact { jid: pn, name: info.push_name.clone(), pn: None, avatar_path: None, rank: 1 }).await;
                                        }
                                    }
                                    let msg = unwrap_message(&msg);
                                    let img = msg.image_message.as_option().cloned();
                                    let doc = msg.document_message.as_option().cloned();
                                    let vid = msg.video_message.as_option().cloned();
                                    let aud = msg.audio_message.as_option().cloned();
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
                                        media_params: None,
                                    };
                                    if m.chat.ends_with("@g.us") {
                                        fetch_group_name(
                                            client.clone(),
                                            contacts.clone(),
                                            avatar_downloads.clone(),
                                            dir.clone(),
                                            m.chat.clone(),
                                        );
                                    }
                                    tracing::info!(
                                        from_me = m.from_me,
                                        has_text = m.text.is_some(),
                                        media = ?m.media_kind,
                                        domain = m.chat.rsplit('@').next().unwrap_or("?"),
                                        "message event"
                                    );
                                    if let Some(note) = msg.protocol_message.as_option().and_then(|p| p.history_sync_notification.as_option()) {
                                        tracing::info!(sync_type = ?note.sync_type, "history sync notification received");
                                    }
                                    if m.text.is_none() && m.media_kind.is_none() {
                                        tracing::debug!("skipping message with no user-visible content");
                                        return;
                                    }
                                    if let Some(doc) = doc {
                                        m.text = m.text.or_else(|| doc_label(&doc));
                                        if auto_download_enabled() {
                                            tokio::spawn(async move {
                                                let path = download_document(&client, &downloads, &dir, &m.id, &doc).await;
                                                m.media_path = path;
                                                if out.send(m).await.is_err() {
                                                    tracing::error!("message writer is gone");
                                                }
                                            });
                                        } else {
                                            m.media_params = extract_document_params(&doc);
                                            if out.send(m).await.is_err() {
                                                tracing::error!("message writer is gone");
                                            }
                                        }
                                        return;
                                    }
                                    if let Some(vid) = vid {
                                        m.text = m.text.or_else(|| vid.caption.clone());
                                        if auto_download_enabled() {
                                            tokio::spawn(async move {
                                                let path = download_video(&client, &downloads, &dir, &vid).await;
                                                m.media_path = path;
                                                if out.send(m).await.is_err() {
                                                    tracing::error!("message writer is gone");
                                                }
                                            });
                                        } else {
                                            m.media_params = extract_video_params(&vid);
                                            if out.send(m).await.is_err() {
                                                tracing::error!("message writer is gone");
                                            }
                                        }
                                        return;
                                    }
                                    if let Some(aud) = aud {
                                        if auto_download_enabled() {
                                            tokio::spawn(async move {
                                                let path = download_audio(&client, &downloads, &dir, &aud).await;
                                                m.media_path = path;
                                                if out.send(m).await.is_err() {
                                                    tracing::error!("message writer is gone");
                                                }
                                            });
                                        } else {
                                            m.media_params = extract_audio_params(&aud);
                                            if out.send(m).await.is_err() {
                                                tracing::error!("message writer is gone");
                                            }
                                        }
                                        return;
                                    }
                                    if let Some(img) = img {
                                        m.text = m.text.or_else(|| img.caption.clone());
                                        if auto_download_enabled() {
                                            tokio::spawn(async move {
                                                m.media_path = download_image(&client, &downloads, &dir, &img).await;
                                                if out.send(m).await.is_err() {
                                                    tracing::error!("message writer is gone");
                                                }
                                            });
                                        } else {
                                            m.media_params = extract_image_params(&img);
                                            if out.send(m).await.is_err() {
                                                tracing::error!("message writer is gone");
                                            }
                                        }
                                        return;
                                    }
                                    if out.send(m).await.is_err() {
                                        tracing::error!("message writer is gone");
                                    }
                                }.await;
                            }
                        }
                        other => {
                            tracing::debug!(event = %event_name(&other), "unhandled event");
                        }
                    }
                }
            })
            .build()
            .await?;

        bot.run().await;
        let run_error: Option<String> = None;
        if logged_out.load(Ordering::SeqCst) {
            for ext in ["", "-wal", "-shm"] {
                let mut p = self.db.clone().into_os_string();
                p.push(ext);
                let _ = std::fs::remove_file(p);
            }
            return Err(REVOKED.into());
        }
        if let Some(error) = run_error {
            return Err(error.into());
        }
        Err("WhatsApp connection loop ended unexpectedly".into())
    }
}

fn store_media(
    dir: &Path,
    sha: &[u8],
    mime: Option<&str>,
    bytes: &[u8],
) -> std::io::Result<String> {
    let m = mime.unwrap_or("");
    let ext = if m.contains("jpeg") {
        "jpg"
    } else if m.contains("png") {
        "png"
    } else if m.contains("webp") {
        "webp"
    } else if m.contains("gif") {
        "gif"
    } else if m.contains("mp4") {
        "mp4"
    } else if m.contains("webm") {
        "webm"
    } else if m.contains("3gpp") {
        "3gp"
    } else if m.contains("quicktime") {
        "mov"
    } else if m.contains("ogg") || m.contains("opus") {
        "ogg"
    } else if m.contains("mpeg") || m.contains("mp3") {
        "mp3"
    } else if m.contains("wav") {
        "wav"
    } else if m.contains("m4a") {
        "m4a"
    } else {
        "bin"
    };
    let name: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    store_file(dir, &name, ext, bytes)
}

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

fn text_of(m: &waproto::whatsapp::Message) -> Option<String> {
    m.conversation
        .clone()
        .or_else(|| {
            m.extended_text_message
                .as_option()
                .and_then(|e| e.text.clone())
        })
        .or_else(|| {
            m.device_sent_message
                .as_option()
                .and_then(|d| d.message.as_option())
                .and_then(text_of)
        })
}