//! Thin client: talks JSON lines to the daemon socket. It never touches the protocol or the DB.
//! One long-lived connection for live events; short one-shot connections for requests.
use iced::futures::{SinkExt, Stream};
use iced::widget::{
    button, column, container, image, mouse_area, qr_code, row, scrollable, text, text_input,
};
use iced::{Alignment, Color, Element, Length, Subscription, Task, stream};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const PAGE: i64 = 50;

fn subtle_button_style(
    _: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    let color = if matches!(
        status,
        iced::widget::button::Status::Hovered | iced::widget::button::Status::Pressed
    ) {
        Color::from_rgb(0.19, 0.19, 0.19)
    } else {
        Color::from_rgb(0.12, 0.12, 0.12)
    };
    iced::widget::button::Style {
        background: Some(color.into()),
        text_color: Color::from_rgb(0.88, 0.92, 0.92),
        border: iced::Border {
            radius: 16.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn accent_button_style(
    _: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    let color = if matches!(
        status,
        iced::widget::button::Status::Hovered | iced::widget::button::Status::Pressed
    ) {
        Color::from_rgb(0.12, 0.78, 0.42)
    } else {
        Color::from_rgb(0.05, 0.72, 0.36)
    };
    iced::widget::button::Style {
        background: Some(color.into()),
        text_color: Color::WHITE,
        border: iced::Border {
            radius: 18.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn chat_row_style(
    _: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    iced::widget::button::Style {
        background: if matches!(
            status,
            iced::widget::button::Status::Hovered | iced::widget::button::Status::Pressed
        ) {
            Some(Color::from_rgb(0.13, 0.13, 0.13).into())
        } else {
            None
        },
        text_color: Color::from_rgb(0.92, 0.93, 0.93),
        border: iced::Border {
            radius: 8.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn rail_button_style(
    _: &iced::Theme,
    status: iced::widget::button::Status,
) -> iced::widget::button::Style {
    iced::widget::button::Style {
        background: Some(
            if matches!(
                status,
                iced::widget::button::Status::Hovered | iced::widget::button::Status::Pressed
            ) {
                Color::from_rgb(0.20, 0.20, 0.20).into()
            } else {
                Color::from_rgb(0.12, 0.12, 0.12).into()
            },
        ),
        text_color: Color::from_rgb(0.82, 0.84, 0.84),
        border: iced::Border {
            radius: 22.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn search_input_style(
    _: &iced::Theme,
    status: iced::widget::text_input::Status,
) -> iced::widget::text_input::Style {
    let border_color = if matches!(status, iced::widget::text_input::Status::Focused { .. }) {
        Color::from_rgb(0.05, 0.72, 0.36)
    } else {
        Color::from_rgb(0.20, 0.20, 0.20)
    };
    iced::widget::text_input::Style {
        background: Color::from_rgb(0.17, 0.17, 0.17).into(),
        border: iced::Border {
            color: border_color,
            width: 1.0,
            radius: 20.0.into(),
        },
        icon: Color::from_rgb(0.65, 0.67, 0.67),
        placeholder: Color::from_rgb(0.61, 0.62, 0.62),
        value: Color::from_rgb(0.94, 0.94, 0.94),
        selection: Color::from_rgb(0.00, 0.45, 0.33),
    }
}

fn circular_avatar(path: &str) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hash);
    let output = std::path::Path::new(path)
        .parent()?
        .join(format!("avatar-circle-{:016x}.png", hash.finish()));
    if output.is_file() {
        return Some(output.to_string_lossy().into_owned());
    }
    let source = ::image::load_from_memory(&bytes).ok()?.to_rgba8();
    let side = source.width().min(source.height());
    if side == 0 {
        return None;
    }
    let x = (source.width() - side) / 2;
    let y = (source.height() - side) / 2;
    let cropped = ::image::imageops::crop_imm(&source, x, y, side, side).to_image();
    let mut avatar =
        ::image::imageops::resize(&cropped, 128, 128, ::image::imageops::FilterType::Lanczos3);
    let center = 63.5_f32;
    let radius_squared = center * center;
    for (px, py, pixel) in avatar.enumerate_pixels_mut() {
        let dx = px as f32 - center;
        let dy = py as f32 - center;
        if dx * dx + dy * dy > radius_squared {
            pixel.0[3] = 0;
        }
    }
    avatar.save(&output).ok()?;
    Some(output.to_string_lossy().into_owned())
}

#[derive(Debug, Clone, Deserialize)]
struct Line {
    id: String,
    sender: String,
    #[serde(default)]
    sender_name: Option<String>,
    from_me: bool,
    ts: i64,
    seq: Option<i64>, // present on history rows only; it is the pagination cursor
    text: Option<String>,
    media_kind: Option<String>,
    media_path: Option<String>, // local file, set for downloaded images and documents
}

#[derive(Debug, Clone, Deserialize)]
struct Chat {
    chat: String,
    #[serde(default)]
    last_ts: i64,
    #[serde(default)]
    last_message: Option<String>,
    name: Option<String>, // address-book name when known
    avatar_path: Option<String>,
    #[serde(skip)]
    avatar_hash: Option<u64>,
    #[serde(skip)]
    avatar_display_path: Option<String>,
}

#[derive(Debug, Clone)]
enum Message {
    Link(bool),
    Event(Value),
    Health(Result<Value, String>),
    Chats(Result<Value, String>),
    History {
        chat: String,
        older: bool,
        r: Result<Value, String>,
    },
    Open(String),
    Older,
    Draft(String),
    Search(String),
    Attach,
    Picked(Result<Option<String>, String>),
    Dropped(PathBuf),
    ConfirmFile,
    CancelFile,
    FileSent(Result<Value, String>),
    OpenFile(String),
    FileOpened(Result<(), String>),
    SyncStart(Option<String>),
    SyncStarted(Result<Value, String>),
    SyncStatus(Result<Value, String>),
    NewChat,
    Send,
    Sent(Result<Value, String>),
    Delete,
    Deleted(Result<Value, String>),
    Category(Category),
    AskPhone,
    AskedPhone(Result<Value, String>),
    Reply(Line),
    MessageClick(Line),
    ToggleMessageMenu(String),
    ToggleChatMenu,
    ToggleSidebarMenu,
    ToggleAccountInfo,
    CancelReply,
    ToggleLog,
    ClearLog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Category {
    #[default]
    Chats,
    Status,
    Channels,
}

#[derive(Default)]
struct App {
    linked: bool,
    state: Value,
    qr: Option<qr_code::Data>,
    account_avatar_display_path: Option<String>,
    chats: Vec<Chat>,
    open: Option<String>,
    lines: Vec<Line>,           // oldest first
    cursor: Option<(i64, i64)>, // oldest loaded history row
    more: bool,
    draft: String,
    search: String,
    pending_file: Option<String>, // chosen or dropped, waiting for the user's "Send"
    sending: bool,
    replying_to: Option<Line>,
    last_message_click: Option<(String, Instant)>,
    open_message_menu: Option<String>,
    show_chat_menu: bool,
    show_sidebar_menu: bool,
    show_account_info: bool,
    error: Option<String>,
    notice: Option<String>,
    category: Category,
    show_log: bool,
    log: Vec<String>,
}

fn sock() -> PathBuf {
    PathBuf::from(std::env::var("XDG_RUNTIME_DIR").unwrap_or_default()).join("whatsapp/daemon.sock")
}

async fn request(req: Value) -> Result<Value, String> {
    tokio::time::timeout(Duration::from_secs(5), request_inner(req))
        .await
        .map_err(|_| "daemon did not answer in time".to_string())?
}

/// Uploading a file can take the daemon up to 120 s to answer.
async fn request_file(req: Value) -> Result<Value, String> {
    tokio::time::timeout(Duration::from_secs(130), request_inner(req))
        .await
        .unwrap_or_else(|_| Err("no answer; the file may not have been sent".to_string()))
}

/// Sending can take the daemon up to 15 s to answer; don't give up before it does.
async fn request_slow(req: Value) -> Result<Value, String> {
    tokio::time::timeout(Duration::from_secs(20), request_inner(req))
        .await
        .unwrap_or_else(|_| Err("no answer; the message may not have been sent".to_string()))
}

async fn request_inner(req: Value) -> Result<Value, String> {
    let (r, mut w) = UnixStream::connect(sock())
        .await
        .map_err(|e| e.to_string())?
        .into_split();
    w.write_all(format!("{req}\n").as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut line = String::new();
    BufReader::new(r)
        .read_line(&mut line)
        .await
        .map_err(|e| e.to_string())?;
    let v: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
    if v["type"] == "error" {
        return Err(v["message"].as_str().unwrap_or("daemon error").to_string());
    }
    Ok(v)
}

/// Live events. Reconnects forever, so a daemon restart needs no UI restart.
fn events() -> impl Stream<Item = Message> {
    stream::channel(64, async |mut out| {
        loop {
            if let Ok(s) = UnixStream::connect(sock()).await {
                let (r, mut w) = s.into_split();
                if w.write_all(b"{\"v\":1,\"id\":1,\"type\":\"subscribe\"}\n")
                    .await
                    .is_ok()
                {
                    let _ = out.send(Message::Link(true)).await;
                    let mut lines = BufReader::new(r).lines();
                    while let Ok(Some(l)) = lines.next_line().await {
                        if let Ok(v) = serde_json::from_str::<Value>(&l) {
                            let _ = out.send(Message::Event(v)).await;
                        }
                    }
                }
                let _ = out.send(Message::Link(false)).await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

/// Asks for the sync job's progress after a short pause; repeated while it runs.
fn poll_sync() -> Task<Message> {
    Task::perform(
        async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            request(json!({"v":1,"id":9,"type":"sync_status"})).await
        },
        Message::SyncStatus,
    )
}

/// A file dropped on the window. (Wayland compositors may not deliver drops; the + button always works.)
fn on_event(
    event: iced::Event,
    _status: iced::event::Status,
    _window: iced::window::Id,
) -> Option<Message> {
    match event {
        iced::Event::Window(iced::window::Event::FileDropped(path)) => Some(Message::Dropped(path)),
        _ => None,
    }
}

/// Native file dialog through whatever the desktop already has: no extra crate, no GUI toolkit.
/// Ok(None) = the user cancelled.
async fn pick_file() -> Result<Option<String>, String> {
    use tokio::process::Command;
    let dialogs: [(&str, &[&str]); 2] = [
        ("zenity", &["--file-selection", "--title=Send a file"]),
        ("kdialog", &["--getopenfilename", "."]),
    ];
    for (prog, args) in dialogs {
        match Command::new(prog).args(args).output().await {
            Ok(out) if out.status.success() => {
                let p = String::from_utf8_lossy(&out.stdout)
                    .trim_end_matches('\n')
                    .to_string();
                return Ok((!p.is_empty()).then_some(p));
            }
            Ok(_) => return Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err("cannot start the file dialog".into()),
        }
    }
    Err("No file dialog found. Install zenity (GNOME) or kdialog (KDE), or drag a file onto the window.".into())
}

/// Opens a downloaded file with the desktop's default app. Unlike a fire-and-forget spawn this waits
/// for the launcher's exit status, so "no PDF viewer", "file is gone" and similar show up as an error
/// instead of nothing happening.
async fn open_path(path: String) -> Result<(), String> {
    use std::process::Stdio;
    if !std::path::Path::new(&path).is_file() {
        return Err(format!("The file is no longer on disk: {path}"));
    }
    let mut last = String::new();
    let launchers: [(&str, &[&str]); 2] = [("xdg-open", &[]), ("gio", &["open"])];
    for (cmd, args) in launchers {
        let out = tokio::process::Command::new(cmd)
            .args(args)
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .await;
        match out {
            Ok(o) if o.status.success() => return Ok(()),
            Ok(o) => {
                let err = String::from_utf8_lossy(&o.stderr).trim().to_string();
                last = format!(
                    "{cmd} could not open the file ({}){}. Is a viewer for this file type installed?",
                    o.status,
                    if err.is_empty() { String::new() } else { format!(": {err}") }
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("cannot run {cmd}: {e}")),
        }
    }
    Err(if last.is_empty() {
        "Neither xdg-open nor gio is installed, so files cannot be opened.".into()
    } else {
        last
    })
}

fn load(chat: String, before: Option<(i64, i64)>) -> Task<Message> {
    let older = before.is_some();
    let req = json!({"v":1,"id":3,"type":"history","chat":chat,"limit":PAGE,"before":before});
    Task::perform(request(req), move |r| Message::History {
        chat: chat.clone(),
        older,
        r,
    })
}

impl App {
    fn log(&mut self, entry: impl Into<String>) {
        let entry = entry.into();
        self.log.push(format!(
            "{}  {entry}",
            chrono::Local::now().format("%H:%M:%S")
        ));
        if self.log.len() > 500 {
            self.log.drain(..self.log.len() - 500);
        }
    }

    fn refresh(&self) -> Task<Message> {
        let mut t = vec![
            Task::perform(
                request(json!({"v":1,"id":1,"type":"health"})),
                Message::Health,
            ),
            Task::perform(
                request(json!({"v":1,"id":2,"type":"chats"})),
                Message::Chats,
            ),
        ];
        if let Some(c) = &self.open {
            t.push(load(c.clone(), None)); // also closes any gap left by a disconnect
        }
        Task::batch(t)
    }

    fn can_send(&self) -> bool {
        self.linked && !self.sending && self.open.is_some() && !self.draft.trim().is_empty()
    }

    fn set_state(&mut self, s: Value) {
        self.qr = s["qr"].as_str().and_then(|q| qr_code::Data::new(q).ok());
        self.account_avatar_display_path =
            s["account_avatar_path"].as_str().and_then(circular_avatar);
        self.state = s;
    }

    fn update(&mut self, m: Message) -> Task<Message> {
        match m {
            Message::Link(up) => {
                self.linked = up;
                self.log(if up {
                    "Connected to daemon event stream"
                } else {
                    "Disconnected from daemon; reconnecting"
                });
                if up {
                    self.error = None;
                    return self.refresh();
                }
            }
            Message::Event(v) => match v["type"].as_str() {
                Some("state") => {
                    self.set_state(v["state"].clone());
                    self.log(format!(
                        "Connection state: {}",
                        self.state["kind"].as_str().unwrap_or("unknown")
                    ));
                }
                Some("message") => {
                    let mut tasks = vec![Task::perform(
                        request(json!({"v":1,"id":2,"type":"chats"})),
                        Message::Chats,
                    )];
                    if let Some(chat) = self.open.clone() {
                        tasks.push(load(chat, None));
                    }
                    return Task::batch(tasks);
                }
                Some("contacts_changed") => {
                    let mut tasks = vec![Task::perform(
                        request(json!({"v":1,"id":2,"type":"chats"})),
                        Message::Chats,
                    )];
                    if let Some(chat) = self.open.clone() {
                        tasks.push(load(chat, None));
                    }
                    return Task::batch(tasks);
                }
                Some("error") => {
                    self.error = v["message"].as_str().map(str::to_string);
                    if let Some(e) = &self.error {
                        self.log(format!("Error: {e}"));
                    }
                }
                _ => {}
            },
            Message::Health(Ok(v)) => self.set_state(v["state"].clone()),
            Message::Chats(Ok(v)) => {
                let mut chats: Vec<Chat> =
                    serde_json::from_value(v["chats"].clone()).unwrap_or_default();
                for chat in &mut chats {
                    if let Some(path) = &chat.avatar_path {
                        chat.avatar_display_path = circular_avatar(path);
                        if let Ok(bytes) = std::fs::read(path) {
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            bytes.hash(&mut hasher);
                            chat.avatar_hash = Some(hasher.finish());
                        }
                    }
                }
                self.chats = chats;
            }
            Message::Health(Err(e))
            | Message::Chats(Err(e))
            | Message::History { r: Err(e), .. } => {
                self.log(format!("Request failed: {e}"));
                self.error = Some(e)
            }
            Message::History {
                chat,
                older,
                r: Ok(v),
            } => {
                if self.open.as_deref() != Some(chat.as_str()) {
                    return Task::none(); // reply for a chat that is no longer open
                }
                let mut page: Vec<Line> =
                    serde_json::from_value(v["messages"].clone()).unwrap_or_default();
                self.more = page.len() as i64 == PAGE;
                if let Some(oldest) = page.last() {
                    self.cursor = oldest.seq.map(|s| (oldest.ts, s));
                }
                page.reverse(); // daemon sends newest first
                if older {
                    page.retain(|l| !self.lines.iter().any(|x| x.id == l.id));
                    page.append(&mut self.lines);
                } else {
                    for old in &self.lines {
                        if !page.iter().any(|l| l.id == old.id) {
                            page.push(old.clone());
                        }
                    }
                    page.sort_by_key(|l| (l.ts, l.seq.unwrap_or(0)));
                }
                self.lines = page;
            }
            Message::Open(chat) => {
                self.log(format!("Opened chat {chat}"));
                self.open = Some(chat.clone());
                self.pending_file = None;
                self.replying_to = None;
                self.open_message_menu = None;
                self.show_chat_menu = false;
                (self.lines, self.cursor, self.more) = (Vec::new(), None, false);
                self.notice = None;
                return load(chat, None);
            }
            Message::Search(s) => self.search = s,
            Message::Attach => return Task::perform(pick_file(), Message::Picked),
            Message::Picked(Ok(Some(path))) => self.pending_file = Some(path),
            Message::Picked(Ok(None)) => {} // dialog cancelled
            Message::Picked(Err(e)) => {
                self.log(format!("File picker: {e}"));
                self.error = Some(e)
            }
            Message::Dropped(path) => {
                if self.open.is_none() {
                    self.error =
                        Some("Open a chat first, then drop the file on the window.".into());
                } else if let Some(p) = path.to_str() {
                    self.pending_file = Some(p.to_string()); // several files dropped at once: the last one wins
                } else {
                    self.error = Some("That file name cannot be sent (not valid UTF-8).".into());
                }
            }
            Message::CancelFile => self.pending_file = None,
            Message::Reply(line) => {
                self.replying_to = Some(line);
                self.open_message_menu = None;
            }
            Message::MessageClick(line) => {
                if self.last_message_click.as_ref().is_some_and(|(id, at)| {
                    id == &line.id && at.elapsed() <= Duration::from_millis(500)
                }) {
                    self.replying_to = Some(line);
                    self.last_message_click = None;
                } else {
                    self.last_message_click = Some((line.id.clone(), Instant::now()));
                }
            }
            Message::CancelReply => self.replying_to = None,
            Message::ToggleMessageMenu(id) => {
                self.open_message_menu = if self.open_message_menu.as_deref() == Some(id.as_str()) {
                    None
                } else {
                    Some(id)
                };
            }
            Message::ToggleChatMenu => self.show_chat_menu = !self.show_chat_menu,
            Message::ToggleSidebarMenu => self.show_sidebar_menu = !self.show_sidebar_menu,
            Message::ToggleAccountInfo => self.show_account_info = !self.show_account_info,
            Message::ConfirmFile => {
                let (Some(chat), Some(path)) = (self.open.clone(), self.pending_file.clone())
                else {
                    return Task::none();
                };
                if !self.linked || self.sending {
                    return Task::none();
                }
                self.sending = true;
                self.error = None;
                self.notice = Some("Uploading file...".into());
                return Task::perform(
                    request_file(json!({"v":1,"id":7,"type":"send_file","chat":chat,"path":path})),
                    Message::FileSent,
                );
            }
            Message::FileSent(r) => {
                self.sending = false;
                self.notice = None;
                match r {
                    Ok(_) => {
                        self.pending_file = None;
                        self.error = None;
                    }
                    Err(e) => {
                        self.log(format!("File send failed: {e}"));
                        self.error = Some(e)
                    } // keep the staged file so it can be retried
                }
            }
            Message::SyncStart(chat) => {
                self.show_chat_menu = false;
                self.show_sidebar_menu = false;
                self.notice = Some("Asking your phone for older messages...".into());
                return Task::perform(
                    request(json!({"v":1,"id":8,"type":"sync","chat":chat})),
                    Message::SyncStarted,
                );
            }
            Message::SyncStarted(r) => match r {
                Ok(_) => {
                    self.error = None;
                    return poll_sync();
                }
                Err(e) => {
                    self.notice = None;
                    self.error = Some(e);
                }
            },
            Message::SyncStatus(Ok(v)) => {
                let n = |k: &str| v[k].as_u64().unwrap_or(0);
                if v["running"].as_bool().unwrap_or(false) {
                    self.notice = Some(format!(
                        "Syncing: {} of {} chats done, {} batches received",
                        n("done"),
                        n("total"),
                        n("rounds")
                    ));
                    return poll_sync();
                }
                self.notice = Some(if n("asked") == 0 {
                    "Sync: the request could not be sent to the phone (not connected?). See the daemon log.".to_string()
                } else if n("rounds") == 0 {
                    format!(
                        "Sync: asked the phone {} time(s) but no older messages arrived. Phone offline, nothing older, or the answer is not reaching the app: check the daemon log.",
                        n("asked")
                    )
                } else {
                    format!(
                        "Sync finished: {} batches received. The phone only returns what it still holds.",
                        n("rounds")
                    )
                });
                if let Some(notice) = &self.notice {
                    self.log(notice.clone());
                }
            }
            Message::SyncStatus(Err(e)) => {
                self.notice = None;
                self.error = Some(e);
            }
            Message::OpenFile(p) => {
                self.log(format!("Opening {p}"));
                return Task::perform(open_path(p), Message::FileOpened);
            }
            Message::FileOpened(Ok(())) => self.error = None,
            Message::FileOpened(Err(e)) => {
                self.log(format!("Open file failed: {e}"));
                self.error = Some(e);
            }
            Message::ToggleLog => {
                self.show_sidebar_menu = false;
                self.show_log = !self.show_log;
            }
            Message::ClearLog => self.log.clear(),
            Message::NewChat => {
                // Digits only, E.164 length. A number without its country code points at no real
                // account: the send "succeeds" and nothing is delivered.
                let valid_number_chars = self
                    .search
                    .chars()
                    .all(|c| c.is_ascii_digit() || matches!(c, '+' | ' ' | '-' | '(' | ')'));
                let digits: String = self.search.chars().filter(|c| c.is_ascii_digit()).collect();
                if !valid_number_chars || !(8..=15).contains(&digits.len()) {
                    return Task::none();
                }
                self.search.clear();
                self.error = None;
                return self.update(Message::Open(format!("{digits}@s.whatsapp.net")));
            }
            Message::Draft(d) => self.draft = d,
            Message::Send => {
                let (Some(chat), true) = (self.open.clone(), self.can_send()) else {
                    return Task::none();
                };
                self.sending = true;
                let reply_to = self.replying_to.as_ref().map(|l| json!({"id":l.id,"sender":l.sender,"from_me":l.from_me,"text":l.text,"media_kind":l.media_kind}));
                let req = json!({"v":1,"id":4,"type":"send_text","chat":chat,"text":self.draft,"reply_to":reply_to});
                return Task::perform(request_slow(req), Message::Sent);
            }
            Message::Sent(r) => {
                self.sending = false;
                match r {
                    Ok(_) => {
                        // The sent line arrives through the live event stream.
                        self.draft.clear();
                        self.replying_to = None;
                        self.error = None;
                    }
                    Err(e) => self.error = Some(e), // keep the draft so nothing is lost
                }
            }
            Message::Delete => {
                self.show_chat_menu = false;
                if let Some(chat) = self.open.clone() {
                    return Task::perform(
                        request(json!({"v":1,"id":5,"type":"delete_chat","chat":chat})),
                        Message::Deleted,
                    );
                }
            }
            Message::Deleted(r) => match r {
                Ok(_) => {
                    self.open = None;
                    self.pending_file = None;
                    self.lines.clear();
                    self.cursor = None;
                    self.more = false;
                    return self.refresh();
                }
                Err(e) => self.error = Some(e),
            },
            Message::Category(c) => {
                self.category = c;
                self.open = None;
                self.pending_file = None;
                self.lines.clear();
            }
            Message::AskPhone => {
                self.show_chat_menu = false;
                if let Some(chat) = self.open.clone() {
                    self.log(format!("Requested older messages for {chat}"));
                    self.notice = Some("Asking your phone for older messages...".into());
                    return Task::perform(
                        request_slow(json!({"v":1,"id":6,"type":"fetch_older","chat":chat})),
                        Message::AskedPhone,
                    );
                }
            }
            Message::AskedPhone(r) => match r {
                Ok(_) => {
                    self.error = None;
                    self.notice = Some("Asked your phone. Older messages appear when it answers (it must be online).".into());
                }
                Err(e) => {
                    self.log(format!("Older message request failed: {e}"));
                    self.notice = None;
                    self.error = Some(e);
                }
            },
            Message::Older => {
                if let (Some(chat), Some(c)) = (self.open.clone(), self.cursor) {
                    return load(chat, Some(c));
                }
            }
        }
        Task::none()
    }

    fn view(&self) -> Element<'_, Message> {
        let kind = self.state["kind"].as_str().unwrap_or("unknown");
        let body: Element<'_, Message> = if let Some(qr) = &self.qr {
            column![
                text("WhatsApp > Linked devices > Link a device, then scan:"),
                qr_code(qr)
            ]
            .spacing(16)
            .into()
        } else {
            let needle = self.search.trim().to_lowercase();
            let is_raw_name = |c: &Chat| c.name.as_deref().is_none_or(|name| name == c.chat);
            let raw_avatars: HashSet<u64> = self
                .chats
                .iter()
                .filter(|c| is_raw_name(c))
                .filter_map(|c| c.avatar_hash)
                .collect();
            let named_avatars: HashSet<u64> = self
                .chats
                .iter()
                .filter(|c| !is_raw_name(c))
                .filter_map(|c| c.avatar_hash)
                .collect();
            let duplicate_avatar_aliases: HashSet<u64> =
                raw_avatars.intersection(&named_avatars).copied().collect();
            let mut seen_avatar_aliases = HashSet::new();
            let visible_chats = self
                .chats
                .iter()
                .filter(|c| match self.category {
                    Category::Chats => {
                        !c.chat.starts_with("status@") && !c.chat.ends_with("@newsletter")
                    }
                    Category::Status => c.chat.starts_with("status@"),
                    Category::Channels => c.chat.ends_with("@newsletter"),
                })
                .filter(|c| {
                    needle.is_empty()
                        || c.name
                            .as_deref()
                            .unwrap_or("")
                            .to_lowercase()
                            .contains(&needle)
                        || c.chat.to_lowercase().contains(&needle)
                })
                .filter(|c| !needle.is_empty() || c.last_ts > 0)
                .filter(|c| {
                    c.avatar_hash.is_none_or(|hash| {
                        !duplicate_avatar_aliases.contains(&hash)
                            || seen_avatar_aliases.insert(hash)
                    })
                });
            let chats = scrollable(
                column(visible_chats.map(|c| {
                    let display_name = if is_raw_name(c) {
                        c.avatar_hash
                            .and_then(|hash| {
                                self.chats
                                    .iter()
                                    .find(|other| {
                                        other.avatar_hash == Some(hash) && !is_raw_name(other)
                                    })
                                    .and_then(|other| other.name.as_deref())
                            })
                            .unwrap_or(c.name.as_deref().unwrap_or(c.chat.as_str()))
                    } else {
                        c.name.as_deref().unwrap_or(c.chat.as_str())
                    };
                    let preview = c.last_message.as_deref().unwrap_or(if c.last_ts > 0 {
                        "Conversation"
                    } else {
                        "Contact"
                    });
                    let label = column![
                        text(display_name).size(15),
                        text(preview)
                            .size(12)
                            .color(Color::from_rgb(0.58, 0.66, 0.67))
                    ]
                    .spacing(3);
                    let item: Element<'_, Message> = if let Some(path) = &c.avatar_display_path {
                        row![
                            image(image::Handle::from_path(path))
                                .width(Length::Fixed(44.0))
                                .height(Length::Fixed(44.0)),
                            label
                        ]
                        .spacing(8)
                        .align_y(Alignment::Center)
                        .into()
                    } else {
                        let initial = display_name
                            .chars()
                            .next()
                            .unwrap_or('•')
                            .to_uppercase()
                            .to_string();
                        row![
                            container(
                                text(initial)
                                    .size(18)
                                    .color(Color::from_rgb(0.75, 0.70, 1.0))
                            )
                            .center_x(Length::Fill)
                            .center_y(Length::Fill)
                            .width(Length::Fixed(44.0))
                            .height(Length::Fixed(44.0))
                            .style(|_| {
                                iced::widget::container::Style {
                                    background: Some(Color::from_rgb(0.17, 0.15, 0.27).into()),
                                    border: iced::Border {
                                        radius: 22.0.into(),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                }
                            }),
                            label
                        ]
                        .spacing(8)
                        .align_y(Alignment::Center)
                        .into()
                    };
                    button(item)
                        .padding(10)
                        .width(Length::Fill)
                        .style(chat_row_style)
                        .on_press(Message::Open(c.chat.clone()))
                        .into()
                }))
                .spacing(4),
            )
            .width(Length::Fixed(360.0))
            .height(Length::Fill);

            let mut convo = column![].spacing(3).width(Length::Fill);
            if self.more {
                convo = convo.push(
                    button("Load older")
                        .on_press(Message::Older)
                        .style(subtle_button_style),
                );
            }
            for l in &self.lines {
                let who = if l.from_me {
                    "me"
                } else {
                    l.sender_name
                        .as_deref()
                        .filter(|name| !name.trim().is_empty() && *name != l.sender)
                        .or_else(|| {
                            self.chats
                                .iter()
                                .find(|c| c.chat == l.sender)
                                .and_then(|c| c.name.as_deref())
                        })
                        .filter(|name| *name != l.sender)
                        .unwrap_or("Contact")
                };
                let body = match (&l.text, &l.media_kind) {
                    (Some(t), _) => t.clone(),
                    (None, Some(k)) => format!("[{k}]"),
                    _ => "[non-text message]".to_string(),
                };
                let is_group = self
                    .open
                    .as_deref()
                    .is_some_and(|chat| chat.ends_with("@g.us"));
                let message_text = if l.from_me || !is_group {
                    body.clone()
                } else {
                    format!("{who}: {body}")
                };
                let mut item = column![
                    text(message_text)
                        .width(Length::Shrink)
                        .wrapping(iced::widget::text::Wrapping::Word)
                        .color(Color::from_rgb(0.93, 0.94, 0.94))
                ]
                .spacing(4);
                if let Some(when) = chrono::DateTime::from_timestamp(l.ts, 0).map(|t| {
                    t.with_timezone(&chrono::Local)
                        .format("%d %b %H:%M")
                        .to_string()
                }) {
                    item = item.push(text(when).size(11).color(Color::from_rgb(0.66, 0.69, 0.70)));
                }
                // Only images are decoded inline; a document is never handed to the image decoder.
                if let Some(p) = l
                    .media_path
                    .as_ref()
                    .filter(|_| l.media_kind.as_deref() == Some("image"))
                {
                    item = item.push(
                        mouse_area(image(image::Handle::from_path(p)).width(Length::Fixed(240.0)))
                            .on_press(Message::OpenFile(p.clone())),
                    );
                }
                if l.media_kind.as_deref() == Some("document") {
                    item = match &l.media_path {
                        Some(p) => item.push(
                            button(text("Open file").size(13))
                                .on_press(Message::OpenFile(p.clone()))
                                .style(subtle_button_style),
                        ),
                        None => item.push(
                            text("File not saved on this computer (received before the daemon could download it, or the download failed; see the daemon log)")
                                .size(11)
                                .color(Color::from_rgb(0.66, 0.69, 0.70)),
                        ),
                    };
                }
                let bubble = container(item).padding(8).max_width(500.0).style(move |_| {
                    iced::widget::container::Style {
                        background: Some(if l.from_me {
                            Color::from_rgb(0.00, 0.36, 0.29).into()
                        } else {
                            Color::from_rgb(0.13, 0.13, 0.13).into()
                        }),
                        border: iced::Border {
                            radius: 10.0.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    }
                });
                let bubble = mouse_area(bubble).on_press(Message::MessageClick(l.clone()));
                let dropdown =
                    mouse_area(text("⌄").size(17).color(Color::from_rgb(0.70, 0.75, 0.75)))
                        .on_press(Message::ToggleMessageMenu(l.id.clone()));
                let mut message_card =
                    column![row![bubble, dropdown].spacing(3).align_y(Alignment::Start)].spacing(2);
                if self.open_message_menu.as_deref() == Some(l.id.as_str()) {
                    message_card = message_card.push(
                        button("Reply")
                            .on_press(Message::Reply(l.clone()))
                            .style(subtle_button_style),
                    );
                    if let Some(path) = l
                        .media_path
                        .as_ref()
                        .filter(|_| l.media_kind.as_deref() == Some("document"))
                    {
                        message_card = message_card.push(
                            button("Open file")
                                .on_press(Message::OpenFile(path.clone()))
                                .style(subtle_button_style),
                        );
                    }
                }
                let bubble = container(message_card);
                let aligned = if l.from_me {
                    row![iced::widget::Space::new().width(Length::Fill), bubble]
                        .align_y(Alignment::End)
                } else {
                    row![bubble, iced::widget::Space::new().width(Length::Fill)]
                        .align_y(Alignment::Start)
                };
                convo = convo.push(aligned);
            }
            let mut right = column![].spacing(8).width(Length::Fill);
            if let Some(open) = &self.open {
                let chat = self.chats.iter().find(|c| &c.chat == open);
                let title = chat.and_then(|c| c.name.as_deref()).unwrap_or(open);
                let heading: Element<'_, Message> = if let Some(path) =
                    chat.and_then(|c| c.avatar_display_path.as_deref())
                {
                    row![
                        image(image::Handle::from_path(path))
                            .width(Length::Fixed(48.0))
                            .height(Length::Fixed(48.0)),
                        column![
                            text(title).size(17),
                            text(open.as_str())
                                .size(11)
                                .color(Color::from_rgb(0.66, 0.68, 0.68))
                        ]
                        .spacing(2)
                    ]
                    .spacing(8)
                    .align_y(Alignment::Center)
                    .into()
                } else {
                    let initial = title
                        .chars()
                        .next()
                        .unwrap_or('•')
                        .to_uppercase()
                        .to_string();
                    row![
                        container(
                            text(initial)
                                .size(20)
                                .color(Color::from_rgb(0.75, 0.70, 1.0))
                        )
                        .center_x(Length::Fill)
                        .center_y(Length::Fill)
                        .width(Length::Fixed(48.0))
                        .height(Length::Fixed(48.0))
                        .style(|_| iced::widget::container::Style {
                            background: Some(Color::from_rgb(0.17, 0.15, 0.27).into()),
                            border: iced::Border {
                                radius: 24.0.into(),
                                ..Default::default()
                            },
                            ..Default::default()
                        }),
                        column![
                            text(title).size(17),
                            text(open.as_str())
                                .size(11)
                                .color(Color::from_rgb(0.66, 0.68, 0.68))
                        ]
                        .spacing(2)
                    ]
                    .align_y(Alignment::Center)
                    .into()
                };
                right = right.push(
                    row![
                        container(heading)
                            .padding(8)
                            .style(|_| iced::widget::container::Style {
                                background: Some(Color::from_rgb(0.12, 0.18, 0.20).into()),
                                border: iced::Border {
                                    radius: 9.0.into(),
                                    ..Default::default()
                                },
                                ..Default::default()
                            }),
                        iced::widget::Space::new().width(Length::Fill),
                        button("⋮")
                            .on_press(Message::ToggleChatMenu)
                            .style(subtle_button_style),
                    ]
                    .align_y(Alignment::Center),
                );
                if self.show_chat_menu {
                    let mut chat_actions = column![
                        button("Sync this chat")
                            .on_press(Message::SyncStart(Some(open.clone())))
                            .style(subtle_button_style),
                        button("Delete chat")
                            .on_press(Message::Delete)
                            .style(subtle_button_style),
                    ]
                    .spacing(4);
                    if !self.more {
                        chat_actions = chat_actions.push(
                            button("Ask phone for older messages")
                                .on_press(Message::AskPhone)
                                .style(subtle_button_style),
                        );
                    }
                    right = right.push(chat_actions);
                }
            }
            // anchor_bottom: the view starts at, and stays pinned to, the newest message. History is loaded
            // newest-first, so without this the chat opened at the top (the oldest loaded message).
            right = right.push(
                scrollable(convo)
                    .anchor_bottom()
                    .width(Length::Fill)
                    .height(Length::Fill),
            );
            if self.open.is_none() {
                right = right.push(container(column![text("Keep your conversations close").size(22), text("Choose a chat to view messages, or start a new conversation from the left.")]).center_x(Length::Fill).center_y(Length::Fill).width(Length::Fill).height(Length::Fill));
            }
            if self.open.is_some() {
                if let Some(quoted) = &self.replying_to {
                    let by = if quoted.from_me {
                        "You"
                    } else {
                        quoted.sender_name.as_deref().unwrap_or("Contact")
                    };
                    let excerpt = quoted
                        .text
                        .as_deref()
                        .or(quoted.media_kind.as_deref())
                        .unwrap_or("Message");
                    right = right.push(
                        container(
                            row![
                                column![
                                    text(format!("Replying to {by}")).size(12),
                                    text(excerpt).size(13)
                                ],
                                iced::widget::Space::new().width(Length::Fill),
                                button("×")
                                    .on_press(Message::CancelReply)
                                    .style(subtle_button_style)
                            ]
                            .spacing(8)
                            .align_y(Alignment::Center),
                        )
                        .padding(10)
                        .style(|_| iced::widget::container::Style {
                            background: Some(Color::from_rgb(0.12, 0.18, 0.20).into()),
                            border: iced::Border {
                                radius: 8.0.into(),
                                ..Default::default()
                            },
                            ..Default::default()
                        }),
                    );
                }
                if let Some(p) = &self.pending_file {
                    let name = std::path::Path::new(p)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(p.as_str());
                    right = right.push(
                        row![
                            text(format!("Send \"{name}\"?")),
                            button("Send").style(accent_button_style).on_press_maybe(
                                (self.linked && !self.sending).then_some(Message::ConfirmFile)
                            ),
                            button("Cancel")
                                .on_press(Message::CancelFile)
                                .style(subtle_button_style),
                        ]
                        .spacing(8)
                        .align_y(Alignment::Center),
                    );
                }
                right = right.push(
                    container(
                        row![
                            button("+")
                                .on_press_maybe(
                                    (self.linked && !self.sending).then_some(Message::Attach)
                                )
                                .style(subtle_button_style),
                            text_input("Message (or drop a file here)", &self.draft)
                                .on_input(Message::Draft)
                                .on_submit(Message::Send)
                                .padding([10, 14])
                                .style(search_input_style),
                            button("Send")
                                .on_press_maybe(self.can_send().then_some(Message::Send))
                                .style(accent_button_style),
                        ]
                        .spacing(8),
                    )
                    .padding(8)
                    .style(|_| iced::widget::container::Style {
                        background: Some(Color::from_rgb(0.10, 0.10, 0.10).into()),
                        border: iced::Border {
                            radius: 10.0.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    }),
                );
            }
            let account = self.state["account"].as_str().unwrap_or("Not linked");
            let account_jid = self.state["account_jid"].as_str();
            let account_pn = self.state["account_pn"].as_str();
            let account_lid = self.state["account_lid"].as_str();
            let account_avatar = self.account_avatar_display_path.as_deref().or_else(|| {
                self.chats
                    .iter()
                    .find(|c| {
                        [account_jid, account_pn, account_lid]
                            .into_iter()
                            .flatten()
                            .any(|id| c.chat == id)
                    })
                    .and_then(|c| c.avatar_display_path.as_deref())
            });
            let digits: String = self.search.chars().filter(|c| c.is_ascii_digit()).collect();
            let valid_phone = self
                .search
                .chars()
                .all(|c| c.is_ascii_digit() || matches!(c, '+' | ' ' | '-' | '(' | ')'))
                && (8..=15).contains(&digits.len());
            let brand = row![
                text("WhatsApp").size(23),
                iced::widget::Space::new().width(Length::Fill),
                button("⋮")
                    .on_press(Message::ToggleSidebarMenu)
                    .style(subtle_button_style),
                button("+")
                    .on_press(Message::NewChat)
                    .style(accent_button_style),
            ]
            .align_y(Alignment::Center);
            let mut left = column![
                brand,
                text_input("Search chats, contacts or number", &self.search)
                    .on_input(Message::Search)
                    .on_submit(Message::NewChat)
                    .padding([10, 14])
                    .style(search_input_style),
            ]
            .spacing(8)
            .width(Length::Fixed(360.0));
            if self.show_sidebar_menu {
                left = left.push(
                    column![
                        button("Sync all chats")
                            .on_press(Message::SyncStart(None))
                            .style(subtle_button_style),
                        button("Show log")
                            .on_press(Message::ToggleLog)
                            .style(subtle_button_style),
                    ]
                    .spacing(4),
                );
            }
            if valid_phone {
                left = left.push(
                    button(text(format!("Start chat with +{digits}")))
                        .width(Length::Fill)
                        .on_press(Message::NewChat)
                        .style(accent_button_style),
                );
            }
            left = left.push(chats);
            let account_initial = account.chars().next().unwrap_or('A').to_string();
            let account_photo: Element<'_, Message> = if let Some(path) = account_avatar {
                image(image::Handle::from_path(path))
                    .width(Length::Fixed(38.0))
                    .height(Length::Fixed(38.0))
                    .into()
            } else {
                container(text(account_initial).size(17).color(Color::WHITE))
                    .center_x(Length::Fill)
                    .center_y(Length::Fill)
                    .width(Length::Fixed(38.0))
                    .height(Length::Fixed(38.0))
                    .style(|_| iced::widget::container::Style {
                        background: Some(Color::from_rgb(0.05, 0.72, 0.36).into()),
                        border: iced::Border {
                            radius: 19.0.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .into()
            };
            let profile = button(account_photo)
                .padding(0)
                .on_press(Message::ToggleAccountInfo)
                .style(rail_button_style);
            let rail = column![
                button("▣")
                    .width(Length::Fill)
                    .on_press(Message::Category(Category::Chats))
                    .style(rail_button_style),
                button("◉")
                    .width(Length::Fill)
                    .on_press(Message::Category(Category::Status))
                    .style(rail_button_style),
                button("◌")
                    .width(Length::Fill)
                    .on_press(Message::Category(Category::Channels))
                    .style(rail_button_style),
                iced::widget::Space::new().height(Length::Fill),
                profile,
            ]
            .spacing(12)
            .width(Length::Fixed(52.0))
            .height(Length::Fill);
            let rail: Element<'_, Message> = if self.show_account_info {
                let popover = container(
                    text(account)
                        .size(13)
                        .color(Color::from_rgb(0.92, 0.93, 0.93)),
                )
                .padding(12)
                .width(Length::Fixed(190.0))
                .style(|_| iced::widget::container::Style {
                    background: Some(Color::from_rgb(0.16, 0.16, 0.16).into()),
                    border: iced::Border {
                        radius: 10.0.into(),
                        ..Default::default()
                    },
                    shadow: iced::Shadow {
                        color: Color::from_rgba(0.0, 0.0, 0.0, 0.45),
                        offset: iced::Vector::new(0.0, 4.0),
                        blur_radius: 14.0,
                    },
                    ..Default::default()
                });
                iced::widget::stack![
                    rail,
                    iced::widget::float(popover).translate(|bounds, viewport| {
                        iced::Vector::new(60.0, (viewport.height - bounds.height - 72.0).max(8.0))
                    })
                ]
                .width(Length::Fixed(52.0))
                .height(Length::Fill)
                .into()
            } else {
                rail.into()
            };
            row![
                container(rail)
                    .padding([14, 6])
                    .height(Length::Fill)
                    .style(|_| iced::widget::container::Style {
                        background: Some(Color::from_rgb(0.10, 0.10, 0.10).into()),
                        ..Default::default()
                    }),
                container(left)
                    .padding(12)
                    .style(|_| iced::widget::container::Style {
                        background: Some(Color::from_rgb(0.06, 0.06, 0.06).into()),
                        border: iced::Border {
                            radius: 10.0.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    }),
                container(right)
                    .padding(12)
                    .width(Length::Fill)
                    .height(Length::Fill)
                    .style(|_| iced::widget::container::Style {
                        background: Some(Color::from_rgb(0.12, 0.12, 0.12).into()),
                        border: iced::Border {
                            radius: 10.0.into(),
                            ..Default::default()
                        },
                        ..Default::default()
                    })
            ]
            .spacing(0)
            .height(Length::Fill)
            .into()
        };
        let content: Element<'_, Message> = if self.show_log {
            let mut entries = column![
                text("Activity log").size(20),
                text(if self.linked {
                    format!("Connected · {kind}")
                } else {
                    "Daemon is not connected. Start whatsapp-daemon.".to_string()
                })
                .size(13)
            ];
            if let Some(message) = &self.notice {
                entries = entries.push(text(format!("Info: {message}")));
            }
            if let Some(error) = &self.error {
                entries = entries
                    .push(text(format!("Error: {error}")).color(Color::from_rgb(0.72, 0.16, 0.14)));
            }
            for entry in self.log.iter().rev() {
                entries = entries.push(text(entry.as_str()).size(13));
            }
            column![
                row![
                    text("Show log").size(22),
                    iced::widget::Space::new().width(Length::Fill),
                    button("Clear")
                        .on_press(Message::ClearLog)
                        .style(subtle_button_style),
                    button("Close")
                        .on_press(Message::ToggleLog)
                        .style(subtle_button_style)
                ]
                .spacing(12),
                scrollable(entries.spacing(8)).height(Length::Fill)
            ]
            .spacing(12)
            .padding(20)
            .into()
        } else {
            body
        };
        container(column![content].spacing(10))
            .padding(12)
            .style(|_| iced::widget::container::Style {
                background: Some(Color::from_rgb(0.06, 0.06, 0.06).into()),
                ..Default::default()
            })
            .into()
    }

    fn subscription(&self) -> Subscription<Message> {
        Subscription::batch([
            Subscription::run(events),
            iced::event::listen_with(on_event),
        ])
    }
}

fn main() -> iced::Result {
    iced::application(App::default, App::update, App::view)
        .title("WhatsApp")
        .theme(iced::Theme::Dark)
        .subscription(App::subscription)
        .run()
}
