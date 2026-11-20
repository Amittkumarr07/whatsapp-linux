//! Thin client: talks JSON lines to the daemon socket. It never touches the protocol or the DB.
//! One long-lived connection for live events; short one-shot connections for requests.
use iced::futures::{SinkExt, Stream};
use iced::widget::{
    button, column, container, image, mouse_area, qr_code, row, scrollable, text, text_input,
};
use iced::alignment::Horizontal;
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

// WhatsApp dark palette, 0xRRGGBB.
const APP_BG: u32 = 0x0b141a; // conversation background
const PANEL: u32 = 0x111b21; // chat list and rail
const HEADER: u32 = 0x202c33; // bars, incoming bubbles, hover
const SELECTED: u32 = 0x2a3942; // selected row, input fields
const DIVIDER: u32 = 0x222d34;
const OUT_BUBBLE: u32 = 0x005c4b;
const TEXT: u32 = 0xe9edef;
const MUTED: u32 = 0x8696a0;
const ICON: u32 = 0xaebac1;
const ACCENT: u32 = 0x00a884;
const DATE_CHIP: u32 = 0x182229;
const EMPTY_BG: u32 = 0x222e35;
const AVATAR_BG: u32 = 0x6b7c85;

type BtnStatus = iced::widget::button::Status;
type BtnStyle = iced::widget::button::Style;
type BoxStyle = iced::widget::container::Style;

fn c(hex: u32) -> Color {
    Color::from_rgb8((hex >> 16) as u8, (hex >> 8) as u8, hex as u8)
}

fn hot(status: BtnStatus) -> bool {
    matches!(status, BtnStatus::Hovered | BtnStatus::Pressed)
}

/// A flat, square panel of one colour.
fn flat(bg: u32) -> impl Fn(&iced::Theme) -> BoxStyle {
    move |_| BoxStyle {
        background: Some(c(bg).into()),
        ..Default::default()
    }
}

/// Menu items and small dialog buttons.
fn subtle_button_style(_: &iced::Theme, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: Some(c(if hot(status) { SELECTED } else { HEADER }).into()),
        text_color: c(TEXT),
        border: iced::Border {
            radius: 8.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn accent_button_style(_: &iced::Theme, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: Some(c(if hot(status) { 0x06cf9c } else { ACCENT }).into()),
        text_color: Color::WHITE,
        border: iced::Border {
            radius: 18.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Round, transparent icon button (search, menu, attach).
fn icon_button_style(_: &iced::Theme, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: if hot(status) {
            Some(c(SELECTED).into())
        } else {
            None
        },
        text_color: c(ICON),
        border: iced::Border {
            radius: 20.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The send arrow: green when it can be used, grey when not.
fn send_button_style(_: &iced::Theme, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: None,
        text_color: if matches!(status, BtnStatus::Disabled) {
            c(MUTED)
        } else {
            c(ACCENT)
        },
        ..Default::default()
    }
}

fn chat_row_style(selected: bool, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: if selected {
            Some(c(SELECTED).into())
        } else if hot(status) {
            Some(c(HEADER).into())
        } else {
            None
        },
        text_color: c(TEXT),
        border: iced::Border::default(),
        ..Default::default()
    }
}

fn rail_style(active: bool, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: if active {
            Some(c(SELECTED).into())
        } else if hot(status) {
            Some(c(HEADER).into())
        } else {
            None
        },
        text_color: c(ICON),
        border: iced::Border {
            radius: 22.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn chip_style(active: bool, status: BtnStatus) -> BtnStyle {
    BtnStyle {
        background: Some(
            if active {
                c(0x103529)
            } else if hot(status) {
                c(SELECTED)
            } else {
                c(HEADER)
            }
            .into(),
        ),
        text_color: if active { c(0x25d366) } else { c(MUTED) },
        border: iced::Border {
            radius: 16.0.into(),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Search and message inputs: borderless rounded field on a dark fill.
fn input_style(
    bg: u32,
) -> impl Fn(&iced::Theme, iced::widget::text_input::Status) -> iced::widget::text_input::Style {
    move |_, _| iced::widget::text_input::Style {
        background: c(bg).into(),
        border: iced::Border {
            color: Color::TRANSPARENT,
            width: 0.0,
            radius: 8.0.into(),
        },
        icon: c(MUTED),
        placeholder: c(MUTED),
        value: c(0xd1d7db),
        selection: Color {
            a: 0.35,
            ..c(ACCENT)
        },
    }
}

fn local(ts: i64) -> Option<chrono::DateTime<chrono::Local>> {
    chrono::DateTime::from_timestamp(ts, 0).map(|t| t.with_timezone(&chrono::Local))
}

fn clock(ts: i64) -> String {
    local(ts)
        .map(|t| t.format("%H:%M").to_string())
        .unwrap_or_default()
}

fn local_date(ts: i64) -> Option<chrono::NaiveDate> {
    local(ts).map(|t| t.date_naive())
}

fn days_ago(ts: i64) -> Option<i64> {
    Some((chrono::Local::now().date_naive() - local_date(ts)?).num_days())
}

/// Right-hand time in the chat list: clock today, "Yesterday", weekday this week, else a date.
fn chat_time(ts: i64) -> String {
    match days_ago(ts) {
        Some(0) => clock(ts),
        Some(1) => "Yesterday".to_string(),
        Some(2..=6) => local(ts)
            .map(|t| t.format("%A").to_string())
            .unwrap_or_default(),
        _ => local(ts)
            .map(|t| t.format("%d/%m/%Y").to_string())
            .unwrap_or_default(),
    }
}

/// The centred day chip inside a conversation.
fn day_label(ts: i64) -> String {
    match days_ago(ts) {
        Some(0) => "TODAY".to_string(),
        Some(1) => "YESTERDAY".to_string(),
        Some(2..=6) => local(ts)
            .map(|t| t.format("%A").to_string().to_uppercase())
            .unwrap_or_default(),
        _ => local(ts)
            .map(|t| t.format("%-d %B %Y").to_string())
            .unwrap_or_default(),
    }
}

/// A readable stand-in for an id that has no saved name.
fn pretty_id(id: &str) -> String {
    match id.split_once('@') {
        Some((user, "s.whatsapp.net"))
            if !user.is_empty() && user.chars().all(|ch| ch.is_ascii_digit()) =>
        {
            format!("+{user}")
        }
        Some((_, "g.us")) => "Group".to_string(),
        Some((_, "lid")) => "Contact".to_string(),
        _ => id.to_string(),
    }
}

/// Single-line, length-capped text for list rows.
fn one_line(s: &str, max: usize) -> String {
    let flat: String = s
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    let flat = flat.trim();
    if flat.chars().count() > max {
        format!("{}…", flat.chars().take(max).collect::<String>())
    } else {
        flat.to_string()
    }
}

/// Group sender names get one of a few fixed colours, stable per sender.
fn sender_color(id: &str) -> Color {
    const NAMES: [u32; 8] = [
        0x53bdeb, 0x25d366, 0xffd279, 0xff8a8c, 0xc29cff, 0xe26ab6, 0xfa6533, 0x7fa6ff,
    ];
    let mut h = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut h);
    c(NAMES[(h.finish() % NAMES.len() as u64) as usize])
}

/// Round photo, or a grey disc with the first letter when there is no photo.
fn avatar<'a>(path: Option<&str>, name: &str, size: f32) -> Element<'a, Message> {
    if let Some(path) = path {
        image(image::Handle::from_path(path))
            .width(Length::Fixed(size))
            .height(Length::Fixed(size))
            .into()
    } else {
        let initial = name
            .chars()
            .find(|ch| ch.is_alphanumeric())
            .unwrap_or('•')
            .to_uppercase()
            .to_string();
        container(text(initial).size(size * 0.42).color(c(0xdfe5e7)))
            .center_x(Length::Fixed(size))
            .center_y(Length::Fixed(size))
            .style(move |_| BoxStyle {
                background: Some(c(AVATAR_BG).into()),
                border: iced::Border {
                    radius: (size / 2.0).into(),
                    ..Default::default()
                },
                ..Default::default()
            })
            .into()
    }
}

/// A full-width status bar: warning (amber), error (red) or plain notice.
fn bar<'a>(msg: String, fg: u32, bg: u32, dismiss: bool) -> Element<'a, Message> {
    let mut r = row![
        text(msg).size(14).color(c(fg)),
        iced::widget::Space::new().width(Length::Fill)
    ]
    .spacing(8)
    .align_y(Alignment::Center);
    if dismiss {
        r = r.push(
            button(text("×").size(16))
                .on_press(Message::DismissBanner)
                .style(icon_button_style),
        );
    }
    container(r)
        .padding([8, 16])
        .width(Length::Fill)
        .style(flat(bg))
        .into()
}

fn date_chip<'a>(label: String) -> Element<'a, Message> {
    container(
        container(text(label).size(12).color(c(MUTED)))
            .padding([5, 12])
            .style(|_| BoxStyle {
                background: Some(c(DATE_CHIP).into()),
                border: iced::Border {
                    radius: 8.0.into(),
                    ..Default::default()
                },
                ..Default::default()
            }),
    )
    .center_x(Length::Fill)
    .padding([10, 0])
    .into()
}

/// Thin line between chat rows, indented past the avatar like WhatsApp's.
fn row_divider<'a>() -> Element<'a, Message> {
    row![
        iced::widget::Space::new().width(Length::Fixed(77.0)),
        container(iced::widget::Space::new())
            .width(Length::Fill)
            .height(Length::Fixed(1.0))
            .style(flat(DIVIDER)),
    ]
    .into()
}

fn divider_v<'a>() -> Element<'a, Message> {
    container(iced::widget::Space::new())
        .width(Length::Fixed(1.0))
        .height(Length::Fill)
        .style(flat(DIVIDER))
        .into()
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

fn avatar_cached(path: &str) -> (Option<String>, Option<u64>) {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::time::SystemTime;
    type Entry = ((SystemTime, u64), Option<String>, Option<u64>);
    static CACHE: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    let Some(stamp) = std::fs::metadata(path).ok().and_then(|m| Some((m.modified().ok()?, m.len()))) else {
        return (None, None);
    };
    let cache = CACHE.get_or_init(Default::default);
    if let Some((seen, circle, hash)) = cache.lock().unwrap_or_else(|p| p.into_inner()).get(path) {
        if *seen == stamp {
            return (circle.clone(), *hash);
        }
    }
    let circle = circular_avatar(path);
    let hash = std::fs::read(path).ok().map(|bytes| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut hasher);
        hasher.finish()
    });
    cache.lock().unwrap_or_else(|p| p.into_inner()).insert(path.to_string(), (stamp, circle.clone(), hash));
    (circle, hash)
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
enum CallUi {
    Ringing { from: String, video: bool },
    Dialing { to: String, video: bool },
    Active { peer: String, muted: bool, video: bool },
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
    GroupsOnly(bool),
    DismissBanner,
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
    CallStart { video: bool },
    CallAnswer,
    CallReject,
    CallHangup,
    CallToggleMute,
    CallDone(Result<Value, String>),
    ClearLog,
    DownloadMedia { chat: String, id: String },
    #[allow(dead_code)]
    MediaDownloaded { chat: String, id: String, r: Result<Value, String> },
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
    groups_only: bool,
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
    call: Option<CallUi>,
    log: Vec<String>,
}

fn sock() -> PathBuf {
    PathBuf::from(std::env::var("XDG_RUNTIME_DIR").unwrap_or_default()).join("whatsapp/daemon.sock")
}

async fn request(req: Value) -> Result<Value, String> {
    tokio::time::timeout(Duration::from_secs(15), request_inner(req))
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
    // Fallback: try direct media players (vlc, mpv) if system opener failed
    for player in ["vlc", "mpv", "totem"] {
        if tokio::process::Command::new(player)
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            return Ok(());
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

    /// Is this chat the one with your own account? Phone number and LID both count; device parts are ignored.
    fn is_self_chat(&self, chat: &str) -> bool {
        fn user(id: &str) -> &str {
            let u = id.split('@').next().unwrap_or(id);
            u.split(':').next().unwrap_or(u)
        }
        let who = user(chat);
        !who.is_empty()
            && ["account_pn", "account_lid", "account_jid"]
                .iter()
                .filter_map(|k| self.state[*k].as_str())
                .any(|id| user(id) == who)
    }

    /// "Your Name (You)", like WhatsApp's chat with yourself.
    fn self_label(&self) -> String {
        match self.state["account"].as_str() {
            Some(a) if !a.is_empty() && !a.contains('@') => format!("{a} (You)"),
            _ => "You".to_string(),
        }
    }

    /// The daemon says it is connected to WhatsApp (not just that this window reaches the daemon).
    fn connected(&self) -> bool {
        self.state["kind"].as_str() == Some("connected")
    }

    /// Sending needs both: the daemon socket and a live WhatsApp connection.
    fn online(&self) -> bool {
        self.linked && self.connected()
    }

    fn can_send(&self) -> bool {
        self.online() && !self.sending && self.open.is_some() && !self.draft.trim().is_empty()
    }

    /// Why nothing is happening, in plain words: daemon down, still connecting, or the last error.
    fn banners(&self) -> Option<Element<'_, Message>> {
        let mut bars = column![];
        let mut any = false;
        if let Some(call) = &self.call {
            let call_bar: Element<'_, Message> = match call {
                CallUi::Ringing { from, video } => {
                    let who = self
                        .chats
                        .iter()
                        .find(|c| c.chat == *from)
                        .and_then(|c| c.name.as_deref())
                        .filter(|n| *n != from.as_str())
                        .map_or_else(|| pretty_id(from), str::to_string);
                    let kind = if *video {
                        "Incoming video call (answers as audio)"
                    } else {
                        "Incoming audio call"
                    };
                    container(
                        row![
                            text(format!("📞  {kind} from {who}")).size(14).color(c(TEXT)),
                            iced::widget::Space::new().width(Length::Fill),
                            button(text("Answer").size(13))
                                .padding([6, 14])
                                .style(accent_button_style)
                                .on_press(Message::CallAnswer),
                            button(text("Reject").size(13))
                                .padding([6, 14])
                                .style(subtle_button_style)
                                .on_press(Message::CallReject),
                        ]
                        .spacing(10)
                        .align_y(Alignment::Center),
                    )
                    .padding([8, 16])
                    .width(Length::Fill)
                    .style(flat(0x1f352e))
                    .into()
                }
                CallUi::Dialing { to, video } => {
                    let who = self
                        .chats
                        .iter()
                        .find(|c| c.chat == *to)
                        .and_then(|c| c.name.as_deref())
                        .filter(|n| *n != to.as_str())
                        .map_or_else(|| pretty_id(to), str::to_string);
                    let label = if *video {
                        format!("🎥  Calling {who} (audio stream)...")
                    } else {
                        format!("📞  Calling {who}...")
                    };
                    container(
                        row![
                            text(label).size(14).color(c(TEXT)),
                            iced::widget::Space::new().width(Length::Fill),
                            button(text("Hang up").size(13))
                                .padding([6, 14])
                                .style(subtle_button_style)
                                .on_press(Message::CallHangup),
                        ]
                        .spacing(10)
                        .align_y(Alignment::Center),
                    )
                    .padding([8, 16])
                    .width(Length::Fill)
                    .style(flat(0x1f352e))
                    .into()
                }
                CallUi::Active { peer, muted, video } => {
                    let who = self
                        .chats
                        .iter()
                        .find(|c| c.chat == *peer)
                        .and_then(|c| c.name.as_deref())
                        .filter(|n| *n != peer.as_str())
                        .map_or_else(|| pretty_id(peer), str::to_string);
                    let icon = if *video { "🎥" } else { "📞" };
                    container(
                        row![
                            text(format!("{icon}  In call with {who}")).size(14).color(c(TEXT)),
                            iced::widget::Space::new().width(Length::Fill),
                            button(text(if *muted { "Unmute" } else { "Mute" }).size(13))
                                .padding([6, 14])
                                .style(subtle_button_style)
                                .on_press(Message::CallToggleMute),
                            button(text("Hang up").size(13))
                                .padding([6, 14])
                                .style(subtle_button_style)
                                .on_press(Message::CallHangup),
                        ]
                        .spacing(10)
                        .align_y(Alignment::Center),
                    )
                    .padding([8, 16])
                    .width(Length::Fill)
                    .style(flat(0x1f352e))
                    .into()
                }
            };
            bars = bars.push(call_bar);
            any = true;
        }
        if !self.linked {
            bars = bars.push(bar(
                format!(
                    "Not connected to the daemon (socket {}). Start it: cargo run -p whatsapp-daemon",
                    sock().display()
                ),
                0xffd279,
                0x2d2410,
                false,
            ));
            any = true;
        } else if self.qr.is_none() && !self.connected() {
            bars = bars.push(bar(
                "Connecting to WhatsApp... messages can be sent and received once this disappears."
                    .to_string(),
                0xffd279,
                0x2d2410,
                false,
            ));
            any = true;
        }
        if let Some(e) = &self.error {
            bars = bars.push(bar(e.clone(), 0xff8a8c, 0x3a1d22, true));
            any = true;
        }
        if let Some(n) = &self.notice {
            bars = bars.push(bar(n.clone(), TEXT, HEADER, true));
            any = true;
        }
        any.then(|| bars.into())
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
                    return Task::perform(
                        request(json!({"v":1,"id":2,"type":"chats"})),
                        Message::Chats,
                    );
                }
                Some("call") => {
                    let ev = &v["call"];
                    match ev["state"].as_str() {
                        Some("ringing") => {
                            let from = ev["from"].as_str().unwrap_or("Unknown").to_string();
                            let video = ev["video"].as_bool().unwrap_or(false);
                            self.log(format!("Incoming call from {from} (video={video})"));
                            self.call = Some(CallUi::Ringing { from, video });
                        }
                        Some("dialing") => {
                            let to = ev["to"].as_str().unwrap_or("Unknown").to_string();
                            let video = matches!(self.call, Some(CallUi::Dialing { video: true, .. }));
                            self.call = Some(CallUi::Dialing { to, video });
                        }
                        Some("active") => {
                            let peer = ev["peer"].as_str().unwrap_or("Unknown").to_string();
                            let video = match &self.call {
                                Some(CallUi::Ringing { video, .. })
                                | Some(CallUi::Dialing { video, .. })
                                | Some(CallUi::Active { video, .. }) => *video,
                                None => false,
                            };
                            self.log(format!("Call active with {peer}"));
                            self.call = Some(CallUi::Active {
                                peer,
                                muted: false,
                                video,
                            });
                        }
                        Some("ended") => {
                            self.log("Call ended");
                            self.call = None;
                        }
                        _ => {}
                    }
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
                        (chat.avatar_display_path, chat.avatar_hash) = avatar_cached(path);
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
                if !self.online() || self.sending {
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
            Message::DownloadMedia { chat, id } => {
                self.log(format!("Downloading media for {id}…"));
                let chat2 = chat.clone();
                let id2 = id.clone();
                return Task::perform(
                    async move {
                        let r = request_file(json!({
                            "v": 1,
                            "id": 99,
                            "type": "download_media",
                            "chat": chat2,
                            "msg_id": id2,
                        }))
                        .await;
                        (chat, id, r)
                    },
                    |(chat, id, r)| Message::MediaDownloaded { chat, id, r },
                );
            }
            Message::MediaDownloaded { chat: _, id, r } => match r {
                Ok(v) => {
                    if let Some(path) = v["media_path"].as_str() {
                        self.log(format!("Downloaded media: {path}"));
                        // Update the line in the current view
                        if let Some(line) = self.lines.iter_mut().find(|l| l.id == id) {
                            line.media_path = Some(path.to_string());
                        }
                    } else {
                        let msg = v["message"].as_str().unwrap_or("download failed");
                        self.log(format!("Download failed: {msg}"));
                        self.error = Some(msg.to_string());
                    }
                }
                Err(e) => {
                    self.log(format!("Download failed: {e}"));
                    self.error = Some(e);
                }
            },
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
            Message::GroupsOnly(on) => self.groups_only = on,
            Message::DismissBanner => {
                self.error = None;
                self.notice = None;
            }
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
            Message::CallStart { video } => {
                let Some(open) = self.open.clone() else {
                    return Task::none();
                };
                if open.ends_with("@g.us") {
                    self.error = Some("Group calls are not supported yet (1:1 calls only).".into());
                    return Task::none();
                }
                let to = if open.ends_with("@s.whatsapp.net") {
                    open
                } else {
                    self.chats
                        .iter()
                        .find(|c| c.chat == open)
                        .map(|c| c.chat.clone())
                        .unwrap_or(open)
                };
                self.error = None;
                self.call = Some(CallUi::Dialing {
                    to: to.clone(),
                    video,
                });
                return Task::perform(
                    request_slow(json!({"v":1,"id":20,"type":"call_start","to":to})),
                    Message::CallDone,
                );
            }
            Message::CallAnswer => {
                return Task::perform(
                    request_slow(json!({"v":1,"id":21,"type":"call_answer"})),
                    Message::CallDone,
                );
            }
            Message::CallReject => {
                self.call = None;
                return Task::perform(
                    request(json!({"v":1,"id":22,"type":"call_reject"})),
                    Message::CallDone,
                );
            }
            Message::CallHangup => {
                self.call = None;
                return Task::perform(
                    request(json!({"v":1,"id":23,"type":"call_hangup"})),
                    Message::CallDone,
                );
            }
            Message::CallToggleMute => {
                let next_mute = match &mut self.call {
                    Some(CallUi::Active { muted, .. }) => {
                        *muted = !*muted;
                        *muted
                    }
                    _ => return Task::none(),
                };
                return Task::perform(
                    request(json!({"v":1,"id":24,"type":"call_mute","on":next_mute})),
                    Message::CallDone,
                );
            }
            Message::CallDone(r) => match r {
                Ok(v) => {
                    if v["type"] == "call_failed" {
                        let msg = v["message"].as_str().unwrap_or("Call failed").to_string();
                        self.log(format!("Call failed: {msg}"));
                        self.error = Some(msg);
                        self.call = None;
                    } else if let Some(msg) = v["message"].as_str() {
                        self.log(format!("Call: {msg}"));
                    }
                }
                Err(e) => {
                    self.log(format!("Call request error: {e}"));
                    self.error = Some(e);
                    self.call = None;
                }
            },
        }
        Task::none()
    }

    fn view(&self) -> Element<'_, Message> {
        let kind = self.state["kind"].as_str().unwrap_or("unknown");
        let body: Element<'_, Message> = if let Some(qr) = &self.qr {
            container(
                column![
                    text("Use WhatsApp on your computer")
                        .size(28)
                        .color(c(TEXT)),
                    text("1. Open WhatsApp on your phone\n2. Go to Settings > Linked devices\n3. Tap Link a device\n4. Scan this code")
                        .size(15)
                        .color(c(MUTED)),
                    qr_code(qr)
                ]
                .spacing(20)
                .align_x(Horizontal::Center),
            )
            .center_x(Length::Fill)
            .center_y(Length::Fill)
            .width(Length::Fill)
            .height(Length::Fill)
            .style(flat(PANEL))
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
                .filter(|c| !self.groups_only || c.chat.ends_with("@g.us"))
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
            let open_chat = self.open.as_deref();
            let chats = scrollable(column(visible_chats.map(|ch| {
                let display_name = if is_raw_name(ch) {
                    ch.avatar_hash
                        .and_then(|hash| {
                            self.chats
                                .iter()
                                .find(|other| {
                                    other.avatar_hash == Some(hash) && !is_raw_name(other)
                                })
                                .and_then(|other| other.name.as_deref())
                        })
                        .unwrap_or(ch.name.as_deref().unwrap_or(ch.chat.as_str()))
                } else {
                    ch.name.as_deref().unwrap_or(ch.chat.as_str())
                };
                let shown: String = if self.is_self_chat(&ch.chat) {
                    self.self_label()
                } else if ch.chat == "status@broadcast" {
                    "All Status Updates".to_string()
                } else if ch.chat.starts_with("status@") {
                    if display_name == ch.chat {
                        pretty_id(display_name.trim_start_matches("status@"))
                    } else {
                        display_name.to_string()
                    }
                } else if display_name == ch.chat {
                    pretty_id(display_name)
                } else {
                    display_name.to_string()
                };
                let preview = if ch.chat.starts_with("status@") && ch.last_message.as_deref().unwrap_or("").trim().is_empty() {
                    "Status update".to_string()
                } else {
                    one_line(
                        ch.last_message.as_deref().unwrap_or(if ch.last_ts > 0 {
                            "Conversation"
                        } else {
                            "Contact"
                        }),
                        60,
                    )
                };
                let when = if ch.last_ts > 0 {
                    chat_time(ch.last_ts)
                } else {
                    String::new()
                };
                let is_selected = open_chat == Some(ch.chat.as_str());
                let label = column![
                    row![
                        text(one_line(&shown, 34)).size(17).color(c(TEXT)),
                        iced::widget::Space::new().width(Length::Fill),
                        text(when).size(12).color(c(MUTED)),
                    ]
                    .align_y(Alignment::Center),
                    text(preview).size(14).color(c(MUTED)),
                ]
                .spacing(3)
                .width(Length::Fill);
                let item = row![
                    avatar(ch.avatar_display_path.as_deref(), &shown, 49.0),
                    label
                ]
                .spacing(14)
                .align_y(Alignment::Center);
                column![
                    button(item)
                        .padding([10, 14])
                        .width(Length::Fill)
                        .style(move |_, status| chat_row_style(is_selected, status))
                        .on_press(Message::Open(ch.chat.clone())),
                    row_divider()
                ]
                .into()
            })))
            .width(Length::Fill)
            .height(Length::Fill);

            // ---- conversation ----
            let mut convo = column![].width(Length::Fill).padding([10, 40]);
            if self.more {
                convo = convo.push(
                    container(
                        button(text("Load older messages").size(13))
                            .on_press(Message::Older)
                            .style(subtle_button_style),
                    )
                    .center_x(Length::Fill),
                );
            }
            let is_group_or_status = self
                .open
                .as_deref()
                .is_some_and(|chat| chat.ends_with("@g.us") || chat.starts_with("status@"));
            let mut prev: Option<&Line> = None;
            for l in &self.lines {
                let day = local_date(l.ts);
                let new_day = prev.is_none_or(|p| local_date(p.ts) != day);
                if new_day {
                    convo = convo.push(date_chip(day_label(l.ts)));
                }
                let first_in_run =
                    new_day || prev.is_none_or(|p| p.from_me != l.from_me || p.sender != l.sender);
                prev = Some(l);
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
                let body: Option<String> = match (&l.text, &l.media_kind) {
                    (Some(t), _) => Some(t.clone()),
                    (None, Some(k)) if l.media_path.is_none() => Some(format!("[{k}]")),
                    _ => None,
                };
                let mut content = column![].spacing(2);
                if is_group_or_status && !l.from_me {
                    content = content.push(
                        text(who.to_string())
                            .size(13)
                            .color(sender_color(&l.sender)),
                    );
                }
                if let Some(b) = body {
                    content = content.push(
                        text(b)
                            .size(14)
                            .width(Length::Shrink)
                            .wrapping(iced::widget::text::Wrapping::Word)
                            .color(c(TEXT)),
                    );
                }
                // Images
                if l.media_kind.as_deref() == Some("image") {
                    if let Some(p) = l.media_path.as_ref() {
                        content = content.push(
                            mouse_area(image(image::Handle::from_path(p)).width(Length::Fixed(260.0)))
                                .on_press(Message::OpenFile(p.clone())),
                        );
                    } else if let Some(chat) = &self.open {
                        content = content.push(
                            button(
                                row![
                                    text("⬇").size(16).color(c(ACCENT)),
                                    text("Download Image").size(13).color(c(TEXT)),
                                ]
                                .spacing(6)
                                .align_y(Alignment::Center),
                            )
                            .padding([6, 12])
                            .on_press(Message::DownloadMedia { chat: chat.clone(), id: l.id.clone() })
                            .style(subtle_button_style),
                        );
                    }
                }
                // Videos
                if l.media_kind.as_deref() == Some("video") {
                    content = match &l.media_path {
                        Some(p) => content.push(
                            button(
                                row![
                                    text("▶").size(18).color(c(ACCENT)),
                                    text("Play Video").size(14).color(c(TEXT)),
                                ]
                                .spacing(8)
                                .align_y(Alignment::Center),
                            )
                            .padding([8, 14])
                            .on_press(Message::OpenFile(p.clone()))
                            .style(subtle_button_style),
                        ),
                        None => {
                            if let Some(chat) = &self.open {
                                content.push(
                                    button(
                                        row![
                                            text("⬇").size(16).color(c(ACCENT)),
                                            text("Download Video").size(13).color(c(TEXT)),
                                        ]
                                        .spacing(6)
                                        .align_y(Alignment::Center),
                                    )
                                    .padding([6, 12])
                                    .on_press(Message::DownloadMedia { chat: chat.clone(), id: l.id.clone() })
                                    .style(subtle_button_style),
                                )
                            } else {
                                content.push(
                                    text("Video not available")
                                        .size(11)
                                        .color(c(MUTED)),
                                )
                            }
                        }
                    };
                }
                // Audio
                if l.media_kind.as_deref() == Some("audio") {
                    content = match &l.media_path {
                        Some(p) => content.push(
                            button(
                                row![
                                    text("🔊").size(18).color(c(ACCENT)),
                                    text("Play Audio").size(14).color(c(TEXT)),
                                ]
                                .spacing(8)
                                .align_y(Alignment::Center),
                            )
                            .padding([8, 14])
                            .on_press(Message::OpenFile(p.clone()))
                            .style(subtle_button_style),
                        ),
                        None => {
                            if let Some(chat) = &self.open {
                                content.push(
                                    button(
                                        row![
                                            text("⬇").size(16).color(c(ACCENT)),
                                            text("Download Audio").size(13).color(c(TEXT)),
                                        ]
                                        .spacing(6)
                                        .align_y(Alignment::Center),
                                    )
                                    .padding([6, 12])
                                    .on_press(Message::DownloadMedia { chat: chat.clone(), id: l.id.clone() })
                                    .style(subtle_button_style),
                                )
                            } else {
                                content.push(
                                    text("Audio not available")
                                        .size(11)
                                        .color(c(MUTED)),
                                )
                            }
                        }
                    };
                }
                // Documents
                if l.media_kind.as_deref() == Some("document") {
                    content = match &l.media_path {
                        Some(p) => content.push(
                            button(text("Open file").size(13))
                                .on_press(Message::OpenFile(p.clone()))
                                .style(subtle_button_style),
                        ),
                        None => {
                            if let Some(chat) = &self.open {
                                content.push(
                                    button(
                                        row![
                                            text("⬇").size(16).color(c(ACCENT)),
                                            text("Download File").size(13).color(c(TEXT)),
                                        ]
                                        .spacing(6)
                                        .align_y(Alignment::Center),
                                    )
                                    .padding([6, 12])
                                    .on_press(Message::DownloadMedia { chat: chat.clone(), id: l.id.clone() })
                                    .style(subtle_button_style),
                                )
                            } else {
                                content.push(
                                    text("File not available")
                                        .size(11)
                                        .color(c(MUTED)),
                                )
                            }
                        }
                    };
                }
                let stamp = text(clock(l.ts))
                    .size(11)
                    .color(Color::from_rgba(1.0, 1.0, 1.0, 0.6));
                let item = column![content, stamp]
                    .spacing(1)
                    .align_x(Horizontal::Right);
                // The corner next to the sender is square on the first bubble of a run: WhatsApp's tail.
                let radius = iced::border::Radius {
                    top_left: if !l.from_me && first_in_run { 0.0 } else { 7.5 },
                    top_right: if l.from_me && first_in_run { 0.0 } else { 7.5 },
                    bottom_right: 7.5,
                    bottom_left: 7.5,
                };
                let from_me = l.from_me;
                let bubble = container(item)
                    .padding([6, 9])
                    .max_width(520.0)
                    .style(move |_| BoxStyle {
                        background: Some(c(if from_me { OUT_BUBBLE } else { HEADER }).into()),
                        border: iced::Border {
                            radius,
                            ..Default::default()
                        },
                        ..Default::default()
                    });
                let bubble = mouse_area(bubble).on_press(Message::MessageClick(l.clone()));
                let dropdown = mouse_area(text("⌄").size(17).color(c(MUTED)))
                    .on_press(Message::ToggleMessageMenu(l.id.clone()));
                let mut message_card =
                    column![row![bubble, dropdown].spacing(3).align_y(Alignment::Start)]
                        .spacing(2);
                if self.open_message_menu.as_deref() == Some(l.id.as_str()) {
                    message_card = message_card.push(
                        button("Reply")
                            .on_press(Message::Reply(l.clone()))
                            .style(subtle_button_style),
                    );
                    if let Some(path) = l
                        .media_path
                        .as_ref()
                        .filter(|_| matches!(l.media_kind.as_deref(), Some("document") | Some("video") | Some("audio") | Some("image")))
                    {
                        let action_label = match l.media_kind.as_deref() {
                            Some("video") => "Play video",
                            Some("audio") => "Play audio",
                            _ => "Open file",
                        };
                        message_card = message_card.push(
                            button(action_label)
                                .on_press(Message::OpenFile(path.clone()))
                                .style(subtle_button_style),
                        );
                    }
                }
                let aligned = if l.from_me {
                    row![
                        iced::widget::Space::new().width(Length::Fill),
                        container(message_card)
                    ]
                    .align_y(Alignment::End)
                } else {
                    row![
                        container(message_card),
                        iced::widget::Space::new().width(Length::Fill)
                    ]
                    .align_y(Alignment::Start)
                };
                let gap = if first_in_run { 10.0 } else { 2.0 };
                convo = convo.push(container(aligned).padding(iced::Padding {
                    top: gap,
                    ..iced::Padding::ZERO
                }));
            }

            // ---- right pane ----
            let right: Element<'_, Message> = if let Some(open) = &self.open {
                let chat = self.chats.iter().find(|c| &c.chat == open);
                let saved_name = chat
                    .and_then(|c| c.name.as_deref())
                    .filter(|n| *n != open.as_str());
                let title: String = if self.is_self_chat(open) {
                    self.self_label()
                } else if open == "status@broadcast" {
                    "All Status Updates".to_string()
                } else if open.starts_with("status@") {
                    saved_name.map_or_else(|| pretty_id(open.trim_start_matches("status@")), str::to_string)
                } else {
                    saved_name.map_or_else(|| pretty_id(open), str::to_string)
                };
                let subtitle: Option<String> = if open.ends_with("@g.us") {
                    Some("Group".to_string())
                } else if open.starts_with("status@") {
                    Some("Status".to_string())
                } else if open.ends_with("@newsletter") {
                    Some("Channel".to_string())
                } else if saved_name.is_some() && open.ends_with("@s.whatsapp.net") {
                    Some(pretty_id(open))
                } else {
                    None
                };
                let mut titles = column![text(one_line(&title, 40)).size(16).color(c(TEXT))].spacing(2);
                if let Some(s) = subtitle {
                    titles = titles.push(text(s).size(13).color(c(MUTED)));
                }
                let can_call = self.online()
                    && self.call.is_none()
                    && !open.ends_with("@g.us")
                    && !open.starts_with("status@")
                    && !open.ends_with("@newsletter")
                    && !self.is_self_chat(open);
                let header = container(
                    row![
                        avatar(chat.and_then(|c| c.avatar_display_path.as_deref()), &title, 40.0),
                        titles,
                        iced::widget::Space::new().width(Length::Fill),
                        button(text("🎥").size(18))
                            .on_press_maybe(can_call.then_some(Message::CallStart { video: true }))
                            .style(icon_button_style),
                        button(text("📞").size(18))
                            .on_press_maybe(can_call.then_some(Message::CallStart { video: false }))
                            .style(icon_button_style),
                        button(text("⋮").size(20))
                            .on_press(Message::ToggleChatMenu)
                            .style(icon_button_style),
                    ]
                    .spacing(14)
                    .align_y(Alignment::Center),
                )
                .padding([9, 16])
                .width(Length::Fill)
                .style(flat(HEADER));
                let mut pane = column![header].width(Length::Fill).height(Length::Fill);
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
                    pane = pane.push(
                        container(chat_actions)
                            .padding(8)
                            .width(Length::Fill)
                            .style(flat(HEADER)),
                    );
                }
                // anchor_bottom: the view starts at, and stays pinned to, the newest message. History is
                // loaded newest-first, so without this the chat opened at the top.
                pane = pane.push(
                    container(
                        scrollable(convo)
                            .anchor_bottom()
                            .width(Length::Fill)
                            .height(Length::Fill),
                    )
                    .width(Length::Fill)
                    .height(Length::Fill)
                    .style(flat(APP_BG)),
                );
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
                    pane = pane.push(
                        container(
                            row![
                                column![
                                    text(format!("Replying to {by}"))
                                        .size(12)
                                        .color(c(ACCENT)),
                                    text(one_line(excerpt, 80)).size(13).color(c(MUTED))
                                ],
                                iced::widget::Space::new().width(Length::Fill),
                                button("×")
                                    .on_press(Message::CancelReply)
                                    .style(icon_button_style)
                            ]
                            .spacing(8)
                            .align_y(Alignment::Center),
                        )
                        .padding([8, 16])
                        .width(Length::Fill)
                        .style(flat(HEADER)),
                    );
                }
                if let Some(p) = &self.pending_file {
                    let name = std::path::Path::new(p)
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or(p.as_str());
                    pane = pane.push(
                        container(
                            row![
                                text(format!("Send \"{name}\"?")).color(c(TEXT)),
                                iced::widget::Space::new().width(Length::Fill),
                                button("Send").style(accent_button_style).on_press_maybe(
                                    (self.online() && !self.sending).then_some(Message::ConfirmFile)
                                ),
                                button("Cancel")
                                    .on_press(Message::CancelFile)
                                    .style(subtle_button_style),
                            ]
                            .spacing(8)
                            .align_y(Alignment::Center),
                        )
                        .padding([8, 16])
                        .width(Length::Fill)
                        .style(flat(HEADER)),
                    );
                }
                if open.starts_with("status@") {
                    pane = pane.push(
                        container(
                            text("Status updates are read-only.")
                                .size(13)
                                .color(c(MUTED)),
                        )
                        .padding([14, 16])
                        .width(Length::Fill)
                        .style(flat(HEADER)),
                    );
                } else if open.ends_with("@newsletter") {
                    pane = pane.push(
                        container(
                            text("This channel is read-only.")
                                .size(13)
                                .color(c(MUTED)),
                        )
                        .padding([14, 16])
                        .width(Length::Fill)
                        .style(flat(HEADER)),
                    );
                } else {
                    pane = pane.push(
                        container(
                            row![
                                button(text("+").size(24))
                                    .on_press_maybe(
                                        (self.online() && !self.sending).then_some(Message::Attach)
                                    )
                                    .style(icon_button_style),
                                text_input("Type a message (or drop a file here)", &self.draft)
                                    .on_input(Message::Draft)
                                    .on_submit(Message::Send)
                                    .padding([10, 14])
                                    .style(input_style(SELECTED)),
                                button(text("➤").size(20))
                                    .on_press_maybe(self.can_send().then_some(Message::Send))
                                    .style(send_button_style),
                            ]
                            .spacing(10)
                            .align_y(Alignment::Center),
                        )
                        .padding([10, 16])
                        .width(Length::Fill)
                        .style(flat(HEADER)),
                    );
                }
                pane.into()
            } else {
                container(
                    column![
                        iced::widget::Space::new().height(Length::Fill),
                        text("WhatsApp").size(34).color(c(0xe1e9ed)),
                        text("Send and receive messages from your computer.")
                            .size(14)
                            .color(c(MUTED)),
                        iced::widget::Space::new().height(Length::Fill),
                        text("Your personal messages are end-to-end encrypted")
                            .size(13)
                            .color(c(MUTED)),
                        iced::widget::Space::new().height(Length::Fixed(24.0)),
                        container(iced::widget::Space::new())
                            .width(Length::Fill)
                            .height(Length::Fixed(6.0))
                            .style(flat(ACCENT)),
                    ]
                    .spacing(10)
                    .align_x(Horizontal::Center)
                    .width(Length::Fill)
                    .height(Length::Fill),
                )
                .width(Length::Fill)
                .height(Length::Fill)
                .style(flat(EMPTY_BG))
                .into()
            };

            // ---- left panel ----
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
            let brand = container(
                row![
                    text("WhatsApp").size(22).color(c(TEXT)),
                    iced::widget::Space::new().width(Length::Fill),
                    button(text("✎").size(20))
                        .on_press(Message::NewChat)
                        .style(icon_button_style),
                    button(text("⋮").size(20))
                        .on_press(Message::ToggleSidebarMenu)
                        .style(icon_button_style),
                ]
                .align_y(Alignment::Center),
            )
            .padding([10, 16]);
            let search_box = container(
                text_input("⌕  Search or start a new chat", &self.search)
                    .on_input(Message::Search)
                    .on_submit(Message::NewChat)
                    .padding([8, 14])
                    .style(input_style(HEADER)),
            )
            .padding([6, 12]);
            let groups_only = self.groups_only;
            let chips = container(
                row![
                    button(text("All").size(14))
                        .padding([6, 12])
                        .on_press(Message::GroupsOnly(false))
                        .style(move |_, status| chip_style(!groups_only, status)),
                    button(text("Groups").size(14))
                        .padding([6, 12])
                        .on_press(Message::GroupsOnly(true))
                        .style(move |_, status| chip_style(groups_only, status)),
                ]
                .spacing(8),
            )
            .padding([4, 12]);
            let mut left = column![brand, search_box, chips]
                .width(Length::Fixed(400.0))
                .height(Length::Fill);
            if self.show_sidebar_menu {
                left = left.push(
                    container(
                        column![
                            button("Sync all chats")
                                .on_press(Message::SyncStart(None))
                                .style(subtle_button_style),
                            button("Show log")
                                .on_press(Message::ToggleLog)
                                .style(subtle_button_style),
                        ]
                        .spacing(4),
                    )
                    .padding([4, 12]),
                );
            }
            if valid_phone {
                left = left.push(
                    container(
                        button(text(format!("Start chat with +{digits}")))
                            .width(Length::Fill)
                            .on_press(Message::NewChat)
                            .style(accent_button_style),
                    )
                    .padding([4, 12]),
                );
            }
            left = left.push(chats);

            // ---- rail ----
            let account_initial = account.chars().next().unwrap_or('A').to_string();
            let account_photo: Element<'_, Message> = if let Some(path) = account_avatar {
                image(image::Handle::from_path(path))
                    .width(Length::Fixed(38.0))
                    .height(Length::Fixed(38.0))
                    .into()
            } else {
                avatar(None, &account_initial, 38.0)
            };
            let profile = button(account_photo)
                .padding(0)
                .on_press(Message::ToggleAccountInfo)
                .style(|_, _| BtnStyle {
                    background: None,
                    ..Default::default()
                });
            let rail_btn = |glyph: &'static str, cat: Category| {
                let active = self.category == cat;
                button(container(text(glyph).size(20)).center_x(Length::Fill))
                    .width(Length::Fill)
                    .padding(10)
                    .on_press(Message::Category(cat))
                    .style(move |_, status| rail_style(active, status))
            };
            let rail = column![
                rail_btn("▣", Category::Chats),
                rail_btn("◉", Category::Status),
                rail_btn("◌", Category::Channels),
                iced::widget::Space::new().height(Length::Fill),
                profile,
            ]
            .spacing(10)
            .width(Length::Fixed(52.0))
            .height(Length::Fill);
            let rail: Element<'_, Message> = if self.show_account_info {
                let popover = container(text(account).size(13).color(c(TEXT)))
                    .padding(12)
                    .width(Length::Fixed(190.0))
                    .style(|_| BoxStyle {
                        background: Some(c(SELECTED).into()),
                        border: iced::Border {
                            radius: 10.0.into(),
                            ..Default::default()
                        },
                        shadow: iced::Shadow {
                            color: Color::from_rgba(0.0, 0.0, 0.0, 0.45),
                            offset: iced::Vector::new(0.0, 4.0),
                            blur_radius: 14.0,
                        },
                        snap: Default::default(),
                        text_color: None,
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
                    .padding([12, 6])
                    .height(Length::Fill)
                    .style(flat(PANEL)),
                divider_v(),
                container(left).height(Length::Fill).style(flat(PANEL)),
                divider_v(),
                right,
            ]
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
        // No outer padding or rounded panels: WhatsApp's panes run edge to edge.
        let mut page = column![].width(Length::Fill).height(Length::Fill);
        if let Some(b) = self.banners() {
            page = page.push(b);
        }
        container(page.push(content))
            .width(Length::Fill)
            .height(Length::Fill)
            .style(flat(APP_BG))
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