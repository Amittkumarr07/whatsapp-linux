//! Desktop notifications in the style of WhatsApp's own: who it is from, then what they said.
//! Best effort: a missing D-Bus session must never hurt the daemon.
use crate::protocol::Incoming;
use crate::storage::{self, Card};
use diesel::SqliteConnection;
use std::sync::atomic::{AtomicBool, Ordering};

static WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy)]
pub struct Prefs {
    /// Show the message text. Off: "New message" only (lock screens, screen sharing).
    pub preview: bool,
    /// Notify for channel (newsletter) posts.
    pub channels: bool,
}

/// What a notification says. Built without touching D-Bus so it can be tested.
pub struct Note {
    pub title: String,
    pub body: String,
    pub icon: Option<String>,
}

/// A person or chat as a human would name it. Falls back to the phone number or a plain word,
/// never to an internal id (a name equal to the id means "no name known").
fn label(jid: &str, name: Option<&str>) -> String {
    if let Some(n) = name.map(str::trim).filter(|n| !n.is_empty() && !n.contains('@')) {
        return n.to_string();
    }
    match jid.split_once('@') {
        Some((user, "s.whatsapp.net")) => format!("+{}", user.split(':').next().unwrap_or(user)),
        Some((_, "g.us")) => "Group".to_string(),
        Some((_, "newsletter")) => "Channel".to_string(),
        _ => "Unknown contact".to_string(),
    }
}

fn media(kind: &str) -> (&'static str, &'static str) {
    match kind {
        "image" => ("📷", "Photo"),
        "video" => ("🎥", "Video"),
        "audio" => ("🎤", "Audio"),
        "document" => ("📄", "Document"),
        "sticker" => ("🙂", "Sticker"),
        _ => ("📎", "Attachment"),
    }
}

fn shorten(s: &str, max: usize) -> String {
    let mut chars = s.chars();
    let head: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() { format!("{head}…") } else { head }
}

/// Notification bodies may be rendered as markup by the server; message text is untrusted.
fn escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// None = this message should not notify (own messages, Status posts, silenced channels).
pub fn compose(m: &Incoming, chat: Option<Card>, sender: Option<Card>, prefs: Prefs) -> Option<Note> {
    if m.from_me || m.chat.starts_with("status@") || (m.chat.ends_with("@newsletter") && !prefs.channels) {
        return None;
    }
    let text = m.text.as_deref().map(str::trim).filter(|t| !t.is_empty());
    let what = if !prefs.preview {
        "New message".to_string()
    } else {
        match (text, m.media_kind.as_deref()) {
            (Some(t), Some(k)) => format!("{} {t}", media(k).0),
            (Some(t), None) => t.to_string(),
            (None, Some(k)) => format!("{} {}", media(k).0, media(k).1),
            (None, None) => "New message".to_string(),
        }
    };
    let what = shorten(&what.replace('\n', " "), 200);
    let body = if m.chat.ends_with("@g.us") {
        format!("{}: {what}", label(&m.sender, sender.as_ref().map(|c| c.name.as_str())))
    } else {
        what
    };
    let title = label(&m.chat, chat.as_ref().map(|c| c.name.as_str()));
    let icon = chat.and_then(|c| c.avatar_path).filter(|p| std::path::Path::new(p).exists());
    Some(Note { title, body, icon })
}

pub fn new_message(conn: Option<&mut SqliteConnection>, m: &Incoming, prefs: Prefs) {
    let (chat, sender) = match conn {
        Some(c) => {
            let chat = storage::card(c, &m.chat).ok().flatten();
            let sender = if m.chat.ends_with("@g.us") { storage::card(c, &m.sender).ok().flatten() } else { None };
            (chat, sender)
        }
        None => (None, None),
    };
    let Some(n) = compose(m, chat, sender, prefs) else { return };
    let mut note = notify_rust::Notification::new();
    note.appname("WhatsApp").summary(&n.title).body(&escape(&n.body));
    if let Some(icon) = &n.icon {
        note.icon(icon);
    }
    if let Err(e) = note.show() {
        if !WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(error = %e, "desktop notifications unavailable");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ON: Prefs = Prefs { preview: true, channels: true };

    fn msg(chat: &str, sender: &str, text: Option<&str>, kind: Option<&str>) -> Incoming {
        Incoming {
            id: "1".into(), chat: chat.into(), sender: sender.into(), from_me: false, ts: 1,
            text: text.map(Into::into), media_kind: kind.map(Into::into), media_path: None, historical: false,
            media_params: None,
        }
    }

    fn card(name: &str) -> Option<Card> {
        Some(Card { name: name.into(), avatar_path: None })
    }

    #[test]
    fn one_to_one_shows_the_name_then_the_text() {
        let n = compose(&msg("p@s.whatsapp.net", "p@s.whatsapp.net", Some("see you at 5"), None), card("Pat"), None, ON).unwrap();
        assert_eq!((n.title.as_str(), n.body.as_str()), ("Pat", "see you at 5"));
    }

    #[test]
    fn group_shows_the_group_then_who_said_it() {
        let n = compose(&msg("g@g.us", "s@lid", Some("hi"), None), card("Family"), card("Sam"), ON).unwrap();
        assert_eq!((n.title.as_str(), n.body.as_str()), ("Family", "Sam: hi"));
    }

    #[test]
    fn never_falls_back_to_an_internal_id() {
        let t = |chat: &str, c: Option<Card>| compose(&msg(chat, chat, Some("x"), None), c, None, ON).unwrap().title;
        assert_eq!(t("919296180294@s.whatsapp.net", None), "+919296180294");
        assert_eq!(t("9192:12@s.whatsapp.net", None), "+9192"); // device suffix dropped
        assert_eq!(t("180985828237353@lid", None), "Unknown contact");
        assert_eq!(t("120363413783072438@g.us", None), "Group");
        assert_eq!(t("120363413783072438@newsletter", None), "Channel");
        // a contact stored with its own jid as the "name" counts as nameless
        assert_eq!(t("919@s.whatsapp.net", card("919@s.whatsapp.net")), "+919");
    }

    #[test]
    fn media_and_captions() {
        let b = |text, kind| compose(&msg("p@lid", "p@lid", text, kind), card("Pat"), None, ON).unwrap().body;
        assert_eq!(b(None, Some("image")), "📷 Photo");
        assert_eq!(b(Some("look at this"), Some("image")), "📷 look at this");
        assert_eq!(b(None, Some("audio")), "🎤 Audio");
        assert_eq!(b(None, None), "New message");
    }

    #[test]
    fn preview_off_hides_the_text_but_keeps_the_name() {
        let off = Prefs { preview: false, channels: true };
        let n = compose(&msg("p@lid", "p@lid", Some("secret"), None), card("Pat"), None, off).unwrap();
        assert_eq!((n.title.as_str(), n.body.as_str()), ("Pat", "New message"));
    }

    #[test]
    fn some_messages_never_notify() {
        let mut own = msg("p@lid", "me", Some("x"), None);
        own.from_me = true;
        assert!(compose(&own, None, None, ON).is_none());
        assert!(compose(&msg("status@broadcast", "p@lid", Some("x"), None), None, None, ON).is_none());
        let quiet = Prefs { preview: true, channels: false };
        assert!(compose(&msg("c@newsletter", "c@newsletter", Some("x"), None), None, None, quiet).is_none());
        assert!(compose(&msg("c@newsletter", "c@newsletter", Some("x"), None), None, None, ON).is_some());
    }

    #[test]
    fn long_and_multiline_text_is_tidied() {
        let n = compose(&msg("p@lid", "p@lid", Some("a".repeat(300).as_str()), None), card("Pat"), None, ON).unwrap();
        assert_eq!(n.body.chars().count(), 201); // 200 + the ellipsis
        let n = compose(&msg("p@lid", "p@lid", Some("one\ntwo"), None), card("Pat"), None, ON).unwrap();
        assert_eq!(n.body, "one two");
    }

    #[test]
    fn missing_avatar_file_means_no_icon_and_markup_is_escaped() {
        let c = Some(Card { name: "Pat".into(), avatar_path: Some("/definitely/not/here.jpg".into()) });
        assert!(compose(&msg("p@lid", "p@lid", Some("x"), None), c, None, ON).unwrap().icon.is_none());
        assert_eq!(escape("<b>a & b</b>"), "&lt;b&gt;a &amp; b&lt;/b&gt;");
    }
}
