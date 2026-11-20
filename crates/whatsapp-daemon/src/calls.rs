//! Audio calls inside the chat daemon.
//!
//! `CallHub` is shared between the protocol task (which feeds it the client and incoming-call
//! events) and the IPC server (which turns socket requests into calls and pushes call events to
//! subscribed clients). Neither side needs to know about the other.
use crate::audio::Audio;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;
use whatsapp_rust::Jid;
use whatsapp_rust::client::Client;
use whatsapp_rust::voip::CallHandle;
use whatsapp_rust::wacore::types::call::{CallAction, IncomingCall};

/// A panic on one thread (an audio callback, say) must not turn every later call into a panic too.
fn locked<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// Pushed to every subscribed UI client as `{"type":"call","call":{"state":...}}`.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CallEvent {
    Ringing { from: String, video: bool },
    Dialing { to: String },
    Active { peer: String },
    Ended,
}

struct Active {
    peer: String,
    handle: Arc<CallHandle>,
    _audio: Audio, // dropping this releases the microphone and speaker
}

pub struct CallHub {
    client: Mutex<Option<Arc<Client>>>,
    ringing: Mutex<Option<(String, Arc<IncomingCall>)>>, // (caller, offer)
    active: Mutex<Option<Active>>,
    pub events: broadcast::Sender<CallEvent>,
}

impl Default for CallHub {
    fn default() -> Self {
        Self::new()
    }
}

impl CallHub {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(16);
        Self { client: Mutex::new(None), ringing: Mutex::new(None), active: Mutex::new(None), events }
    }

    /// Called by the protocol task once the connection is up.
    pub fn set_client(&self, client: Arc<Client>) {
        *locked(&self.client) = Some(client);
    }

    pub fn client(&self) -> Result<Arc<Client>, String> {
        locked(&self.client).clone().ok_or_else(|| "not connected to WhatsApp yet".to_string())
    }

    fn busy(&self) -> bool {
        locked(&self.active).is_some()
    }

    fn emit(&self, e: CallEvent) {
        let _ = self.events.send(e); // Err = nobody subscribed, fine
    }

    pub fn status(&self) -> String {
        if let Some(a) = locked(&self.active).as_ref() {
            return format!("in call with {}", a.peer);
        }
        if let Some((who, _)) = locked(&self.ringing).as_ref() {
            return format!("ringing: {who}");
        }
        "idle".into()
    }

    /// "+91 98765-43210" -> "919876543210@s.whatsapp.net"; a full JID passes through.
    fn to_jid(input: &str) -> Result<Jid, String> {
        let s = input.trim();
        let full = if s.contains('@') {
            s.to_string()
        } else {
            let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
            if digits.len() < 7 {
                return Err("give the number with country code, e.g. 919876543210".into());
            }
            format!("{digits}@s.whatsapp.net")
        };
        full.parse::<Jid>().map_err(|_| "not a valid WhatsApp id".to_string())
    }

    pub async fn place(self: &Arc<Self>, to: &str) -> Result<String, String> {
        if self.busy() {
            return Err("already in a call".into());
        }
        let client = self.client()?;
        let jid = Self::to_jid(to)?;
        let audio = Audio::start()?;
        self.emit(CallEvent::Dialing { to: to.trim().to_string() });
        let started = client
            .voip()
            .call(&jid)
            .audio(audio.mic.clone(), audio.speaker.clone())
            .start()
            .await;
        match started {
            Ok(handle) => {
                self.install(to.trim().to_string(), handle, audio);
                Ok(format!("calling {}", to.trim()))
            }
            Err(e) => {
                self.emit(CallEvent::Ended);
                Err(format!("could not start the call: {e}"))
            }
        }
    }

    pub async fn answer(self: &Arc<Self>) -> Result<String, String> {
        if self.busy() {
            return Err("already in a call".into());
        }
        let (who, offer) = locked(&self.ringing).take().ok_or("no incoming call")?;
        let client = self.client()?;
        let audio = Audio::start()?; // a video offer is answered audio-only
        let started = client
            .voip()
            .accept(&offer)
            .audio(audio.mic.clone(), audio.speaker.clone())
            .start()
            .await;
        match started {
            Ok(handle) => {
                self.install(who.clone(), handle, audio);
                Ok(format!("answered {who}"))
            }
            Err(e) => {
                self.emit(CallEvent::Ended);
                Err(format!("could not answer (the caller may have hung up): {e}"))
            }
        }
    }

    pub async fn reject(self: &Arc<Self>) -> Result<String, String> {
        let (who, offer) = locked(&self.ringing).take().ok_or("no incoming call")?;
        let client = self.client()?;
        let res = client.voip().reject(&offer).await;
        self.emit(CallEvent::Ended);
        res.map_err(|e| format!("reject failed: {e}"))?;
        Ok(format!("rejected {who}"))
    }

    pub async fn hangup(self: &Arc<Self>) -> Result<String, String> {
        let handle = locked(&self.active).as_ref().map(|a| a.handle.clone()).ok_or("not in a call")?;
        handle.hangup().await;
        self.end_if_current(&handle);
        Ok("call ended".into())
    }

    pub fn mute(&self, on: bool) -> Result<String, String> {
        let guard = locked(&self.active);
        let a = guard.as_ref().ok_or("not in a call")?;
        a.handle.set_muted(on);
        Ok(if on { "muted".into() } else { "unmuted".into() })
    }

    /// Clears the live call (and sends `Ended`) only if `handle` is still the current one, so a
    /// hangup and the end-of-call watcher cannot both report it.
    fn end_if_current(&self, handle: &Arc<CallHandle>) {
        let mut active = locked(&self.active);
        if active.as_ref().is_some_and(|a| Arc::ptr_eq(&a.handle, handle)) {
            *active = None;
            drop(active);
            self.emit(CallEvent::Ended);
        }
    }

    fn install(self: &Arc<Self>, peer: String, handle: CallHandle, audio: Audio) {
        let handle = Arc::new(handle);
        *locked(&self.active) = Some(Active { peer: peer.clone(), handle: handle.clone(), _audio: audio });
        self.emit(CallEvent::Active { peer });
        let hub = self.clone();
        tokio::spawn(async move {
            handle.wait_ended().await; // peer hung up, or the network dropped
            hub.end_if_current(&handle);
        });
    }

    /// Called by the protocol task for every `Event::IncomingCall`.
    pub async fn on_incoming(self: &Arc<Self>, client: Arc<Client>, call: IncomingCall) {
        let CallAction::Offer { is_video, .. } = &call.action else { return };
        let is_video = *is_video;
        let who = call.from.to_string();

        if call.group.is_some() {
            tracing::info!("group call rejected (not supported yet)");
            let _ = client.voip().reject(&call).await;
            return;
        }
        if self.busy() || locked(&self.ringing).is_some() {
            tracing::info!("busy; rejecting an incoming call");
            let _ = client.voip().reject(&call).await;
            return;
        }

        tracing::info!(video = is_video, "incoming call");
        *locked(&self.ringing) = Some((who.clone(), Arc::new(call)));
        self.emit(CallEvent::Ringing { from: who.clone(), video: is_video });

        let body = format!(
            "{who}{}\nAnswer: whatsapp-daemon ctl answer",
            if is_video { " (video: will be answered as audio)" } else { "" }
        );
        std::thread::spawn(move || {
            let _ = notify_rust::Notification::new()
                .appname("WhatsApp")
                .summary("Incoming WhatsApp call")
                .body(&body)
                .timeout(notify_rust::Timeout::Milliseconds(30_000))
                .show();
        });

        // A caller who gives up sends us nothing we act on; stop offering the call after a while.
        let hub = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(45)).await;
            let mut ringing = locked(&hub.ringing);
            if ringing.as_ref().is_some_and(|(w, _)| *w == who) {
                *ringing = None;
                drop(ringing);
                hub.emit(CallEvent::Ended);
            }
        });
    }
}

/// `whatsapp-daemon ctl call 919876543210 | answer | reject | hangup | mute | unmute | status`
/// Talks to the running daemon over its normal socket, like the UI does.
pub fn ctl(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use std::io::{BufRead, BufReader, Write};
    let req = match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        ["call", to] => serde_json::json!({"v":1,"id":1,"type":"call_start","to":to}),
        ["answer"] => serde_json::json!({"v":1,"id":1,"type":"call_answer"}),
        ["reject"] => serde_json::json!({"v":1,"id":1,"type":"call_reject"}),
        ["hangup"] => serde_json::json!({"v":1,"id":1,"type":"call_hangup"}),
        ["mute"] => serde_json::json!({"v":1,"id":1,"type":"call_mute","on":true}),
        ["unmute"] => serde_json::json!({"v":1,"id":1,"type":"call_mute","on":false}),
        ["status"] => serde_json::json!({"v":1,"id":1,"type":"call_status"}),
        ["typing", chat, state] => serde_json::json!({"v":1,"id":1,"type":"typing","chat":chat,"state":state}),
        ["edit", chat, id, text @ ..] if !text.is_empty() => {
            serde_json::json!({"v":1,"id":1,"type":"edit","chat":chat,"id":id,"text":text.join(" ")})
        }
        ["react", chat, id, emoji] => {
            serde_json::json!({"v":1,"id":1,"type":"react","chat":chat,"id":id,"emoji":emoji,"from_me":false})
        }
        ["react", chat, id, emoji, "mine"] => {
            serde_json::json!({"v":1,"id":1,"type":"react","chat":chat,"id":id,"emoji":emoji,"from_me":true})
        }
        [act @ ("archive" | "unarchive" | "pin" | "unpin" | "mute" | "unmute"), chat] => {
            serde_json::json!({"v":1,"id":1,"type":"chat_action","chat":chat,"action":act})
        }
        ["star", chat, id] => serde_json::json!({"v":1,"id":1,"type":"star","chat":chat,"id":id,"from_me":false,"on":true}),
        ["star", chat, id, "mine"] => serde_json::json!({"v":1,"id":1,"type":"star","chat":chat,"id":id,"from_me":true,"on":true}),
        ["unstar", chat, id] => serde_json::json!({"v":1,"id":1,"type":"star","chat":chat,"id":id,"from_me":false,"on":false}),
        ["unstar", chat, id, "mine"] => serde_json::json!({"v":1,"id":1,"type":"star","chat":chat,"id":id,"from_me":true,"on":false}),
        ["read", chat] => serde_json::json!({"v":1,"id":1,"type":"mark_read","chat":chat,"read":true}),
        ["unread", chat] => serde_json::json!({"v":1,"id":1,"type":"mark_read","chat":chat,"read":false}),
        ["block", chat] => serde_json::json!({"v":1,"id":1,"type":"block","chat":chat,"on":true}),
        ["unblock", chat] => serde_json::json!({"v":1,"id":1,"type":"block","chat":chat,"on":false}),
        _ => {
            return Err("usage: whatsapp-daemon ctl\n  call <number> | answer | reject | hangup | mute | unmute | status\n  typing <chat> composing|recording|paused\n  edit <chat> <message-id> <new text>\n  react <chat> <message-id> <emoji> [mine]\n  archive|unarchive|pin|unpin|mute|unmute <chat>\n  star|unstar <chat> <message-id> [mine]\n  read|unread <chat>\n  block|unblock <chat>".into());
        }
    };
    let runtime = std::env::var("XDG_RUNTIME_DIR").map_err(|_| "XDG_RUNTIME_DIR not set")?;
    let sock = std::path::PathBuf::from(runtime).join("whatsapp/daemon.sock");
    let mut stream = std::os::unix::net::UnixStream::connect(&sock)
        .map_err(|e| format!("cannot reach whatsapp-daemon (is it running?): {e}"))?;
    writeln!(stream, "{req}")?;
    let mut line = String::new();
    BufReader::new(&stream).read_line(&mut line)?;
    let reply: serde_json::Value = serde_json::from_str(&line)?;
    match reply["type"].as_str() {
        Some("call_ok") | Some("op_ok") => {
            println!("{}", reply["message"].as_str().unwrap_or(""));
            Ok(())
        }
        Some("call_failed") | Some("op_failed") => Err(reply["message"].as_str().unwrap_or("call failed").into()),
        _ => Err(format!("unexpected reply: {}", line.trim()).into()),
    }
}
