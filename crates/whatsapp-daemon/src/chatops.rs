//! Small chat controls on top of the connected client: typing indicators, edit, reactions,
//! archive / pin / mute, block. Each is a thin wrapper; the IPC layer validates the inputs.
//!
//! Signatures marked `API-CHECK` come from the 0.7 source listings but were never compiled.
use waproto::whatsapp as wa;
use whatsapp_rust::Jid;
use whatsapp_rust::client::Client;
use whatsapp_rust::ChatStateType;

type R = Result<String, String>;

fn jid(s: &str) -> Result<Jid, String> {
    s.parse::<Jid>().map_err(|_| "not a valid chat id".to_string())
}

/// state: "composing" | "recording" | "paused"
pub async fn typing(client: &Client, chat: &str, state: &str) -> R {
    let st = match state {
        "composing" => ChatStateType::Composing,
        "recording" => ChatStateType::Recording,
        "paused" => ChatStateType::Paused,
        _ => return Err("state must be composing, recording or paused".into()),
    };
    client.chatstate().send(&jid(chat)?, st).await.map_err(|e| e.to_string())?;
    Ok(format!("{state} sent"))
}

/// Edit a text message you sent (WhatsApp only allows this for a limited time after sending).
pub async fn edit(client: &Client, chat: &str, id: &str, text: &str) -> R {
    let msg = wa::Message { conversation: Some(text.to_string()), ..Default::default() };
    client.edit_message(jid(chat)?, id.to_string(), msg).await.map_err(|e| e.to_string())?;
    Ok("edited".into())
}

/// React to a message. `participant` is the sender's id and is only needed in groups.
pub async fn react(client: &Client, chat: &str, id: &str, emoji: &str, from_me: bool, participant: Option<String>) -> R {
    let key = wa::MessageKey {
        remote_jid: Some(chat.to_string()),
        from_me: Some(from_me),
        id: Some(id.to_string()),
        participant,
        ..Default::default()
    };
    client.send_reaction(jid(chat)?, key, emoji).await.map_err(|e| e.to_string())?;
    Ok("reaction sent".into())
}

/// action: archive | unarchive | pin | unpin | mute | unmute  (syncs to your phone via app state)
pub async fn chat_action(client: &Client, chat: &str, action: &str) -> R {
    let j = jid(chat)?;
    let actions = client.chat_actions();
    // Signatures confirmed in the 0.7 docs: archive/unarchive take an optional message range.
    let res = match action {
        "archive" => actions.archive_chat(&j, None).await,
        "unarchive" => actions.unarchive_chat(&j, None).await,
        "pin" => actions.pin_chat(&j).await,
        "unpin" => actions.unpin_chat(&j).await,
        "mute" => actions.mute_chat(&j).await,
        "unmute" => actions.unmute_chat(&j).await,
        _ => return Err("unknown chat action".into()),
    };
    res.map_err(|e| e.to_string())?;
    Ok(format!("{action} done"))
}

pub async fn block(client: &Client, chat: &str, on: bool) -> R {
    let j = jid(chat)?;
    // API-CHECK: Blocking::block / unblock take &Jid
    let res = if on { client.blocking().block(&j).await } else { client.blocking().unblock(&j).await };
    res.map_err(|e| e.to_string())?;
    Ok(if on { "blocked".into() } else { "unblocked".into() })
}

/// Star / unstar a message. `participant` is the sender's id, needed only for others' messages in groups.
pub async fn star(client: &Client, chat: &str, id: &str, from_me: bool, participant: Option<String>, on: bool) -> R {
    let j = jid(chat)?;
    let p = match participant {
        Some(p) => Some(jid(&p)?),
        None => None,
    };
    let actions = client.chat_actions();
    let res = if on {
        actions.star_message(&j, p.as_ref(), id, from_me).await
    } else {
        actions.unstar_message(&j, p.as_ref(), id, from_me).await
    };
    res.map_err(|e| e.to_string())?;
    Ok(if on { "starred".into() } else { "unstarred".into() })
}

/// Mark a chat read or unread; syncs to your phone and other linked devices.
pub async fn mark_read(client: &Client, chat: &str, read: bool) -> R {
    client.chat_actions().mark_chat_as_read(&jid(chat)?, read, None).await.map_err(|e| e.to_string())?;
    Ok(if read { "marked read".into() } else { "marked unread".into() })
}
