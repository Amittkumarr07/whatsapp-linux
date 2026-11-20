//! Backfill: keeps asking the phone for older messages until it has nothing more to give.
//! The phone is the only source of old messages and it answers asynchronously, so progress is
//! measured in the DB (did the oldest cached message get older?) instead of by counting library
//! events. One job at a time, one request in flight, pauses between requests: a burst of history
//! requests is exactly what an unofficial client should not send.
use crate::ipc::{query, Ctx};
use crate::protocol::Outgoing;
use crate::storage;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::SeqCst};
use std::time::Duration;
use tokio::sync::oneshot;

const ROUNDS_ONE_CHAT: u32 = 10; // x50 messages asked per round
const ROUNDS_PER_CHAT_IN_ALL: u32 = 3;
const MAX_CHATS_IN_ALL: usize = 100;
const STOP_AFTER_IDLE_CHATS: u32 = 10; // phone offline or nothing older anywhere: stop, don't hammer
const ANSWER_POLLS: u32 = 30; // seconds to wait for the phone's answer to show up in the DB
const PAUSE: Duration = Duration::from_secs(2);

#[derive(Default)]
pub struct SyncState {
    pub running: AtomicBool,
    pub asked: AtomicU32,  // requests that were handed to the phone
    pub rounds: AtomicU32, // requests the phone answered with older messages
    pub done: AtomicU32,   // chats finished
    pub total: AtomicU32,  // chats in this job
}

/// Starts a job in the background. False if one is already running.
pub fn start(ctx: Ctx, chat: Option<String>) -> bool {
    if ctx.sync.running.swap(true, SeqCst) {
        return false;
    }
    for c in [&ctx.sync.asked, &ctx.sync.rounds, &ctx.sync.done, &ctx.sync.total] {
        c.store(0, SeqCst);
    }
    tokio::spawn(async move {
        run(&ctx, chat).await;
        ctx.sync.running.store(false, SeqCst);
    });
    true
}

async fn run(ctx: &Ctx, chat: Option<String>) {
    let chats: Vec<(String, u32)> = match chat {
        Some(c) => vec![(c, ROUNDS_ONE_CHAT)],
        None => match query(ctx, storage::chats).await {
            Some(rows) => rows
                .into_iter()
                .filter(|r| r.last_ts > 0 && !r.chat.starts_with("status@") && !r.chat.ends_with("@newsletter"))
                .take(MAX_CHATS_IN_ALL)
                .map(|r| (r.chat, ROUNDS_PER_CHAT_IN_ALL))
                .collect(),
            None => return,
        },
    };
    ctx.sync.total.store(chats.len() as u32, SeqCst);
    let mut idle = 0;
    for (chat, rounds) in chats {
        if backfill(ctx, &chat, rounds).await {
            idle = 0;
        } else {
            idle += 1;
        }
        ctx.sync.done.fetch_add(1, SeqCst);
        if idle >= STOP_AFTER_IDLE_CHATS {
            tracing::info!("phone returned nothing for several chats in a row; sync stopped");
            return;
        }
        tokio::time::sleep(PAUSE).await;
    }
}

/// True if at least one round brought older messages.
async fn backfill(ctx: &Ctx, chat: &str, rounds: u32) -> bool {
    let mut progressed = false;
    for _ in 0..rounds {
        let c = chat.to_string();
        let Some(Some(a)) = query(ctx, move |conn| storage::oldest(conn, &c)).await else { break };
        // The phone finds the anchor by chat id plus message id. A person can have two ids (phone number
        // and @lid) and only the form the phone itself uses matches, so when one form gets no answer the
        // other forms are tried too.
        let mut forms = vec![a.chat.clone()];
        let c = chat.to_string();
        for id in query(ctx, move |conn| storage::chat_ids(conn, &c)).await.unwrap_or_default() {
            if !forms.contains(&id) {
                forms.push(id);
            }
        }
        let mut moved = false;
        for form in forms {
            let (reply, answer) = oneshot::channel();
            let cmd = Outgoing::Older {
                chat: form,
                oldest_id: a.id.clone(),
                oldest_from_me: a.from_me,
                oldest_ts_ms: a.ts.saturating_mul(1000),
                count: 50,
                reply,
            };
            if ctx.cmds.try_send(cmd).is_err() {
                return progressed;
            }
            if !matches!(tokio::time::timeout(Duration::from_secs(15), answer).await, Ok(Ok(Ok(_)))) {
                tracing::info!("sync: history request could not be sent");
                return progressed; // could not even ask (offline)
            }
            ctx.sync.asked.fetch_add(1, SeqCst);
            tracing::info!("sync: history request sent, waiting for the phone");
            for _ in 0..ANSWER_POLLS {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let c = chat.to_string();
                if let Some(Some(b)) = query(ctx, move |conn| storage::oldest(conn, &c)).await {
                    if b.id != a.id || b.ts != a.ts {
                        moved = true;
                        break;
                    }
                }
            }
            if moved {
                break;
            }
            tracing::info!("sync: no older message arrived for this id form");
        }
        if !moved {
            break; // nothing older arrived under any id: the phone has no more (or is offline)
        }
        progressed = true;
        ctx.sync.rounds.fetch_add(1, SeqCst);
        tokio::time::sleep(PAUSE).await;
    }
    progressed
}
