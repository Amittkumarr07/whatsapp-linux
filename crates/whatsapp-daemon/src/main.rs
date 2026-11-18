mod ipc;
mod notify;
mod protocol;
mod storage;
mod sync;

use protocol::{Contact, Incoming, Outgoing, Protocol, Res, State, WhatsApp};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::{broadcast, mpsc, watch};

/// $WA_DATA, else $XDG_DATA_HOME/whatsapp-linux-client, else ~/.local/share/whatsapp-linux-client.
fn data_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(p) = std::env::var_os("WA_DATA") {
        return Ok(p.into());
    }
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))
        .ok_or("neither XDG_DATA_HOME nor HOME is set")?;
    Ok(base.join("whatsapp-linux-client"))
}

fn private_dir(p: &Path) -> std::io::Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(p)?;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o700))
}

/// Creates the file 0600 *before* SQLite does; its -wal/-shm files inherit the mode.
fn private_file(p: &Path) -> std::io::Result<()> {
    std::fs::OpenOptions::new().create(true).append(true).mode(0o600).open(p)?;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600))
}

/// The session was revoked and its files are gone. Start over in this same process slot (exec), so a
/// fresh pairing QR appears without anyone re-running the daemon (cargo run and systemd alike).
/// Bounded: a loop of revocations must not hammer WhatsApp with connection attempts.
async fn restart_to_pair(sock: &Path) -> Res {
    use std::os::unix::process::CommandExt;
    let n: u32 = std::env::var("WA_REPAIR_COUNT").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    if n >= 3 {
        return Err("session revoked repeatedly; start the daemon again by hand".into());
    }
    let _ = std::fs::remove_file(sock);
    tracing::warn!("session revoked; restarting to show a new pairing QR");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let err = std::process::Command::new(std::env::current_exe()?)
        .args(std::env::args_os().skip(1))
        .env("WA_REPAIR_COUNT", (n + 1).to_string())
        .exec();
    Err(err.into())
}

#[tokio::main]
async fn main() -> Res {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let data = data_dir()?;
    let media_dir = data.join("media");
    let (wa_db, cache_db) = (data.join("whatsapp.db"), data.join("cache.db"));
    private_dir(&data)?;
    private_dir(&media_dir)?;
    private_file(&wa_db)?;
    private_file(&cache_db)?;
    // A crash mid-download leaves a .part file behind; the atomic rename means it is never a real one.
    for e in std::fs::read_dir(&media_dir)?.flatten() {
        if e.path().extension().is_some_and(|x| x == "part") {
            let _ = std::fs::remove_file(e.path());
        }
    }

    let (tx, mut rx) = watch::channel(State::Starting);
    let state_rx = rx.clone();
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let s = rx.borrow().clone();
            tracing::info!(state = ?s, "health");
        }
    });

    // Bind first: fails fast if another daemon is already running.
    let runtime = std::env::var("XDG_RUNTIME_DIR").map_err(|_| "XDG_RUNTIME_DIR not set")?;
    let sock = PathBuf::from(runtime).join("whatsapp/daemon.sock");
    let listener = ipc::bind(&sock).await?;

    // Persist, then broadcast. One writer thread (diesel is blocking), bounded queues everywhere.
    let cache = cache_db.to_str().ok_or("non-UTF-8 data path")?;
    let mut writer_conn = storage::open(cache).map_err(|e| e.to_string())?;
    let reader_conn = Arc::new(Mutex::new(storage::open(cache).map_err(|e| e.to_string())?));
    let (bus, _) = broadcast::channel::<Incoming>(256);
    let (contact_bus, _) = broadcast::channel::<()>(32);
    let (mtx, mut mrx) = mpsc::channel::<Incoming>(256);
    let (ctx_tx, ctx_rx) = mpsc::channel::<Outgoing>(16);

    // Notifications run on their own thread (a slow D-Bus must not stall persistence) with their own
    // read connection to look up names. Like WhatsApp's, they show who wrote and what; set
    // WA_NOTIFY_PREVIEW=0 to hide the text, WA_NOTIFY_CHANNELS=0 to silence channel posts.
    let prefs = notify::Prefs {
        preview: std::env::var("WA_NOTIFY_PREVIEW").map_or(true, |v| v != "0"),
        channels: std::env::var("WA_NOTIFY_CHANNELS").map_or(true, |v| v != "0"),
    };
    let notify_db = cache.to_string();
    let (ntx, nrx) = std::sync::mpsc::sync_channel::<Incoming>(32);
    std::thread::spawn(move || {
        let mut conn = storage::open(&notify_db).ok(); // without it, no names, but still notifies
        for m in nrx {
            notify::new_message(conn.as_mut(), &m, prefs);
        }
    });

    let writer_bus = bus.clone();
    let history_refresh = contact_bus.clone(); // one "data changed" ping per imported batch, not one event per message
    std::thread::spawn(move || {
        let mut batch: Vec<Incoming> = Vec::new();
        while let Some(first) = mrx.blocking_recv() {
            batch.push(first);
            while batch.len() < 500 {
                match mrx.try_recv() {
                    Ok(m) => batch.push(m),
                    Err(_) => break,
                }
            }
            // One transaction per batch (history sync is thousands of rows). If it fails, retry row by
            // row so a single bad row cannot take the whole batch with it.
            let fresh = storage::insert_batch(&mut writer_conn, &batch).unwrap_or_else(|e| {
                tracing::error!(error = %e, "batch persist failed; retrying one by one"); // never log the message
                batch
                    .iter()
                    .map(|m| {
                        storage::insert(&mut writer_conn, m).unwrap_or_else(|e| {
                            tracing::error!(error = %e, "persist failed");
                            false
                        })
                    })
                    .collect()
            });
            let mut imported = false;
            for (m, is_new) in batch.drain(..).zip(fresh) {
                if !is_new {
                    continue; // duplicate: already stored and already broadcast
                }
                if m.historical {
                    imported = true; // old messages: no notification, no per-message live event
                    continue;
                }
                let _ = ntx.try_send(m.clone()); // full queue: drop the notification, not the message
                let _ = writer_bus.send(m); // Err = no subscribers, fine
            }
            if imported {
                let _ = history_refresh.send(());
            }
        }
    });

    // Address-book names arrive in a burst right after pairing; their own thread and connection.
    let (ktx, mut krx) = mpsc::channel::<Contact>(256);
    let mut contacts_conn = storage::open(cache).map_err(|e| e.to_string())?;
    let cached_contacts = storage::contacts(&mut contacts_conn).map_err(|e| e.to_string())?
        .into_iter().filter(|c| !c.jid.ends_with("@lid"))
        .map(|c| Contact { jid: c.jid, name: c.name, pn: c.pn, avatar_path: c.avatar_path })
        .collect();
    let contact_updates = contact_bus.clone();
    std::thread::spawn(move || {
        while let Some(k) = krx.blocking_recv() {
            if let Err(e) = storage::upsert_contact(&mut contacts_conn, &k.jid, &k.name, k.pn.as_deref(), k.avatar_path.as_deref()) {
                tracing::error!(error = %e, "contact persist failed");
            } else {
                let _ = contact_updates.send(());
            }
        }
    });

    tokio::spawn(ipc::serve(listener, ipc::Ctx { state: state_rx, bus, db: reader_conn, cmds: ctx_tx, contact_bus, sync: Default::default() }));

    let mut term = signal(SignalKind::terminate())?; // systemd stop

    tokio::select! {
        r = WhatsApp { db: wa_db, media_dir, cached_contacts }.run(tx, mtx, ctx_rx, ktx) => {
            if let Err(e) = r {
                if e.to_string() == protocol::REVOKED {
                    restart_to_pair(&sock).await?; // only returns if the restart itself failed
                }
                return Err(e);
            }
        }
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT, shutting down"),
        _ = term.recv() => tracing::info!("SIGTERM, shutting down"),
    }
    let _ = std::fs::remove_file(&sock);
    Ok(())
}
