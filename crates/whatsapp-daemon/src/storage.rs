//! Local message cache. Its own DB file, separate from the library's session store.
//! Uses diesel because whatsapp-rust-sqlite-storage already pulls it in; a second
//! SQLite binding (e.g. sqlx) would clash on libsqlite3-sys.
use crate::protocol::Incoming;
use diesel::{
    connection::SimpleConnection,
    prelude::*,
    sql_query,
    sql_types::{BigInt, Bool, Nullable, Text},
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub fn open(path: &str) -> Result<SqliteConnection, Box<dyn std::error::Error + Send + Sync>> {
    let mut c = SqliteConnection::establish(path)?;
    c.batch_execute(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;",
    )?;
    c.run_pending_migrations(MIGRATIONS)?;
    c.batch_execute(
        "CREATE TABLE IF NOT EXISTS media_downloads (chat TEXT NOT NULL, id TEXT NOT NULL, params TEXT NOT NULL, PRIMARY KEY (chat, id));",
    )?;
    Ok(c)
}

/// Idempotent: (chat, id) is the primary key. Returns true if the message was new.
pub fn insert(c: &mut SqliteConnection, m: &Incoming) -> QueryResult<bool> {
    insert_batch(c, std::slice::from_ref(m)).map(|fresh| fresh[0])
}

/// Many messages in ONE transaction (history sync can be tens of thousands). Per message: was it new?
/// A live message brings back a chat the user deleted; a historical one never does and is skipped
/// while its chat is deleted, so importing old history cannot resurrect it. Both checks follow the
/// @lid -> phone-number link, because the UI deletes by the phone-number id.
pub fn insert_batch(c: &mut SqliteConnection, batch: &[Incoming]) -> QueryResult<Vec<bool>> {
    c.transaction(|c| {
        let mut fresh = Vec::with_capacity(batch.len());
        for m in batch {
            if !m.historical {
                sql_query("DELETE FROM deleted_chats WHERE chat = ? OR chat = (SELECT pn FROM contacts WHERE jid = ?)")
                    .bind::<Text, _>(&m.chat)
                    .bind::<Text, _>(&m.chat)
                    .execute(c)?;
            }
            let n = sql_query(
                "INSERT OR IGNORE INTO messages(chat,id,sender,from_me,ts,text,media_kind,media_path) \
                 SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS ( \
                   SELECT 1 FROM deleted_chats d WHERE d.chat = ? OR d.chat = (SELECT pn FROM contacts WHERE jid = ?))",
            )
            .bind::<Text, _>(&m.chat)
            .bind::<Text, _>(&m.id)
            .bind::<Text, _>(&m.sender)
            .bind::<Bool, _>(m.from_me)
            .bind::<BigInt, _>(m.ts)
            .bind::<Nullable<Text>, _>(&m.text)
            .bind::<Nullable<Text>, _>(&m.media_kind)
            .bind::<Nullable<Text>, _>(&m.media_path)
            .bind::<Text, _>(&m.chat)
            .bind::<Text, _>(&m.chat)
            .execute(c)?;
            fresh.push(n == 1);
        }
        Ok(fresh)
    })
}

#[derive(QueryableByName, Debug)]
pub struct Anchor {
    #[diesel(sql_type = Text)]
    pub chat: String,
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Bool)]
    pub from_me: bool,
    #[diesel(sql_type = BigInt)]
    pub ts: i64,
}

/// Oldest cached message of a chat (either id of a linked pair), with its own chat id: the anchor
/// WhatsApp needs to know where "older than this" starts.
pub fn oldest(c: &mut SqliteConnection, chat: &str) -> QueryResult<Option<Anchor>> {
    sql_query(
        "SELECT chat,id,from_me,ts FROM messages \
         WHERE chat IN (SELECT ? UNION SELECT jid FROM contacts WHERE pn = ?) AND ts > 0 ORDER BY ts ASC, rowid ASC LIMIT 1",
    )
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat)
    .get_result(c)
    .optional()
}

#[derive(QueryableByName, Debug, serde::Serialize)]
pub struct Row {
    #[diesel(sql_type = BigInt)]
    pub seq: i64,
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Text)]
    pub sender: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub sender_name: Option<String>,
    #[diesel(sql_type = Bool)]
    pub from_me: bool,
    #[diesel(sql_type = BigInt)]
    pub ts: i64,
    #[diesel(sql_type = Nullable<Text>)]
    pub text: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub media_kind: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub media_path: Option<String>,
}

/// Newest first. First page: before = (i64::MAX, i64::MAX). Next page: (last.ts, last.seq).
pub fn page(
    c: &mut SqliteConnection,
    chat: &str,
    before: (i64, i64),
    limit: i64,
) -> QueryResult<Vec<Row>> {
    if let Some(sender) = chat.strip_prefix("status@") {
        if sender != "broadcast" {
            return sql_query(
                "SELECT messages.rowid AS seq,messages.id,messages.sender, \
                   COALESCE((SELECT name FROM contacts WHERE jid=messages.sender LIMIT 1), \
                            (SELECT name FROM contacts WHERE pn=messages.sender LIMIT 1)) AS sender_name, \
                   messages.from_me,messages.ts,messages.text,messages.media_kind,messages.media_path \
                 FROM messages WHERE messages.chat = 'status@broadcast' \
                   AND messages.sender IN (SELECT ? UNION SELECT jid FROM contacts WHERE pn = ?) \
                   AND (messages.ts,messages.rowid)<(?,?) ORDER BY messages.ts DESC,messages.rowid DESC LIMIT ?",
            )
            .bind::<Text, _>(sender)
            .bind::<Text, _>(sender)
            .bind::<BigInt, _>(before.0)
            .bind::<BigInt, _>(before.1)
            .bind::<BigInt, _>(limit)
            .load(c);
        }
    }
    sql_query(
        "SELECT messages.rowid AS seq,messages.id,messages.sender, \
           COALESCE((SELECT name FROM contacts WHERE jid=messages.sender LIMIT 1), \
                    (SELECT name FROM contacts WHERE pn=messages.sender LIMIT 1)) AS sender_name, \
           messages.from_me,messages.ts,messages.text,messages.media_kind,messages.media_path \
         FROM messages WHERE messages.chat IN (SELECT ? UNION SELECT jid FROM contacts WHERE pn = ?) \
           AND (messages.ts,messages.rowid)<(?,?) ORDER BY messages.ts DESC,messages.rowid DESC LIMIT ?",
    )
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat) // a phone-number chat also covers the @lid id linked to it
    .bind::<BigInt, _>(before.0)
    .bind::<BigInt, _>(before.1)
    .bind::<BigInt, _>(limit)
    .load(c)
}

#[derive(QueryableByName, Debug, serde::Serialize)]
pub struct ChatRow {
    #[diesel(sql_type = Text)]
    pub chat: String,
    #[diesel(sql_type = BigInt)]
    pub last_ts: i64,
    #[diesel(sql_type = Nullable<Text>)]
    pub last_message: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub name: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub avatar_path: Option<String>,
}

#[derive(QueryableByName, Debug, serde::Serialize)]
pub struct ContactRow {
    #[diesel(sql_type = Text)]
    pub jid: String,
    #[diesel(sql_type = Text)]
    pub name: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub pn: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub avatar_path: Option<String>,
}

pub fn contacts(c: &mut SqliteConnection) -> QueryResult<Vec<ContactRow>> {
    sql_query(
        "SELECT jid,name,pn,avatar_path FROM contacts          UNION ALL SELECT DISTINCT chat AS jid, chat AS name, NULL AS pn, NULL AS avatar_path          FROM messages WHERE chat LIKE '%@g.us' AND chat NOT IN (SELECT jid FROM contacts)          ORDER BY jid",
    )
    .load(c)
}

/// A name with no stated origin (rank 0). Only the tests use this shorthand.
#[cfg(test)]
pub fn upsert_contact(
    c: &mut SqliteConnection,
    jid: &str,
    name: &str,
    pn: Option<&str>,
    avatar_path: Option<&str>,
) -> QueryResult<()> {
    upsert_contact_ranked(c, jid, name, pn, avatar_path, 0)
}

/// Name, number and picture of a jid. Re-sent contacts update the row instead of duplicating it.
/// `rank` says where the name came from: 2 = the phone's address book, 1 = the name the person chose
/// for themselves (push name), 0 = unknown. A name is replaced only by one of equal or higher rank, so
/// a push name arriving later can never replace the name you saved.
pub fn upsert_contact_ranked(
    c: &mut SqliteConnection,
    jid: &str,
    name: &str,
    pn: Option<&str>,
    avatar_path: Option<&str>,
    rank: i64,
) -> QueryResult<()> {
    sql_query(
        "INSERT INTO contacts(jid,name,pn,avatar_path,name_rank) VALUES(?,?,?,?,?) \
         ON CONFLICT(jid) DO UPDATE SET \
         name=CASE WHEN excluded.name<>excluded.jid AND excluded.name_rank>=contacts.name_rank THEN excluded.name ELSE contacts.name END, \
         name_rank=CASE WHEN excluded.name<>excluded.jid AND excluded.name_rank>=contacts.name_rank THEN excluded.name_rank ELSE contacts.name_rank END, \
         pn=COALESCE(excluded.pn, contacts.pn), avatar_path=COALESCE(excluded.avatar_path, contacts.avatar_path)",
    )
    .bind::<Text, _>(jid)
    .bind::<Text, _>(name)
    .bind::<Nullable<Text>, _>(pn)
    .bind::<Nullable<Text>, _>(avatar_path)
    .bind::<BigInt, _>(rank)
        .execute(c)
        .map(drop)
}

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = Text)]
    id: String,
}

/// Every id a chat is known by: its own, and the other half of a phone-number / @lid pair.
pub fn chat_ids(c: &mut SqliteConnection, chat: &str) -> QueryResult<Vec<String>> {
    let rows: Vec<IdRow> = sql_query(
        "SELECT jid AS id FROM contacts WHERE jid = ? OR pn = ? \
         UNION SELECT pn AS id FROM contacts WHERE (jid = ? OR pn = ?) AND pn IS NOT NULL",
    )
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat)
    .load(c)?;
    Ok(rows.into_iter().map(|r| r.id).collect())
}

/// Chats with messages, plus contacts you have never exchanged a message with (last_ts 0).
/// Most recently active first; bounded.
pub fn chats(c: &mut SqliteConnection) -> QueryResult<Vec<ChatRow>> {
    sql_query(
        "WITH chat_messages AS ( \
           SELECT COALESCE(c.pn, m.chat) AS chat, m.ts, m.text, \
                  ROW_NUMBER() OVER (PARTITION BY COALESCE(c.pn, m.chat) ORDER BY m.ts DESC, m.rowid DESC) AS rn \
           FROM messages m \
           LEFT JOIN contacts c ON c.jid = m.chat \
           WHERE m.chat != 'status@broadcast' \
         ), \
         latest_messages AS ( \
           SELECT chat, ts AS last_ts, text AS last_message \
           FROM chat_messages \
           WHERE rn = 1 \
         ), \
         status_senders AS ( \
           SELECT 'status@' || m.sender AS chat, MAX(m.ts) AS last_ts, \
                  (SELECT m2.text FROM messages m2 WHERE m2.chat = 'status@broadcast' AND m2.sender = m.sender ORDER BY m2.ts DESC, m2.rowid DESC LIMIT 1) AS last_message \
           FROM messages m \
           WHERE m.chat = 'status@broadcast' AND m.sender != 'status@broadcast' \
           GROUP BY m.sender \
         ), \
         all_chats AS ( \
           SELECT chat, last_ts, last_message FROM latest_messages \
           UNION ALL \
           SELECT chat, last_ts, last_message FROM status_senders \
           UNION ALL \
           SELECT COALESCE(pn, jid) AS chat, 0 AS last_ts, NULL AS last_message \
           FROM contacts \
           WHERE COALESCE(pn, jid) NOT IN (SELECT chat FROM latest_messages) \
         ) \
         SELECT a.chat, a.last_ts, a.last_message, \
           COALESCE(CASE WHEN c.name IS NULL OR c.name = a.chat THEN \
             (SELECT x.name FROM contacts x WHERE (x.jid = a.chat OR x.pn = a.chat OR ('status@' || x.jid) = a.chat OR ('status@' || x.pn) = a.chat) AND x.name <> x.jid ORDER BY x.name_rank DESC LIMIT 1) \
           END, c.name, a.chat) AS name, \
           COALESCE(c.avatar_path, \
             (SELECT x.avatar_path FROM contacts x WHERE (x.jid = a.chat OR x.pn = a.chat OR ('status@' || x.jid) = a.chat OR ('status@' || x.pn) = a.chat) AND x.avatar_path IS NOT NULL LIMIT 1)) AS avatar_path \
         FROM all_chats a \
         LEFT JOIN contacts c ON c.jid = a.chat OR c.pn = a.chat OR ('status@' || c.jid) = a.chat OR ('status@' || c.pn) = a.chat \
         WHERE a.chat NOT IN (SELECT chat FROM deleted_chats) \
         GROUP BY a.chat \
         ORDER BY a.last_ts DESC, name LIMIT 2000",
    )
    .load(c)
}

/// Remove a conversation and its cached messages from this device.
pub fn delete_chat(c: &mut SqliteConnection, chat: &str) -> QueryResult<()> {
    c.transaction(|c| {
        sql_query("DELETE FROM messages WHERE chat = ? OR chat IN (SELECT jid FROM contacts WHERE pn = ?)")
            .bind::<Text, _>(chat).bind::<Text, _>(chat).execute(c)?;
        sql_query("INSERT OR REPLACE INTO deleted_chats(chat) VALUES(?)").bind::<Text, _>(chat).execute(c)?;
        Ok(())
    })
}

/// How a person or chat is named and pictured. Follows the @lid <-> phone-number link both ways.
#[derive(QueryableByName, Debug, Clone)]
pub struct Card {
    #[diesel(sql_type = Text)]
    pub name: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub avatar_path: Option<String>,
}

pub fn card(c: &mut SqliteConnection, jid: &str) -> QueryResult<Option<Card>> {
    sql_query(
        "SELECT name, \
           COALESCE(avatar_path, (SELECT a.avatar_path FROM contacts a \
             WHERE a.avatar_path IS NOT NULL AND (a.jid = ? OR a.pn = ? OR a.jid = contacts.pn OR a.pn = contacts.pn) LIMIT 1)) \
             AS avatar_path \
         FROM contacts WHERE jid = ? OR pn = ? ORDER BY (jid = ?) DESC LIMIT 1",
    )
    .bind::<Text, _>(jid)
    .bind::<Text, _>(jid)
    .bind::<Text, _>(jid)
    .bind::<Text, _>(jid)
    .bind::<Text, _>(jid)
    .get_result(c)
    .optional()
}

#[derive(QueryableByName)]
struct ParamsRow {
    #[diesel(sql_type = Text)]
    params: String,
}

pub fn get_media_params(c: &mut SqliteConnection, chat: &str, id: &str) -> QueryResult<Option<String>> {
    let row: Option<ParamsRow> = sql_query(
        "SELECT params FROM media_downloads WHERE id = ? AND (chat = ? OR chat IN (SELECT jid FROM contacts WHERE pn = ?)) LIMIT 1"
    )
    .bind::<Text, _>(id)
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat)
    .get_result(c)
    .optional()?;
    Ok(row.map(|r| r.params))
}

pub fn save_media_params(c: &mut SqliteConnection, chat: &str, id: &str, params: &str) -> QueryResult<()> {
    sql_query("INSERT OR REPLACE INTO media_downloads (chat, id, params) VALUES (?, ?, ?)")
        .bind::<Text, _>(chat)
        .bind::<Text, _>(id)
        .bind::<Text, _>(params)
        .execute(c)?;
    Ok(())
}

pub fn update_media_path(c: &mut SqliteConnection, chat: &str, id: &str, path: &str) -> QueryResult<()> {
    sql_query(
        "UPDATE messages SET media_path = ? WHERE id = ? AND (chat = ? OR chat IN (SELECT jid FROM contacts WHERE pn = ?))"
    )
    .bind::<Text, _>(path)
    .bind::<Text, _>(id)
    .bind::<Text, _>(chat)
    .bind::<Text, _>(chat)
    .execute(c)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, ts: i64) -> Incoming {
        Incoming {
            id: id.into(),
            chat: "a".into(),
            sender: "s".into(),
            from_me: false,
            ts,
            text: None,
            media_kind: None,
            media_path: None,
            historical: false,
            media_params: None,
        }
    }

    #[test]
    fn picture_and_name_are_found_through_the_linked_id() {
        let path = std::env::temp_dir().join(format!("wa-test-avatar-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut c = open(path.to_str().unwrap()).unwrap();
        // The picture was fetched for the @lid id; the chat is listed under the phone-number id.
        let mut m = msg("1", 100);
        m.chat = "p@s.whatsapp.net".into();
        insert(&mut c, &m).unwrap();
        upsert_contact(&mut c, "p@s.whatsapp.net", "p@s.whatsapp.net", Some("p@s.whatsapp.net"), None).unwrap();
        upsert_contact(&mut c, "p@lid", "Pat", Some("p@s.whatsapp.net"), Some("/x/a.jpg")).unwrap();
        let rows = chats(&mut c).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].avatar_path.as_deref(), Some("/x/a.jpg"));
        assert_eq!(rows[0].name.as_deref(), Some("Pat"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_saved_name_is_never_replaced_by_a_push_name() {
        let path = std::env::temp_dir().join(format!("wa-test-rank-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut c = open(path.to_str().unwrap()).unwrap();
        let name_of = |c: &mut SqliteConnection| {
            contacts(c).unwrap().into_iter().find(|r| r.jid == "p@s.whatsapp.net").unwrap().name
        };
        // push name first, then the saved name: the saved name wins
        upsert_contact_ranked(&mut c, "p@s.whatsapp.net", "Pat the Plumber", None, None, 1).unwrap();
        upsert_contact_ranked(&mut c, "p@s.whatsapp.net", "Mom", None, None, 2).unwrap();
        assert_eq!(name_of(&mut c), "Mom");
        // a later push name (live message, push name update) must not undo it
        upsert_contact_ranked(&mut c, "p@s.whatsapp.net", "Pat the Plumber", None, None, 1).unwrap();
        assert_eq!(name_of(&mut c), "Mom");
        // an avatar-only update that repeats some name at rank 0 must not undo it either
        upsert_contact_ranked(&mut c, "p@s.whatsapp.net", "whatever", None, Some("/a.jpg"), 0).unwrap();
        assert_eq!(name_of(&mut c), "Mom");
        // the address book changing the name is allowed
        upsert_contact_ranked(&mut c, "p@s.whatsapp.net", "Mother", None, None, 2).unwrap();
        assert_eq!(name_of(&mut c), "Mother");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn chat_ids_cover_both_halves_of_a_linked_pair() {
        let path = std::env::temp_dir().join(format!("wa-test-ids-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut c = open(path.to_str().unwrap()).unwrap();
        upsert_contact(&mut c, "p@lid", "Pat", Some("p@s.whatsapp.net"), None).unwrap();
        for id in ["p@lid", "p@s.whatsapp.net"] {
            let mut ids = chat_ids(&mut c, id).unwrap();
            ids.sort();
            assert_eq!(ids, ["p@lid", "p@s.whatsapp.net"], "from {id}");
        }
        assert!(chat_ids(&mut c, "nobody@s.whatsapp.net").unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn dedup_and_pagination() {
        let path = std::env::temp_dir().join(format!("wa-test-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut c = open(path.to_str().unwrap()).unwrap();

        assert!(insert(&mut c, &msg("1", 100)).unwrap());
        assert!(
            !insert(&mut c, &msg("1", 100)).unwrap(),
            "duplicate must be ignored"
        );
        for (id, ts) in [("2", 300), ("3", 200), ("4", 300)] {
            insert(&mut c, &msg(id, ts)).unwrap();
        }

        let p1 = page(&mut c, "a", (i64::MAX, i64::MAX), 2).unwrap();
        let last = p1.last().unwrap();
        let p2 = page(&mut c, "a", (last.ts, last.seq), 10).unwrap();
        let ids: Vec<_> = p1.iter().chain(&p2).map(|r| r.id.as_str()).collect();
        assert_eq!(ids, ["4", "2", "3", "1"]);
        upsert_contact(&mut c, "a", "Alice", None, None).unwrap();
        upsert_contact(&mut c, "z", "Zed", None, None).unwrap();
        upsert_contact(&mut c, "z", "Zed Z", None, None).unwrap(); // update, not a duplicate
        let cs = chats(&mut c).unwrap();
        assert_eq!(cs.len(), 2);
        assert_eq!(
            (cs[0].chat.as_str(), cs[0].last_ts, cs[0].name.as_deref()),
            ("a", 300, Some("Alice"))
        );
        assert_eq!(
            (cs[1].chat.as_str(), cs[1].last_ts, cs[1].name.as_deref()),
            ("z", 0, Some("Zed Z"))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lid_is_merged_into_its_phone_number_chat() {
        let path = std::env::temp_dir().join(format!("wa-alias-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let mut c = open(path.to_str().unwrap()).unwrap();

        upsert_contact(&mut c, "p@s.whatsapp.net", "Pat", None, None).unwrap();
        upsert_contact(&mut c, "p@lid", "Pat", Some("p@s.whatsapp.net"), None).unwrap();
        let mk = |id: &str, chat: &str, ts| Incoming {
            id: id.into(),
            chat: chat.into(),
            sender: "s".into(),
            from_me: false,
            ts,
            text: None,
            media_kind: None,
            media_path: None,
            historical: false,
            media_params: None,
        };
        insert(&mut c, &mk("1", "p@s.whatsapp.net", 100)).unwrap(); // what we sent
        insert(&mut c, &mk("2", "p@lid", 200)).unwrap(); // their reply arrives under the lid id

        let cs = chats(&mut c).unwrap();
        assert_eq!(cs.len(), 1, "one person, one chat");
        assert_eq!(
            (cs[0].chat.as_str(), cs[0].last_ts),
            ("p@s.whatsapp.net", 200)
        );
        let ids: Vec<_> = page(&mut c, "p@s.whatsapp.net", (i64::MAX, i64::MAX), 10)
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect();
        assert_eq!(ids, ["2", "1"]);
        let _ = std::fs::remove_file(path);
    }

    fn hist(id: &str, chat: &str, ts: i64) -> Incoming {
        Incoming {
            id: id.into(),
            chat: chat.into(),
            sender: "s".into(),
            from_me: false,
            ts,
            text: Some("t".into()),
            media_kind: None,
            media_path: None,
            historical: true,
            media_params: None,
        }
    }

    fn fresh_db(name: &str) -> (SqliteConnection, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("wa-{name}-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        (open(path.to_str().unwrap()).unwrap(), path)
    }

    #[test]
    fn history_batch_is_idempotent() {
        let (mut c, path) = fresh_db("hist");
        let batch = [hist("1", "a", 10), hist("2", "a", 20), hist("2", "a", 20)]; // a duplicate inside one batch
        assert_eq!(insert_batch(&mut c, &batch).unwrap(), [true, true, false]);
        assert_eq!(insert_batch(&mut c, &batch).unwrap(), [false, false, false]); // re-importing changes nothing
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn history_never_resurrects_a_deleted_chat_but_a_live_message_does() {
        let (mut c, path) = fresh_db("del");
        upsert_contact(&mut c, "p@s.whatsapp.net", "Pat", None, None).unwrap();
        upsert_contact(&mut c, "p@lid", "Pat", Some("p@s.whatsapp.net"), None).unwrap();
        insert(&mut c, &hist("1", "p@s.whatsapp.net", 10)).unwrap();
        delete_chat(&mut c, "p@s.whatsapp.net").unwrap();

        // old history arriving under the @lid id of the same person stays out
        assert_eq!(
            insert_batch(&mut c, &[hist("2", "p@lid", 5)]).unwrap(),
            [false]
        );
        assert!(chats(&mut c).unwrap().is_empty());

        // a live message is a reason to bring the chat back
        let mut live = hist("3", "p@lid", 30);
        live.historical = false;
        assert_eq!(insert_batch(&mut c, &[live]).unwrap(), [true]);
        assert!(
            chats(&mut c)
                .unwrap()
                .iter()
                .any(|r| r.chat == "p@s.whatsapp.net" && r.last_ts == 30)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn oldest_anchor_spans_both_ids_and_keeps_the_raw_chat() {
        let (mut c, path) = fresh_db("anchor");
        upsert_contact(&mut c, "p@s.whatsapp.net", "Pat", None, None).unwrap();
        upsert_contact(&mut c, "p@lid", "Pat", Some("p@s.whatsapp.net"), None).unwrap();
        insert(&mut c, &hist("new", "p@s.whatsapp.net", 500)).unwrap();
        insert(&mut c, &hist("old", "p@lid", 300)).unwrap();
        let a = oldest(&mut c, "p@s.whatsapp.net").unwrap().unwrap();
        assert_eq!(
            (a.chat.as_str(), a.id.as_str(), a.ts),
            ("p@lid", "old", 300)
        );
        assert!(oldest(&mut c, "nobody").unwrap().is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn card_resolves_names_and_avatars_through_the_lid_link() {
        let (mut c, path) = fresh_db("card");
        upsert_contact(&mut c, "p@s.whatsapp.net", "Pat", None, Some("/av/pat.jpg")).unwrap();
        upsert_contact(&mut c, "p@lid", "Pat", Some("p@s.whatsapp.net"), None).unwrap();
        for jid in ["p@s.whatsapp.net", "p@lid"] {
            let k = card(&mut c, jid).unwrap().unwrap();
            assert_eq!((k.name.as_str(), k.avatar_path.as_deref()), ("Pat", Some("/av/pat.jpg")), "{jid}");
        }
        assert!(card(&mut c, "nobody@lid").unwrap().is_none());
        let _ = std::fs::remove_file(path);
    }
}
