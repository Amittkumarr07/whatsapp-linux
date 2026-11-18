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
         WHERE chat IN (SELECT ? UNION SELECT jid FROM contacts WHERE pn = ?) ORDER BY ts ASC, rowid ASC LIMIT 1",
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
    sql_query("SELECT jid,name,pn,avatar_path FROM contacts ORDER BY jid").load(c)
}

/// Address-book name for a jid. Re-sent contacts update the name instead of duplicating it.
pub fn upsert_contact(
    c: &mut SqliteConnection,
    jid: &str,
    name: &str,
    pn: Option<&str>,
    avatar_path: Option<&str>,
) -> QueryResult<()> {
    sql_query(
        "INSERT INTO contacts(jid,name,pn,avatar_path) VALUES(?,?,?,?) \
         ON CONFLICT(jid) DO UPDATE SET name=CASE WHEN excluded.name=excluded.jid THEN contacts.name ELSE excluded.name END, \
         pn=COALESCE(excluded.pn, contacts.pn), avatar_path=COALESCE(excluded.avatar_path, contacts.avatar_path)",
    )
    .bind::<Text, _>(jid)
    .bind::<Text, _>(name)
    .bind::<Nullable<Text>, _>(pn)
    .bind::<Nullable<Text>, _>(avatar_path)
        .execute(c)
        .map(drop)
}

/// Chats with messages, plus contacts you have never exchanged a message with (last_ts 0).
/// Most recently active first; bounded.
pub fn chats(c: &mut SqliteConnection) -> QueryResult<Vec<ChatRow>> {
    sql_query(
        "SELECT groups.chat, MAX(groups.ts) AS last_ts, groups.name, groups.avatar_path, \
           (SELECT m2.text FROM messages m2 LEFT JOIN contacts c2 ON c2.jid=m2.chat \
             WHERE COALESCE(c2.pn,m2.chat)=groups.chat ORDER BY m2.ts DESC,m2.rowid DESC LIMIT 1) AS last_message \
         FROM ( \
           SELECT COALESCE(c.pn, m.chat) AS chat, m.ts AS ts, c.name AS name, c.avatar_path AS avatar_path \
             FROM messages m LEFT JOIN contacts c ON c.jid = m.chat \
           UNION ALL SELECT COALESCE(pn, jid), 0, name, avatar_path FROM contacts \
         ) AS groups GROUP BY groups.chat HAVING groups.chat NOT IN (SELECT chat FROM deleted_chats) ORDER BY last_ts DESC, groups.name LIMIT 2000",
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
        }
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
