CREATE TABLE messages (
    chat    TEXT    NOT NULL,
    id      TEXT    NOT NULL,
    sender  TEXT    NOT NULL,
    from_me INTEGER NOT NULL,
    ts      INTEGER NOT NULL,
    text    TEXT,
    PRIMARY KEY (chat, id)
);
CREATE INDEX messages_chat_ts ON messages (chat, ts DESC);
