-- pn = the phone-number id that a @lid contact belongs to (NULL for phone-number ids themselves).
ALTER TABLE contacts ADD COLUMN pn TEXT;
CREATE INDEX contacts_pn ON contacts (pn);

-- Backfill for contacts imported before this migration: a name that exists exactly once as a @lid id
-- and exactly once as a phone-number id is taken to be the same person. Ambiguous names stay unlinked.
UPDATE contacts SET pn = (
    SELECT p.jid FROM contacts p WHERE p.name = contacts.name AND p.jid LIKE '%@s.whatsapp.net'
)
WHERE jid LIKE '%@lid'
  AND (SELECT count(*) FROM contacts x WHERE x.name = contacts.name AND x.jid LIKE '%@lid') = 1
  AND (SELECT count(*) FROM contacts x WHERE x.name = contacts.name AND x.jid LIKE '%@s.whatsapp.net') = 1;
