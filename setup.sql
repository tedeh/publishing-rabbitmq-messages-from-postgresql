CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS queue_message (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  exchange text NOT NULL,
  routing_key text NOT NULL DEFAULT '',
  content jsonb NOT NULL DEFAULT '{}',
  options jsonb NOT NULL DEFAULT '{}'
);

DROP FUNCTION IF EXISTS queue_message_notify() CASCADE;
CREATE OR REPLACE FUNCTION queue_message_notify() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('queue_message_notify', jsonb_build_object('id', NEW.id::text)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tgr_queue_message_after_insert ON queue_message;
CREATE TRIGGER tgr_queue_message_after_insert
AFTER INSERT ON queue_message
FOR EACH ROW EXECUTE PROCEDURE queue_message_notify();
