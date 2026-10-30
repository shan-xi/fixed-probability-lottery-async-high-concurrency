-- A lease lets publishers claim work, commit, and then perform Kafka I/O without
-- holding PostgreSQL row locks. Expired leases are automatically eligible again.
ALTER TABLE lottery_outbox
    ADD COLUMN claim_token VARCHAR(36),
    ADD COLUMN locked_until TIMESTAMPTZ;

DROP INDEX IF EXISTS idx_lottery_outbox_unpublished;
CREATE INDEX idx_lottery_outbox_ready
    ON lottery_outbox (id)
    WHERE published_at IS NULL;
