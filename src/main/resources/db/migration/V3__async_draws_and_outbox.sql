-- Convert draw processing to asynchronous state transitions. Existing historical rows remain COMPLETED.
ALTER TABLE lottery_draws ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED';
ALTER TABLE lottery_draws ADD CONSTRAINT ck_lottery_draws_status CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED'));
ALTER TABLE lottery_draws ALTER COLUMN selected_prize_type DROP NOT NULL;
ALTER TABLE lottery_draws ALTER COLUMN random_ticket DROP NOT NULL;
ALTER TABLE lottery_draws ALTER COLUMN prize_type SET DEFAULT 'NO_PRIZE';

CREATE TABLE lottery_outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id BIGINT NOT NULL REFERENCES lottery_draws(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL
);
CREATE INDEX idx_lottery_outbox_unpublished ON lottery_outbox(id) WHERE published_at IS NULL;
CREATE UNIQUE INDEX uq_lottery_outbox_draw_event ON lottery_outbox(event_type, aggregate_id);
CREATE INDEX idx_lottery_draws_status_created ON lottery_draws(status, created_at);
