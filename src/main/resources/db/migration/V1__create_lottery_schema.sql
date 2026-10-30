-- Fresh-database schema for the asynchronous monthly lottery.
-- This project is intentionally reset from an empty PostgreSQL volume, so the
-- complete schema lives in one Flyway migration.

CREATE SEQUENCE lottery_draws_id_seq;

CREATE TABLE lottery_draws (
    id BIGINT NOT NULL DEFAULT nextval('lottery_draws_id_seq'),
    campaign_month DATE NOT NULL,
    user_id BIGINT NOT NULL,
    prize_type VARCHAR(20) NOT NULL DEFAULT 'NO_PRIZE' CHECK (prize_type IN (
        'PRIZE_1','PRIZE_2','PRIZE_3','PRIZE_4','PRIZE_5',
        'PRIZE_6','PRIZE_7','PRIZE_8','PRIZE_9','PRIZE_10','NO_PRIZE'
    )),
    selected_prize_type VARCHAR(20) CHECK (selected_prize_type IN (
        'PRIZE_1','PRIZE_2','PRIZE_3','PRIZE_4','PRIZE_5',
        'PRIZE_6','PRIZE_7','PRIZE_8','PRIZE_9','PRIZE_10'
    )),
    random_ticket INTEGER CHECK (random_ticket >= 0 AND random_ticket < 10000),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','COMPLETED','FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_lottery_draws PRIMARY KEY (campaign_month, id),
    CONSTRAINT uq_lottery_draws_campaign_user UNIQUE (campaign_month, user_id)
) PARTITION BY RANGE (campaign_month);

ALTER SEQUENCE lottery_draws_id_seq OWNED BY lottery_draws.id;

-- Keep explicit monthly partitions ahead of the campaign calendar. The default
-- partition prevents inserts from failing if a campaign is opened outside this window.
DO $$
DECLARE
    month_start DATE := (date_trunc('month', CURRENT_DATE) - INTERVAL '12 months')::date;
    month_end DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..36 LOOP
        month_end := (month_start + INTERVAL '1 month')::date;
        partition_name := 'lottery_draws_' || to_char(month_start, 'YYYY_MM');
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF lottery_draws FOR VALUES FROM (%L) TO (%L)',
            partition_name, month_start, month_end
        );
        month_start := month_end;
    END LOOP;
END $$;

CREATE TABLE lottery_draws_default PARTITION OF lottery_draws DEFAULT;

CREATE INDEX idx_lottery_draws_id ON lottery_draws (id);
CREATE INDEX idx_lottery_draws_status_created ON lottery_draws (status, created_at);
CREATE INDEX idx_lottery_draws_user ON lottery_draws (user_id, campaign_month DESC);

CREATE TABLE lottery_prize_inventory (
    prize_type VARCHAR(20) PRIMARY KEY CHECK (prize_type IN ('PRIZE_1','PRIZE_2')),
    quota INTEGER NOT NULL CHECK (quota >= 0),
    awarded_count INTEGER NOT NULL DEFAULT 0 CHECK (awarded_count >= 0 AND awarded_count <= quota),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE lottery_outbox (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id BIGINT NOT NULL,
    campaign_month DATE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    claim_token VARCHAR(36) NULL,
    locked_until TIMESTAMPTZ NULL,
    CONSTRAINT fk_lottery_outbox_draw
        FOREIGN KEY (campaign_month, aggregate_id)
        REFERENCES lottery_draws (campaign_month, id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX uq_lottery_outbox_draw_event
    ON lottery_outbox (event_type, aggregate_id);

CREATE INDEX idx_lottery_outbox_ready
    ON lottery_outbox (id)
    WHERE published_at IS NULL;

CREATE INDEX idx_lottery_outbox_draw_ref
    ON lottery_outbox (campaign_month, aggregate_id);
