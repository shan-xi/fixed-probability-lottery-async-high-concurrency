CREATE TABLE lottery_draws (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT NOT NULL,
    prize_type VARCHAR(20) NOT NULL CHECK (prize_type IN (
        'PRIZE_1','PRIZE_2','PRIZE_3','PRIZE_4','PRIZE_5',
        'PRIZE_6','PRIZE_7','PRIZE_8','PRIZE_9','PRIZE_10'
    )),
    random_ticket INTEGER NOT NULL CHECK (random_ticket >= 0 AND random_ticket < 10000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_lottery_draws_user UNIQUE (user_id)
);
CREATE INDEX idx_lottery_draws_created_at ON lottery_draws(created_at);
