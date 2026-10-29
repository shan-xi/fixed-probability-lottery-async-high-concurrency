-- Keep V1 immutable so an existing fixed-probability-lottery database can be upgraded safely.
ALTER TABLE lottery_draws ADD COLUMN selected_prize_type VARCHAR(20);
UPDATE lottery_draws SET selected_prize_type = prize_type;
ALTER TABLE lottery_draws ALTER COLUMN selected_prize_type SET NOT NULL;

-- Existing V1 inline check is named by PostgreSQL as lottery_draws_prize_type_check.
ALTER TABLE lottery_draws DROP CONSTRAINT lottery_draws_prize_type_check;
ALTER TABLE lottery_draws ADD CONSTRAINT ck_lottery_draws_prize_type CHECK (prize_type IN (
    'PRIZE_1','PRIZE_2','PRIZE_3','PRIZE_4','PRIZE_5',
    'PRIZE_6','PRIZE_7','PRIZE_8','PRIZE_9','PRIZE_10','NO_PRIZE'
));
ALTER TABLE lottery_draws ADD CONSTRAINT ck_lottery_draws_selected_prize_type CHECK (selected_prize_type IN (
    'PRIZE_1','PRIZE_2','PRIZE_3','PRIZE_4','PRIZE_5',
    'PRIZE_6','PRIZE_7','PRIZE_8','PRIZE_9','PRIZE_10'
));

-- Only prizes with a finite quota have rows in this table. Other prize types are unlimited.
CREATE TABLE lottery_prize_inventory (
    prize_type VARCHAR(20) PRIMARY KEY CHECK (prize_type IN ('PRIZE_1','PRIZE_2')),
    quota INTEGER NOT NULL CHECK (quota >= 0),
    awarded_count INTEGER NOT NULL DEFAULT 0 CHECK (awarded_count >= 0 AND awarded_count <= quota),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
