package com.example.lottery.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Seeds default quotas on a fresh database without overwriting operator-managed inventory.
 */
@Component
public class LimitedPrizeInventoryInitializer implements ApplicationRunner {
    private final JdbcTemplate jdbc;
    private final int prize1Quota;
    private final int prize2Quota;

    public LimitedPrizeInventoryInitializer(
            JdbcTemplate jdbc,
            @Value("${lottery.limited-prizes.prize-1-quota:1}") int prize1Quota,
            @Value("${lottery.limited-prizes.prize-2-quota:10}") int prize2Quota) {
        if (prize1Quota < 0 || prize2Quota < 0) {
            throw new IllegalArgumentException("Limited prize quotas cannot be negative");
        }
        this.jdbc = jdbc;
        this.prize1Quota = prize1Quota;
        this.prize2Quota = prize2Quota;
    }

    @Override
    public void run(ApplicationArguments args) {
        seed("PRIZE_1", prize1Quota);
        seed("PRIZE_2", prize2Quota);
    }

    private void seed(String prizeType, int quota) {
        // Do not overwrite existing counts or quotas on application restart.
        jdbc.update("""
                INSERT INTO lottery_prize_inventory(prize_type, quota, awarded_count)
                VALUES (?, ?, 0)
                ON CONFLICT (prize_type) DO NOTHING
                """, prizeType, quota);
    }
}
