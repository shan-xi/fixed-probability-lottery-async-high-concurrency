package com.example.lottery.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class PrizeInventoryService {
    private final JdbcTemplate jdbc;
    public PrizeInventoryService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public List<InventoryView> list() {
        return jdbc.query("""
                SELECT prize_type, quota, awarded_count, quota - awarded_count AS remaining_count
                FROM lottery_prize_inventory ORDER BY prize_type
                """, (rs, rowNum) -> new InventoryView(rs.getString("prize_type"),
                rs.getInt("quota"), rs.getInt("awarded_count"), rs.getInt("remaining_count")));
    }

    public record InventoryView(String prizeType, int quota, int awardedCount, int remainingCount) { }
}
