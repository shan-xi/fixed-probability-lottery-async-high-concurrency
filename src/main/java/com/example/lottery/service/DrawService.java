package com.example.lottery.service;

import com.example.lottery.domain.DrawRecord;
import com.example.lottery.domain.PrizeDraw;
import com.example.lottery.domain.PrizeType;
import com.example.lottery.domain.WeightedPrizeSelector;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class DrawService {
    private final JdbcTemplate jdbc;
    private final WeightedPrizeSelector selector;

    public DrawService(JdbcTemplate jdbc, WeightedPrizeSelector selector) {
        this.jdbc = jdbc;
        this.selector = selector;
    }

    /**
     * One draw per user. First insert the candidate outcome; only the transaction that wins the
     * UNIQUE(user_id) race may attempt to consume limited inventory. This prevents duplicate
     * requests from consuming quota. The insert, inventory claim, and final outcome are atomic.
     */
    @Transactional
    public DrawRecord draw(long userId) {
        if (userId <= 0) throw new IllegalArgumentException("userId must be positive");
        PrizeDraw candidate = selector.draw();

        List<DrawRecord> inserted = jdbc.query("""
                INSERT INTO lottery_draws(user_id, prize_type, selected_prize_type, random_ticket)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (user_id) DO NOTHING
                RETURNING id, user_id, prize_type, selected_prize_type, random_ticket, created_at
                """, (rs, rowNum) -> mapRecord(rs, false), userId,
                candidate.prizeType().name(), candidate.prizeType().name(), candidate.randomTicket());

        if (inserted.isEmpty()) {
            // Under PostgreSQL READ COMMITTED, this separate statement sees the committed winner
            // of the unique-key race. No inventory was consumed by this duplicate request.
            return findByUserId(userId, true);
        }

        DrawRecord created = inserted.getFirst();
        if (candidate.prizeType().isLimited()) {
            Integer newCount = claimOneUnit(candidate.prizeType());
            if (newCount == null) {
                // The probability mapping stays fixed. If the selected limited prize is sold out,
                // this outcome becomes NO_PRIZE rather than redistributing its probability.
                jdbc.update("UPDATE lottery_draws SET prize_type='NO_PRIZE' WHERE id=?", created.id());
                return new DrawRecord(created.id(), created.userId(), PrizeType.NO_PRIZE,
                        created.selectedPrizeType(), created.randomTicket(), created.createdAt(), false);
            }
        }
        return created;
    }

    /** Atomic conditional update: concurrent requests cannot claim beyond the configured quota. */
    private Integer claimOneUnit(PrizeType prizeType) {
        List<Integer> counts = jdbc.query("""
                UPDATE lottery_prize_inventory
                SET awarded_count = awarded_count + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE prize_type = ? AND awarded_count < quota
                RETURNING awarded_count
                """, (rs, rowNum) -> rs.getInt(1), prizeType.name());
        return counts.isEmpty() ? null : counts.getFirst();
    }


    public DrawRecord findByUserId(long userId) {
        return findByUserId(userId, true);
    }

    private DrawRecord findByUserId(long userId, boolean alreadyDrawn) {
        try {
            return jdbc.queryForObject("""
                    SELECT id, user_id, prize_type, selected_prize_type, random_ticket, created_at
                    FROM lottery_draws WHERE user_id = ?
                    """, (rs, rowNum) -> mapRecord(rs, alreadyDrawn), userId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private DrawRecord mapRecord(java.sql.ResultSet rs, boolean alreadyDrawn) throws java.sql.SQLException {
        return new DrawRecord(rs.getLong("id"), rs.getLong("user_id"),
                PrizeType.valueOf(rs.getString("prize_type")),
                PrizeType.valueOf(rs.getString("selected_prize_type")),
                rs.getInt("random_ticket"), rs.getTimestamp("created_at").toInstant(), alreadyDrawn);
    }
}
