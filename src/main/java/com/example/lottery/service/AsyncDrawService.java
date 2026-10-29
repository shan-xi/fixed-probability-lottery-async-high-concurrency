package com.example.lottery.service;

import com.example.lottery.api.AsyncDrawResponse;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Service
public class AsyncDrawService {
    private final JdbcTemplate jdbc;

    public AsyncDrawService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Idempotently creates the draw and its outbox event in one PostgreSQL transaction.
     */
    @Transactional
    public Submission submit(long userId) {
        if (userId <= 0) throw new IllegalArgumentException("userId must be positive");
        List<AsyncDrawResponse> rows = jdbc.query("""
                INSERT INTO lottery_draws(user_id, prize_type, selected_prize_type, random_ticket, status)
                VALUES (?, 'NO_PRIZE', NULL, NULL, 'PENDING')
                ON CONFLICT (user_id) DO NOTHING
                RETURNING id, user_id, status, prize_type, selected_prize_type, random_ticket, created_at
                """, (rs, n) -> map(rs, false), userId);
        if (rows.isEmpty()) return new Submission(findByUserId(userId), true);

        AsyncDrawResponse created = rows.getFirst();
        jdbc.update("""
                INSERT INTO lottery_outbox(event_type, aggregate_id, payload)
                VALUES ('LOTTERY_DRAW_REQUESTED', ?, jsonb_build_object('drawId', ?))
                ON CONFLICT (event_type, aggregate_id) DO NOTHING
                """, created.drawId(), created.drawId());
        return new Submission(created, false);
    }

    public AsyncDrawResponse findByUserId(long userId) {
        return queryOne("SELECT id, user_id, status, prize_type, selected_prize_type, random_ticket, created_at FROM lottery_draws WHERE user_id = ?", userId);
    }

    public AsyncDrawResponse findByDrawId(long drawId) {
        return queryOne("SELECT id, user_id, status, prize_type, selected_prize_type, random_ticket, created_at FROM lottery_draws WHERE id = ?", drawId);
    }

    private AsyncDrawResponse queryOne(String sql, long value) {
        try {
            return jdbc.queryForObject(sql, (rs, n) -> map(rs, true), value);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private AsyncDrawResponse map(ResultSet rs, boolean alreadySubmitted) throws SQLException {
        String selected = rs.getString("selected_prize_type");
        Double probability = null;
        if (selected != null) probability = com.example.lottery.domain.PrizeType.valueOf(selected).probabilityPercent();
        int ticketValue = rs.getInt("random_ticket");
        Integer ticket = rs.wasNull() ? null : ticketValue;
        String status = rs.getString("status");
        String prize = "COMPLETED".equals(status) ? rs.getString("prize_type") : null;
        return new AsyncDrawResponse(rs.getLong("id"), rs.getLong("user_id"), status, prize, selected,
                probability, ticket, rs.getTimestamp("created_at").toInstant(), alreadySubmitted);
    }

    public record Submission(AsyncDrawResponse response, boolean alreadySubmitted) {
    }
}
