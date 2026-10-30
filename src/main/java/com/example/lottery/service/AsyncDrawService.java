package com.example.lottery.service;

import com.example.lottery.api.AsyncDrawResponse;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

@Service
public class AsyncDrawService {
    private final JdbcTemplate jdbc;

    public AsyncDrawService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Creates the current month's draw and its outbox event atomically. The API does not wait
     * for Kafka or the lottery worker; clients follow statusUrl until the result is ready.
     */
    @Transactional
    public Submission submit(long userId) {
        if (userId <= 0) throw new IllegalArgumentException("userId must be positive");
        List<AsyncDrawResponse> rows = jdbc.query("""
                INSERT INTO lottery_draws(campaign_month, user_id, prize_type, selected_prize_type, random_ticket, status)
                VALUES (date_trunc('month', CURRENT_TIMESTAMP)::date, ?, 'NO_PRIZE', NULL, NULL, 'PENDING')
                ON CONFLICT (campaign_month, user_id) DO NOTHING
                RETURNING id, user_id, campaign_month, status, prize_type, selected_prize_type, random_ticket, created_at
                """, (rs, n) -> map(rs, false), userId);
        if (rows.isEmpty()) return new Submission(findByCurrentCampaignUser(userId), true);

        AsyncDrawResponse created = rows.getFirst();
        jdbc.update("""
                INSERT INTO lottery_outbox(event_type, aggregate_id, campaign_month, payload)
                VALUES ('LOTTERY_DRAW_REQUESTED', ?, CAST(? AS DATE), jsonb_build_object('drawId', ?, 'campaignMonth', ?))
                ON CONFLICT (event_type, aggregate_id) DO NOTHING
                """, created.drawId(), created.campaignMonth(), created.drawId(), created.campaignMonth());
        return new Submission(created, false);
    }

    public AsyncDrawResponse findByCurrentCampaignUser(long userId) {
        return queryOne("""
                SELECT id, user_id, campaign_month, status, prize_type, selected_prize_type, random_ticket, created_at
                FROM lottery_draws
                WHERE campaign_month = date_trunc('month', CURRENT_TIMESTAMP)::date AND user_id = ?
                """, userId);
    }

    public AsyncDrawResponse findByUserId(long userId) {
        return findByCurrentCampaignUser(userId);
    }

    public AsyncDrawResponse findByDrawId(long drawId) {
        return queryOne("""
                SELECT id, user_id, campaign_month, status, prize_type, selected_prize_type, random_ticket, created_at
                FROM lottery_draws WHERE id = ?
                """, drawId);
    }

    public AsyncDrawResponse findByDrawId(LocalDate campaignMonth, long drawId) {
        try {
            return jdbc.queryForObject("""
                    SELECT id, user_id, campaign_month, status, prize_type, selected_prize_type, random_ticket, created_at
                    FROM lottery_draws
                    WHERE campaign_month = ? AND id = ?
                    """, (rs, n) -> map(rs, true), campaignMonth, drawId);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
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
        long drawId = rs.getLong("id");
        String campaignMonth = rs.getDate("campaign_month").toLocalDate().toString();
        return new AsyncDrawResponse(drawId, rs.getLong("user_id"), campaignMonth,
                status, "/api/lottery/draws/" + campaignMonth + "/id/" + drawId, prize, selected, probability, ticket,
                rs.getTimestamp("created_at").toInstant(), alreadySubmitted);
    }

    public record Submission(AsyncDrawResponse response, boolean alreadySubmitted) {
    }
}
