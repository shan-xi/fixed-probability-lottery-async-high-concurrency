package com.example.lottery.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/** Claims a batch in a short transaction. Kafka I/O happens only after this transaction commits. */
@Service
public class OutboxBatchClaimer {
    private final JdbcTemplate jdbc;
    private final int batchSize;
    private final int leaseSeconds;

    public OutboxBatchClaimer(JdbcTemplate jdbc,
            @Value("${lottery.outbox.batch-size:100}") int batchSize,
            @Value("${lottery.outbox.lease-seconds:60}") int leaseSeconds) {
        this.jdbc = jdbc;
        this.batchSize = batchSize;
        this.leaseSeconds = leaseSeconds;
    }

    @Transactional
    public List<ClaimedOutboxEvent> claimBatch() {
        String claimToken = UUID.randomUUID().toString();
        return jdbc.query("""
                WITH candidates AS (
                    SELECT id
                    FROM lottery_outbox
                    WHERE published_at IS NULL
                      AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP)
                    ORDER BY id
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE lottery_outbox o
                SET claim_token = ?,
                    locked_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'),
                    publish_attempts = publish_attempts + 1
                FROM candidates c
                WHERE o.id = c.id
                RETURNING o.id, o.aggregate_id, o.payload::text AS payload
                """, (rs, rowNum) -> new ClaimedOutboxEvent(
                    rs.getLong("id"), rs.getLong("aggregate_id"), rs.getString("payload"), claimToken),
                batchSize, claimToken, leaseSeconds);
    }

    public record ClaimedOutboxEvent(long id, long drawId, String payload, String claimToken) { }
}
