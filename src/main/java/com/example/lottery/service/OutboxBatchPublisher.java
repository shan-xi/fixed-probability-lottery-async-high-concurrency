package com.example.lottery.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Publishes a locked batch in a DB transaction. Re-publication after a crash is safe by design.
 */
@Service
public class OutboxBatchPublisher {
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private final int batchSize;

    public OutboxBatchPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka,
                                @Value("${lottery.kafka.topic}") String topic,
                                @Value("${lottery.outbox.batch-size:100}") int batchSize) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.topic = topic;
        this.batchSize = batchSize;
    }

    @Transactional
    public void publishBatch() throws Exception {
        List<OutboxRow> rows = jdbc.query("""
                SELECT id, aggregate_id, payload::text AS payload
                FROM lottery_outbox WHERE published_at IS NULL
                ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED
                """, (rs, n) -> new OutboxRow(rs.getLong("id"), rs.getLong("aggregate_id"), rs.getString("payload")), batchSize);
        for (OutboxRow row : rows) {
            // Wait for broker acknowledgement before marking the outbox row published.
            // A crash between these operations can cause duplicate delivery; the worker is idempotent.
            kafka.send(topic, Long.toString(row.drawId()), row.payload()).get(10, TimeUnit.SECONDS);
            jdbc.update("UPDATE lottery_outbox SET published_at=CURRENT_TIMESTAMP, publish_attempts=publish_attempts+1, last_error=NULL WHERE id=?", row.id());
        }
    }

    private record OutboxRow(long id, long drawId, String payload) {
    }
}
