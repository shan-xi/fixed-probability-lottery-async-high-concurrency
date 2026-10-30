package com.example.lottery.service;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Publishes claimed events without holding PostgreSQL row locks during Kafka network I/O.
 */
@Service
public class OutboxBatchPublisher {
    private final OutboxBatchClaimer claimer;
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private final int leaseSeconds;
    private final MeterRegistry meterRegistry;

    public OutboxBatchPublisher(OutboxBatchClaimer claimer, JdbcTemplate jdbc,
                                KafkaTemplate<String, String> kafka,
                                @Value("${lottery.kafka.topic}") String topic,
                                @Value("${lottery.outbox.lease-seconds:60}") int leaseSeconds,
                                MeterRegistry meterRegistry) {
        this.claimer = claimer;
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.topic = topic;
        this.leaseSeconds = leaseSeconds;
        this.meterRegistry = meterRegistry;
    }

    public void publishBatch() {
        List<OutboxBatchClaimer.ClaimedOutboxEvent> events = claimer.claimBatch();
        meterRegistry.counter("lottery.outbox.claimed").increment(events.size());
        for (var event : events) {
            try {
                // Renew the remaining rows in this batch before each bounded Kafka wait.
                jdbc.update("""
                        UPDATE lottery_outbox
                        SET locked_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second')
                        WHERE claim_token = ? AND published_at IS NULL
                        """, leaseSeconds, event.claimToken());
                kafka.send(topic, Long.toString(event.drawId()), event.payload()).get(10, TimeUnit.SECONDS);
                int updated = jdbc.update("""
                        UPDATE lottery_outbox
                        SET published_at = CURRENT_TIMESTAMP,
                            locked_until = NULL,
                            claim_token = NULL,
                            last_error = NULL
                        WHERE id = ? AND claim_token = ? AND published_at IS NULL
                        """, event.id(), event.claimToken());
                if (updated == 1) meterRegistry.counter("lottery.outbox.published").increment();
            } catch (Exception ex) {
                // Clear our lease so a later poll can retry. A crash before this update is
                // recovered automatically when locked_until expires. Duplicate delivery is expected.
                jdbc.update("""
                        UPDATE lottery_outbox
                        SET locked_until = NULL,
                            claim_token = NULL,
                            last_error = LEFT(?, 2000)
                        WHERE id = ? AND claim_token = ? AND published_at IS NULL
                        """, safeMessage(ex), event.id(), event.claimToken());
                meterRegistry.counter("lottery.outbox.publish.failures").increment();
            }
        }
    }

    private String safeMessage(Exception ex) {
        String message = ex.getMessage();
        return ex.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
