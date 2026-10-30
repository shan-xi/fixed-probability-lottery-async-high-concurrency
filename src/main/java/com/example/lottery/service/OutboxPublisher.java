package com.example.lottery.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Scheduler wrapper; transactional work lives in another bean so Spring's proxy is applied. */
@Component
public class OutboxPublisher {
    private final OutboxBatchPublisher batchPublisher;
    public OutboxPublisher(OutboxBatchPublisher batchPublisher) { this.batchPublisher = batchPublisher; }

    @Scheduled(fixedDelayString = "${lottery.outbox.poll-delay-ms:250}")
    public void publishPending() {
        try {
            batchPublisher.publishBatch();
        } catch (Exception e) {
            // Rows remain unpublished and will be retried on the next poll.
            System.err.println("Outbox publish batch failed; will retry: " + e.getMessage());
        }
    }
}
