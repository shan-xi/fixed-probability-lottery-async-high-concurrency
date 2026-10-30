package com.example.lottery.api;

import java.time.Instant;

/**
 * Async API resource. A client can submit a draw, receive 202 + a status URL,
 * and poll the resource until it becomes COMPLETED or FAILED.
 */
public record AsyncDrawResponse(long drawId, long userId, String campaignMonth, String status,
                                String statusUrl, String prizeType, String selectedPrizeType,
                                Double probabilityPercent, Integer randomTicket, Instant createdAt,
                                boolean alreadySubmitted) {
}
