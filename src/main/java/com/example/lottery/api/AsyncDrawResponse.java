package com.example.lottery.api;

import java.time.Instant;

/**
 * The prize fields remain null while status is PENDING.
 */
public record AsyncDrawResponse(long drawId, long userId, String status, String prizeType,
                                String selectedPrizeType, Double probabilityPercent, Integer randomTicket,
                                Instant createdAt, boolean alreadySubmitted) {
}
