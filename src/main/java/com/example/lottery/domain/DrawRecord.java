package com.example.lottery.domain;

import java.time.Instant;

/**
 * Persisted draw. prizeType is the final outcome; selectedPrizeType is the weighted candidate.
 */
public record DrawRecord(long id, long userId, PrizeType prizeType, PrizeType selectedPrizeType,
                         int randomTicket, Instant createdAt, boolean alreadyDrawn) {
}
