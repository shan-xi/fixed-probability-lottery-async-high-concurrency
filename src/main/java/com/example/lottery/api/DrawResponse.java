package com.example.lottery.api;

import com.example.lottery.domain.DrawRecord;

import java.time.Instant;

/**
 * probabilityPercent describes the weighted candidate, not a quota-adjusted final outcome.
 */
public record DrawResponse(long id, long userId, String prizeType, String selectedPrizeType,
                           double probabilityPercent, int randomTicket, Instant createdAt,
                           boolean alreadyDrawn) {
    public static DrawResponse from(DrawRecord r) {
        return new DrawResponse(r.id(), r.userId(), r.prizeType().name(),
                r.selectedPrizeType().name(), r.selectedPrizeType().probabilityPercent(),
                r.randomTicket(), r.createdAt(), r.alreadyDrawn());
    }
}
