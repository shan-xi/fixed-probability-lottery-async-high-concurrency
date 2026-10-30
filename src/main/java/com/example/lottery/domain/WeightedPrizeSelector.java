package com.example.lottery.domain;

import java.util.concurrent.ThreadLocalRandom;

/** Stateless weighted selection. Inventory is deliberately handled separately by DrawService. */
public class WeightedPrizeSelector {
    public PrizeDraw draw() {
        return drawTicket(ThreadLocalRandom.current().nextInt(PrizeType.TOTAL_BASIS_POINTS));
    }

    /** Public visibility makes exact probability boundaries easy to unit-test. */
    public PrizeDraw drawTicket(int ticket) {
        if (ticket < 0 || ticket >= PrizeType.TOTAL_BASIS_POINTS) {
            throw new IllegalArgumentException("ticket must be between 0 and 9999");
        }
        int cumulative = 0;
        for (PrizeType prize : PrizeType.weightedPrizes()) {
            cumulative += prize.probabilityBasisPoints();
            if (ticket < cumulative) return new PrizeDraw(prize, ticket);
        }
        throw new IllegalStateException("Probability configuration does not cover ticket " + ticket);
    }
}
