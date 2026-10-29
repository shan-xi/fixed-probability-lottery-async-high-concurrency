package com.example.lottery.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WeightedPrizeSelectorTest {
    private final WeightedPrizeSelector selector = new WeightedPrizeSelector();

    @Test
    void probabilitiesMustSumTo100PercentExcludingFallbackOutcome() {
        assertEquals(10_000, PrizeType.configuredTotalBasisPoints());
        assertEquals(0, PrizeType.NO_PRIZE.probabilityBasisPoints());
    }

    @Test
    void mapsBoundaryTicketsToCorrectPrize() {
        assertEquals(PrizeType.PRIZE_1, selector.drawTicket(0).prizeType());
        assertEquals(PrizeType.PRIZE_1, selector.drawTicket(99).prizeType());
        assertEquals(PrizeType.PRIZE_2, selector.drawTicket(100).prizeType());
        assertEquals(PrizeType.PRIZE_2, selector.drawTicket(299).prizeType());
        assertEquals(PrizeType.PRIZE_3, selector.drawTicket(300).prizeType());
        assertEquals(PrizeType.PRIZE_10, selector.drawTicket(9999).prizeType());
    }

    @Test
    void rejectsOutOfRangeTickets() {
        assertThrows(IllegalArgumentException.class, () -> selector.drawTicket(-1));
        assertThrows(IllegalArgumentException.class, () -> selector.drawTicket(10_000));
    }

    @Test
    void ticketIntervalsHaveExactlyTheConfiguredSizes() {
        int start = 0;
        for (PrizeType prize : PrizeType.weightedPrizes()) {
            assertEquals(prize, selector.drawTicket(start).prizeType());
            assertEquals(prize, selector.drawTicket(start + prize.probabilityBasisPoints() - 1).prizeType());
            start += prize.probabilityBasisPoints();
        }
        assertEquals(10_000, start);
    }

    @Test
    void onlyFirstTwoPrizeTypesAreLimited() {
        assertTrue(PrizeType.PRIZE_1.isLimited());
        assertTrue(PrizeType.PRIZE_2.isLimited());
        assertFalse(PrizeType.PRIZE_3.isLimited());
        assertFalse(PrizeType.NO_PRIZE.isLimited());
    }
}
