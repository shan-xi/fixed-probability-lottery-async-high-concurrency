package com.example.lottery.domain;

import java.util.Arrays;
import java.util.List;

/** Fixed probabilities are expressed in basis points: 100 basis points = 1%. */
public enum PrizeType {
    PRIZE_1(100),    // 1%, limited quota
    PRIZE_2(200),    // 2%, limited quota
    PRIZE_3(500),    // 5%, unlimited
    PRIZE_4(700),    // 7%, unlimited
    PRIZE_5(1000),   // 10%, unlimited
    PRIZE_6(1200),   // 12%, unlimited
    PRIZE_7(1300),   // 13%, unlimited
    PRIZE_8(1500),   // 15%, unlimited
    PRIZE_9(1500),   // 15%, unlimited
    PRIZE_10(2000),  // 20%, unlimited
    NO_PRIZE(0);     // Final outcome when PRIZE_1/PRIZE_2 is selected but sold out.

    public static final int TOTAL_BASIS_POINTS = 10_000;
    private static final List<PrizeType> WEIGHTED_PRIZES = Arrays.stream(values())
            .filter(PrizeType::isWeightedPrize).toList();

    private final int probabilityBasisPoints;

    PrizeType(int probabilityBasisPoints) {
        this.probabilityBasisPoints = probabilityBasisPoints;
    }

    public int probabilityBasisPoints() { return probabilityBasisPoints; }
    public double probabilityPercent() { return probabilityBasisPoints / 100.0; }
    public boolean isWeightedPrize() { return this != NO_PRIZE; }
    public boolean isLimited() { return this == PRIZE_1 || this == PRIZE_2; }
    public static List<PrizeType> weightedPrizes() { return WEIGHTED_PRIZES; }

    public static int configuredTotalBasisPoints() {
        return WEIGHTED_PRIZES.stream().mapToInt(PrizeType::probabilityBasisPoints).sum();
    }

    static {
        int total = configuredTotalBasisPoints();
        if (total != TOTAL_BASIS_POINTS) {
            throw new IllegalStateException("Prize probabilities must total 10000 basis points, got " + total);
        }
        if (WEIGHTED_PRIZES.stream().anyMatch(p -> p.probabilityBasisPoints <= 0)) {
            throw new IllegalStateException("Every weighted prize probability must be positive");
        }
    }
}
