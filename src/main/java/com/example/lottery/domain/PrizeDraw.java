package com.example.lottery.domain;

/**
 * The random ticket and resulting prize for one draw.
 */
public record PrizeDraw(PrizeType prizeType, int randomTicket) {
}
