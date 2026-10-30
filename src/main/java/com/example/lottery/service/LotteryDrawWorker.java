package com.example.lottery.service;

import com.example.lottery.domain.PrizeDraw;
import com.example.lottery.domain.PrizeType;
import com.example.lottery.domain.WeightedPrizeSelector;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Idempotent worker: the database status transition is the deduplication guard.
 */
@Service
public class LotteryDrawWorker {
    private final JdbcTemplate jdbc;
    private final WeightedPrizeSelector selector;

    public LotteryDrawWorker(JdbcTemplate jdbc, WeightedPrizeSelector selector) {
        this.jdbc = jdbc;
        this.selector = selector;
    }

    @Transactional
    public void process(long drawId) {
        List<PendingDraw> rows = jdbc.query("""
                SELECT id FROM lottery_draws WHERE id = ? AND status = 'PENDING' FOR UPDATE
                """, (rs, n) -> new PendingDraw(rs.getLong(1)), drawId);
        if (rows.isEmpty()) return; // duplicate Kafka delivery or already completed

        PrizeDraw candidate = selector.draw();
        PrizeType finalPrize = candidate.prizeType();
        if (candidate.prizeType().isLimited() && !claimOneUnit(candidate.prizeType())) {
            finalPrize = PrizeType.NO_PRIZE;
        }
        jdbc.update("""
                UPDATE lottery_draws
                SET prize_type = ?, selected_prize_type = ?, random_ticket = ?, status = 'COMPLETED'
                WHERE id = ? AND status = 'PENDING'
                """, finalPrize.name(), candidate.prizeType().name(), candidate.randomTicket(), drawId);
    }

    private boolean claimOneUnit(PrizeType prizeType) {
        List<Integer> claimed = jdbc.query("""
                UPDATE lottery_prize_inventory
                SET awarded_count = awarded_count + 1, updated_at = CURRENT_TIMESTAMP
                WHERE prize_type = ? AND awarded_count < quota
                RETURNING awarded_count
                """, (rs, n) -> rs.getInt(1), prizeType.name());
        return !claimed.isEmpty();
    }

    private record PendingDraw(long id) {
    }
}
