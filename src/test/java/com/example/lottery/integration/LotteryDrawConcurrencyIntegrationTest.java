package com.example.lottery.integration;

import com.example.lottery.LotteryApplication;
import com.example.lottery.domain.DrawRecord;
import com.example.lottery.service.DrawService;
import com.example.lottery.service.AsyncDrawService;
import com.example.lottery.service.LotteryDrawWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest(classes = LotteryApplication.class)
class LotteryDrawConcurrencyIntegrationTest {
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("lottery").withUsername("lottery").withPassword("lottery");

    @DynamicPropertySource static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("lottery.outbox.poll-delay-ms", () -> "3600000");
    }

    @Autowired DrawService service;
    @Autowired AsyncDrawService asyncDrawService;
    @Autowired LotteryDrawWorker worker;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void resetDatabase() {
        jdbc.execute("TRUNCATE TABLE lottery_outbox, lottery_draws RESTART IDENTITY CASCADE");
        jdbc.update("UPDATE lottery_prize_inventory SET quota=1, awarded_count=0 WHERE prize_type='PRIZE_1'");
        jdbc.update("UPDATE lottery_prize_inventory SET quota=10, awarded_count=0 WHERE prize_type='PRIZE_2'");
    }

    @Test void concurrentDuplicateRequestsReturnOnePersistentResultAndConsumeAtMostOneQuota() throws Exception {
        int calls = 100;
        ExecutorService pool = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<DrawRecord>> futures = new ArrayList<>();
            for (int i = 0; i < calls; i++) {
                futures.add(pool.submit(() -> { start.await(); return service.draw(90001); }));
            }
            start.countDown();
            List<DrawRecord> results = new ArrayList<>();
            for (Future<DrawRecord> f : futures) results.add(f.get(30, TimeUnit.SECONDS));
            assertEquals(1, results.stream().map(DrawRecord::id).distinct().count());
            assertEquals(1, results.stream().map(DrawRecord::prizeType).distinct().count());
            assertNotNull(service.findByUserId(90001));
            assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM lottery_draws WHERE user_id=90001", Integer.class));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void limitedPrizeQuotasAreNeverExceededUnderConcurrentDraws() throws Exception {
        int users = 2_000;
        ExecutorService pool = Executors.newFixedThreadPool(40);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<DrawRecord>> futures = new ArrayList<>();
            for (int i = 0; i < users; i++) {
                long userId = 200_000L + i;
                futures.add(pool.submit(() -> { start.await(); return service.draw(userId); }));
            }
            start.countDown();
            for (Future<DrawRecord> f : futures) assertNotNull(f.get(90, TimeUnit.SECONDS));

            assertQuotaMatchesAwardedDraws("PRIZE_1", 1);
            assertQuotaMatchesAwardedDraws("PRIZE_2", 10);
            assertEquals(users, jdbc.queryForObject("SELECT COUNT(*) FROM lottery_draws", Integer.class));
            assertEquals(0, jdbc.queryForObject("""
                    SELECT COUNT(*) FROM lottery_draws d
                    JOIN lottery_prize_inventory i ON i.prize_type=d.prize_type
                    WHERE d.prize_type IN ('PRIZE_1','PRIZE_2') AND i.awarded_count > i.quota
                    """, Integer.class));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test void exhaustedLimitedPrizeIsRecordedAsNoPrizeAndCandidateIsAuditable() {
        jdbc.update("UPDATE lottery_prize_inventory SET quota=0, awarded_count=0 WHERE prize_type IN ('PRIZE_1','PRIZE_2')");
        // A moderate number of draws makes it overwhelmingly likely that both limited ticket ranges are sampled.
        for (long userId = 300_000; userId < 302_000; userId++) service.draw(userId);
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM lottery_draws WHERE prize_type IN ('PRIZE_1','PRIZE_2')", Integer.class));
        assertTrue(jdbc.queryForObject("""
                SELECT COUNT(*) FROM lottery_draws
                WHERE selected_prize_type IN ('PRIZE_1','PRIZE_2') AND prize_type='NO_PRIZE'
                """, Integer.class) > 0);
    }


    @Test void asyncSubmissionAndOutboxAreAtomicAndIdempotent() {
        var first = asyncDrawService.submit(70001);
        var duplicate = asyncDrawService.submit(70001);

        assertFalse(first.alreadySubmitted());
        assertEquals("PENDING", first.response().status());
        assertTrue(duplicate.alreadySubmitted());
        assertEquals(first.response().drawId(), duplicate.response().drawId());
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM lottery_draws WHERE user_id=70001", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM lottery_outbox WHERE aggregate_id=?", Integer.class, first.response().drawId()));

        worker.process(first.response().drawId());
        assertEquals("COMPLETED", asyncDrawService.findByDrawId(first.response().drawId()).status());
        worker.process(first.response().drawId()); // duplicate event must not process/award again
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM lottery_draws WHERE user_id=70001", Integer.class));
    }

    private void assertQuotaMatchesAwardedDraws(String prizeType, int expectedQuota) {
        Integer quota = jdbc.queryForObject("SELECT quota FROM lottery_prize_inventory WHERE prize_type=?", Integer.class, prizeType);
        Integer awarded = jdbc.queryForObject("SELECT awarded_count FROM lottery_prize_inventory WHERE prize_type=?", Integer.class, prizeType);
        Integer actualWins = jdbc.queryForObject("SELECT COUNT(*) FROM lottery_draws WHERE prize_type=?", Integer.class, prizeType);
        assertEquals(expectedQuota, quota);
        assertTrue(awarded >= 0 && awarded <= quota, prizeType + " exceeded quota");
        assertEquals(actualWins, awarded, prizeType + " inventory count must match persisted wins");
    }
}
