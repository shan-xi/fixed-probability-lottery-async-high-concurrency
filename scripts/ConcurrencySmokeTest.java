import com.example.lottery.domain.PrizeDraw;
import com.example.lottery.domain.PrizeType;
import com.example.lottery.domain.WeightedPrizeSelector;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Dependency-free local stress test for the exact production prize-selection classes.
 */
public class ConcurrencySmokeTest {
    public static void main(String[] args) throws Exception {
        final int draws = args.length > 0 ? Integer.parseInt(args[0]) : 100_000;
        final int workers = args.length > 1 ? Integer.parseInt(args[1]) : 64;
        WeightedPrizeSelector selector = new WeightedPrizeSelector();
        Map<PrizeType, LongAdder> counts = new EnumMap<>(PrizeType.class);
        for (PrizeType p : PrizeType.weightedPrizes()) counts.put(p, new LongAdder());
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        int perWorker = draws / workers;
        int remainder = draws % workers;
        long started = System.nanoTime();
        try {
            Future<?>[] futures = new Future<?>[workers];
            for (int w = 0; w < workers; w++) {
                final int n = perWorker + (w < remainder ? 1 : 0);
                futures[w] = pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < n; i++) {
                        PrizeDraw result = selector.draw();
                        counts.get(result.prizeType()).increment();
                    }
                    return null;
                });
            }
            start.countDown();
            for (Future<?> future : futures) future.get();
        } finally {
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.MINUTES);
        }
        double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
        long total = counts.values().stream().mapToLong(LongAdder::sum).sum();
        System.out.printf("Completed %,d independent draws using %d threads in %.3f seconds (%.0f draws/sec)%n",
                total, workers, seconds, total / seconds);
        System.out.println("Prize       configured%   observed%   count");
        for (PrizeType p : PrizeType.weightedPrizes()) {
            long count = counts.get(p).sum();
            System.out.printf("%-10s %8.2f%% %10.3f%% %,10d%n", p,
                    p.probabilityPercent(), count * 100.0 / total, count);
        }
        if (total != draws) throw new AssertionError("Lost draws: expected " + draws + " got " + total);
        System.out.println("PASS: all draws completed; no shared mutable selector state or allocation lock.");
        System.out.println("NOTE: this is an algorithm-level concurrency smoke test, not an HTTP/PostgreSQL load test.");
    }
}
