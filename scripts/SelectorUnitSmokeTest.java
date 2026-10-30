import com.example.lottery.domain.PrizeType;
import com.example.lottery.domain.WeightedPrizeSelector;

/** Dependency-free assertions for selector behavior; complements the JUnit tests in src/test. */
public class SelectorUnitSmokeTest {
    private static void eq(Object expected, Object actual, String message) {
        if (!expected.equals(actual)) throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
    }
    private static void throwsIllegalArgument(Runnable action) {
        try { action.run(); } catch (IllegalArgumentException expected) { return; }
        throw new AssertionError("Expected IllegalArgumentException");
    }
    public static void main(String[] args) {
        WeightedPrizeSelector selector = new WeightedPrizeSelector();
        eq(10_000, PrizeType.configuredTotalBasisPoints(), "probability total");
        eq(PrizeType.PRIZE_1, selector.drawTicket(0).prizeType(), "first ticket");
        eq(PrizeType.PRIZE_1, selector.drawTicket(99).prizeType(), "last ticket of prize 1");
        eq(PrizeType.PRIZE_2, selector.drawTicket(100).prizeType(), "first ticket of prize 2");
        eq(PrizeType.PRIZE_2, selector.drawTicket(299).prizeType(), "last ticket of prize 2");
        eq(PrizeType.PRIZE_3, selector.drawTicket(300).prizeType(), "first ticket of prize 3");
        eq(PrizeType.PRIZE_10, selector.drawTicket(9999).prizeType(), "last ticket");
        throwsIllegalArgument(() -> selector.drawTicket(-1));
        throwsIllegalArgument(() -> selector.drawTicket(10000));
        System.out.println("PASS: probability sum, interval boundaries, and invalid-ticket checks.");
    }
}
