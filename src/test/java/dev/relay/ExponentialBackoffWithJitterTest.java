package dev.relay;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ExponentialBackoffWithJitterTest {

    private final BackoffStrategy backoff = new ExponentialBackoffWithJitter();

    @ParameterizedTest(name = "attempts={0} stays within ±25% of {1}s")
    @CsvSource({
            "1, 30",
            "2, 120",
            "3, 600",
            "4, 3600",
            "5, 21600",
            "6, 21600",
            "100, 21600",
    })
    void delayFallsWithinJitterBand(int attempts, long baseSeconds) {
        long baseMillis = Duration.ofSeconds(baseSeconds).toMillis();
        long lowerBound = Math.round(baseMillis * 0.75);
        long upperBound = Math.round(baseMillis * 1.25);

        for (int i = 0; i < 500; i++) {
            long delayMillis = backoff.nextDelay(attempts).toMillis();
            assertThat(delayMillis).isBetween(lowerBound, upperBound);
        }
    }
}
