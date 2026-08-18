package dev.relay;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
public class ExponentialBackoffWithJitter implements BackoffStrategy {

    private static final List<Duration> BASE_SCHEDULE = List.of(
            Duration.ofSeconds(30),
            Duration.ofMinutes(2),
            Duration.ofMinutes(10),
            Duration.ofHours(1),
            Duration.ofHours(6));

    private static final double JITTER_FRACTION = 0.25;

    @Override
    public Duration nextDelay(int attempts) {
        int index = Math.min(attempts, BASE_SCHEDULE.size()) - 1;
        Duration base = BASE_SCHEDULE.get(Math.max(index, 0));

        double jitterFactor = 1 + ThreadLocalRandom.current().nextDouble(-JITTER_FRACTION, JITTER_FRACTION);
        long jitteredMillis = Math.round(base.toMillis() * jitterFactor);
        return Duration.ofMillis(jitteredMillis);
    }
}
