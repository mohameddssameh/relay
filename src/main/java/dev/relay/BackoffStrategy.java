package dev.relay;

import java.time.Duration;

public interface BackoffStrategy {

    Duration nextDelay(int attempts);
}
