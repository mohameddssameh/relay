package dev.relay;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.time.Duration;

public class TestBackoffStrategy implements BackoffStrategy {

    @Override
    public Duration nextDelay(int attempts) {
        return Duration.ofMillis(50);
    }

    @TestConfiguration
    public static class Config {

        @Bean
        @Primary
        public BackoffStrategy testBackoffStrategy() {
            return new TestBackoffStrategy();
        }
    }
}
