package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;

public interface JobHandler {

    void handle(JsonNode payload);

    default int maxAttempts() {
        return 5;
    }

    default Duration timeout() {
        return Duration.ofSeconds(30);
    }
}
