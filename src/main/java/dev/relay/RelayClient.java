package dev.relay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

@Component
public class RelayClient {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public RelayClient(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void enqueue(String type, Object payload) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Payload for job type '" + type + "' could not be serialized to JSON", e);
        }

        Timestamp now = Timestamp.from(Instant.now());
        jdbcTemplate.update(
                """
                INSERT INTO jobs (id, type, payload, status, created_at, run_at)
                VALUES (?, ?, ?::jsonb, 'queued', ?, ?)
                """,
                UUID.randomUUID(), type, payloadJson, now, now);
    }
}
