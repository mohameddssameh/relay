package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;

public interface JobHandler {

    void handle(JsonNode payload);
}
