package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component("send_email")
public class SendEmailJob implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(SendEmailJob.class);

    @Override
    public void handle(JsonNode payload) {
        String to = payload.path("to").asText("unknown");
        log.info("would send email to {}", to);
    }
}
