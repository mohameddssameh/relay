package dev.relay;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "relay.worker.instances=0")
@Testcontainers
class DashboardTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void dashboardLandingPageReturns200WithExpectedContent() {
        ResponseEntity<String> response = restTemplate.getForEntity("/dashboard", String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).contains("Relay").contains("queued").contains("active workers");
    }

    @Test
    void jobsPageReturns200() {
        ResponseEntity<String> response = restTemplate.getForEntity("/dashboard/jobs", String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).contains("Jobs");
    }

    @Test
    void deadLetterPageReturns200() {
        ResponseEntity<String> response = restTemplate.getForEntity("/dashboard/dead-letter", String.class);

        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody()).contains("Dead letter");
    }
}
