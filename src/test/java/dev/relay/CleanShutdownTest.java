package dev.relay;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the real Spring lifecycle - WorkerFleet's actual @PreDestroy - rather than calling
 * Worker.shutdown() directly, since that's what the test name is actually asserting about.
 */
@SpringBootTest(properties = "relay.worker.instances=1")
@Testcontainers
@DirtiesContext
class CleanShutdownTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private WorkerFleet workerFleet;

    @Test
    void workersRowIsDeletedOnPreDestroy() {
        UUID workerId = workerFleet.getWorkers().get(0).getWorkerId();

        Integer beforeCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM workers WHERE id = ?", Integer.class, workerId);
        assertThat(beforeCount).isEqualTo(1);

        // Destroy just the workerFleet singleton through Spring's real bean-destruction
        // machinery (which is what actually invokes @PreDestroy) instead of closing the whole
        // context - closing the context also tears down the Testcontainers-backed DataSource
        // and container itself, leaving nothing left to verify against afterward.
        ((DefaultListableBeanFactory) applicationContext.getBeanFactory()).destroySingleton("workerFleet");

        Integer afterCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM workers WHERE id = ?", Integer.class, workerId);
        assertThat(afterCount).isZero();
    }
}
