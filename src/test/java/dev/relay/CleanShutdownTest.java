package dev.relay;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    void workersRowIsDeletedOnPreDestroy() throws SQLException {
        UUID workerId = workerFleet.getWorkers().get(0).getWorkerId();

        Integer beforeCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM workers WHERE id = ?", Integer.class, workerId);
        assertThat(beforeCount).isEqualTo(1);

        // Closing the context shuts down the DataSource too, so jdbcTemplate is unusable
        // afterward - verify through a fresh, independent JDBC connection instead.
        applicationContext.close();

        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM workers WHERE id = ?")) {
            statement.setObject(1, workerId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                assertThat(resultSet.getInt(1)).isZero();
            }
        }
    }
}
