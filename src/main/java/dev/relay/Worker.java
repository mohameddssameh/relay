package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Component
public class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private static final String CLAIM_SQL = """
            SELECT id, type, payload
            FROM jobs
            WHERE status = 'queued' AND run_at <= now()
            ORDER BY run_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, JobHandler> handlersByType;

    public Worker(JdbcTemplate jdbcTemplate,
                  PlatformTransactionManager transactionManager,
                  ObjectMapper objectMapper,
                  Map<String, JobHandler> handlersByType) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.objectMapper = objectMapper;
        this.handlersByType = handlersByType;
    }

    @Scheduled(fixedDelay = 2000)
    public void pollAndRun() {
        claimNextJob().ifPresent(this::execute);
    }

    private Optional<ClaimedJob> claimNextJob() {
        return Optional.ofNullable(transactionTemplate.execute(status -> {
            List<ClaimedJob> rows = jdbcTemplate.query(CLAIM_SQL, Worker::mapRow);
            if (rows.isEmpty()) {
                return null;
            }
            ClaimedJob job = rows.get(0);
            jdbcTemplate.update("UPDATE jobs SET status = 'running' WHERE id = ?", job.id());
            return job;
        }));
    }

    private void execute(ClaimedJob job) {
        try {
            JobHandler handler = handlersByType.get(job.type());
            if (handler == null) {
                throw new IllegalStateException("No handler registered for job type '" + job.type() + "'");
            }
            JsonNode payload = objectMapper.readTree(job.payloadJson());
            handler.handle(payload);
            jdbcTemplate.update("UPDATE jobs SET status = 'completed' WHERE id = ?", job.id());
        } catch (Exception e) {
            log.error("Job {} of type '{}' failed", job.id(), job.type(), e);
            jdbcTemplate.update("UPDATE jobs SET status = 'failed' WHERE id = ?", job.id());
        }
    }

    private static ClaimedJob mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedJob((UUID) rs.getObject("id"), rs.getString("type"), rs.getString("payload"));
    }

    private record ClaimedJob(UUID id, String type, String payloadJson) {
    }
}
