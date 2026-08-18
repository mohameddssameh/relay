package dev.relay;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Controller
public class DashboardController {

    private static final int LIST_LIMIT = 50;

    // Matches the same 30s staleness threshold reclaimStaleWorkers() uses, so a worker
    // about to be reclaimed doesn't inflate the "active" count.
    private static final String ACTIVE_WORKERS_SQL =
            "SELECT COUNT(*) FROM workers WHERE last_heartbeat_at >= now() - interval '30 seconds'";

    private final JdbcTemplate jdbcTemplate;

    public DashboardController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @GetMapping("/dashboard")
    public String index(Model model) {
        addCounts(model);
        return "dashboard/index";
    }

    @GetMapping("/dashboard/counts")
    public String counts(Model model) {
        addCounts(model);
        return "dashboard/index :: countsFragment";
    }

    @GetMapping("/dashboard/jobs")
    public String jobs(@RequestParam(required = false) String status, Model model) {
        List<JobSummary> jobs = (status == null || status.isBlank())
                ? jdbcTemplate.query(
                        "SELECT id, type, status, attempts, created_at, last_error FROM jobs "
                                + "ORDER BY created_at DESC LIMIT ?",
                        DashboardController::mapJobSummary, LIST_LIMIT)
                : jdbcTemplate.query(
                        "SELECT id, type, status, attempts, created_at, last_error FROM jobs "
                                + "WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                        DashboardController::mapJobSummary, status, LIST_LIMIT);
        model.addAttribute("jobs", jobs);
        model.addAttribute("statusFilter", status);
        return "dashboard/jobs";
    }

    @GetMapping("/dashboard/jobs/{id}")
    public String jobDetail(@PathVariable UUID id, Model model) {
        List<JobDetail> matches = jdbcTemplate.query(
                "SELECT id, type, payload::text AS payload, status, attempts, created_at, run_at, "
                        + "last_error, last_failed_at, worker_id FROM jobs WHERE id = ?",
                DashboardController::mapJobDetail, id);
        model.addAttribute("job", matches.isEmpty() ? null : matches.get(0));
        return "dashboard/job-detail";
    }

    @GetMapping("/dashboard/dead-letter")
    public String deadLetter(Model model) {
        List<DeadLetterSummary> deadLetterJobs = jdbcTemplate.query(
                "SELECT id, type, attempts, dead_lettered_at, last_error FROM dead_letter_jobs "
                        + "ORDER BY dead_lettered_at DESC LIMIT ?",
                DashboardController::mapDeadLetterSummary, LIST_LIMIT);
        model.addAttribute("deadLetterJobs", deadLetterJobs);
        return "dashboard/dead-letter";
    }

    // Runs as one transaction (unlike Worker, this class IS a Spring bean, so @Transactional's
    // AOP proxying works normally here - no self-invocation concern).
    @PostMapping("/dashboard/dead-letter/{id}/retry")
    @Transactional
    @ResponseBody
    public String retry(@PathVariable UUID id) {
        jdbcTemplate.update(
                """
                INSERT INTO jobs (id, type, payload, status, created_at, run_at, attempts)
                SELECT id, type, payload, 'queued', created_at, now(), 0
                FROM dead_letter_jobs WHERE id = ?
                """,
                id);
        jdbcTemplate.update("DELETE FROM dead_letter_jobs WHERE id = ?", id);
        return "";
    }

    @PostMapping("/dashboard/dead-letter/{id}/delete")
    @ResponseBody
    public String delete(@PathVariable UUID id) {
        jdbcTemplate.update("DELETE FROM dead_letter_jobs WHERE id = ?", id);
        return "";
    }

    private void addCounts(Model model) {
        model.addAttribute("queuedCount", count("SELECT COUNT(*) FROM jobs WHERE status = 'queued'"));
        model.addAttribute("runningCount", count("SELECT COUNT(*) FROM jobs WHERE status = 'running'"));
        model.addAttribute("completedCount", count(
                "SELECT COUNT(*) FROM jobs WHERE status = 'completed' AND created_at >= now() - interval '24 hours'"));
        model.addAttribute("deadLetterCount", count("SELECT COUNT(*) FROM dead_letter_jobs"));
        model.addAttribute("activeWorkerCount", count(ACTIVE_WORKERS_SQL));
    }

    private int count(String sql) {
        Integer result = jdbcTemplate.queryForObject(sql, Integer.class);
        return result != null ? result : 0;
    }

    private static JobSummary mapJobSummary(ResultSet rs, int rowNum) throws SQLException {
        return new JobSummary(
                (UUID) rs.getObject("id"),
                rs.getString("type"),
                rs.getString("status"),
                rs.getInt("attempts"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getString("last_error"));
    }

    private static JobDetail mapJobDetail(ResultSet rs, int rowNum) throws SQLException {
        Timestamp lastFailedAtTs = rs.getTimestamp("last_failed_at");
        return new JobDetail(
                (UUID) rs.getObject("id"),
                rs.getString("type"),
                rs.getString("status"),
                rs.getInt("attempts"),
                rs.getString("payload"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("run_at").toInstant(),
                rs.getString("last_error"),
                lastFailedAtTs != null ? lastFailedAtTs.toInstant() : null,
                (UUID) rs.getObject("worker_id"));
    }

    record JobSummary(UUID id, String type, String status, int attempts, Instant createdAt, String lastError) {

        private static final int PREVIEW_LENGTH = 80;

        public String lastErrorPreview() {
            if (lastError == null) {
                return "";
            }
            return lastError.length() > PREVIEW_LENGTH ? lastError.substring(0, PREVIEW_LENGTH) + "..." : lastError;
        }
    }

    record JobDetail(UUID id, String type, String status, int attempts, String payload, Instant createdAt,
                      Instant runAt, String lastError, Instant lastFailedAt, UUID workerId) {
    }

    private static DeadLetterSummary mapDeadLetterSummary(ResultSet rs, int rowNum) throws SQLException {
        return new DeadLetterSummary(
                (UUID) rs.getObject("id"),
                rs.getString("type"),
                rs.getInt("attempts"),
                rs.getTimestamp("dead_lettered_at").toInstant(),
                rs.getString("last_error"));
    }

    record DeadLetterSummary(UUID id, String type, int attempts, Instant deadLetteredAt, String lastError) {

        private static final int PREVIEW_LENGTH = 80;

        public String lastErrorPreview() {
            if (lastError == null) {
                return "";
            }
            return lastError.length() > PREVIEW_LENGTH ? lastError.substring(0, PREVIEW_LENGTH) + "..." : lastError;
        }
    }
}
