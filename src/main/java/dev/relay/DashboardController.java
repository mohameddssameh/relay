package dev.relay;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class DashboardController {

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
}
