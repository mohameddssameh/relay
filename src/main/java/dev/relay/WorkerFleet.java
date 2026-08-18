package dev.relay;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;

/**
 * Owns and schedules the fleet of {@link Worker} instances. The only Spring-managed piece of
 * the worker subsystem — Worker itself is a plain class so it stays directly instantiable in
 * tests without a Spring context.
 */
@Component
public class WorkerFleet {

    private static final Logger log = LoggerFactory.getLogger(WorkerFleet.class);

    private final JdbcTemplate jdbcTemplate;
    private final PlatformTransactionManager transactionManager;
    private final ObjectMapper objectMapper;
    private final Map<String, JobHandler> handlersByType;
    private final BackoffStrategy backoffStrategy;
    private final int poolSize;
    private final int instanceCount;

    private final List<Worker> workers = new CopyOnWriteArrayList<>();
    private final Map<UUID, List<ScheduledFuture<?>>> schedulesByWorkerId = new ConcurrentHashMap<>();
    private ThreadPoolTaskScheduler taskScheduler;

    public WorkerFleet(JdbcTemplate jdbcTemplate,
                        PlatformTransactionManager transactionManager,
                        ObjectMapper objectMapper,
                        Map<String, JobHandler> handlersByType,
                        BackoffStrategy backoffStrategy,
                        @Value("${relay.worker.pool-size:4}") int poolSize,
                        @Value("${relay.worker.instances:1}") int instanceCount) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionManager = transactionManager;
        this.objectMapper = objectMapper;
        this.handlersByType = handlersByType;
        this.backoffStrategy = backoffStrategy;
        this.poolSize = poolSize;
        this.instanceCount = instanceCount;
    }

    @PostConstruct
    public void start() {
        taskScheduler = new ThreadPoolTaskScheduler();
        // One dedicated thread per task-type per worker (poll/heartbeat/reclaim) so a slow
        // pollAndRun() (it can block for up to a handler's timeout) can never starve that same
        // worker's own heartbeat and cause a false-positive reclaim.
        taskScheduler.setPoolSize(Math.max(instanceCount * 3, 1));
        taskScheduler.setThreadNamePrefix("relay-worker-scheduler-");
        taskScheduler.initialize();

        for (int i = 0; i < instanceCount; i++) {
            startWorker();
        }
    }

    private void startWorker() {
        UUID workerId = UUID.randomUUID();
        Worker worker = new Worker(workerId, jdbcTemplate, transactionManager, objectMapper,
                handlersByType, backoffStrategy, poolSize, () -> onZombieDetected(workerId));
        worker.start();
        workers.add(worker);
        schedulesByWorkerId.put(workerId, List.of(
                taskScheduler.scheduleWithFixedDelay(worker::pollAndRun, Worker.POLL_INTERVAL),
                taskScheduler.scheduleWithFixedDelay(worker::heartbeat, Worker.HEARTBEAT_INTERVAL),
                taskScheduler.scheduleWithFixedDelay(worker::reclaimStaleWorkers, Worker.RECLAIM_INTERVAL)));
        log.info("Started worker {}", workerId);
    }

    private void onZombieDetected(UUID workerId) {
        List<ScheduledFuture<?>> futures = schedulesByWorkerId.remove(workerId);
        if (futures != null) {
            futures.forEach(future -> future.cancel(false));
        }
        log.info("Cancelled schedules for zombie worker {}", workerId);
    }

    @PreDestroy
    public void shutdown() {
        for (Worker worker : workers) {
            List<ScheduledFuture<?>> futures = schedulesByWorkerId.remove(worker.getWorkerId());
            if (futures != null) {
                futures.forEach(future -> future.cancel(false));
            }
            worker.shutdown();
        }
        if (taskScheduler != null) {
            taskScheduler.shutdown();
        }
    }

    List<Worker> getWorkers() {
        return workers;
    }
}
