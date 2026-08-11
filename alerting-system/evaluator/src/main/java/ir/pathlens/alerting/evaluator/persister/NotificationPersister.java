package ir.pathlens.alerting.evaluator.persister;

import ir.pathlens.alerting.db.jooq.tables.records.TrackedLogRecord;
import ir.pathlens.alerting.evaluator.configs.ApplicationConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Persists tracked log records to PostgreSQL in batches. */
public class NotificationPersister implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(NotificationPersister.class);
    private final int batchSize;
    private final long persistBatchTimeOutInMillis;
    private final boolean running;
    private final List<PersistRecord> targets = new ArrayList<>();
    private final BlockingQueue<PersistRecord> targetLogBlockingQueue;
    private final PostgresWriter postgresWriter;
    private Thread persistLoopThread;

    public NotificationPersister(ApplicationConfig applicationConfig, PostgresWriter postgresWriter) {
        batchSize = applicationConfig.persisterBatchSize();
        targetLogBlockingQueue = new LinkedBlockingDeque<>(applicationConfig.persistQueueSize());
        this.postgresWriter = postgresWriter;
        running = true;
        persistBatchTimeOutInMillis = applicationConfig.persistBatchTimeOutInMillis();
    }

    public void start() {
        persistLoopThread = new Thread(this::persistBatchLoop);
        persistLoopThread.start();
        logger.info("notification persister successfully started...");
    }

    public void persist(PersistRecord persistRecord) {
        try {
            while (!targetLogBlockingQueue.offer(persistRecord)) {
                logger.warn("you should never see this message, persist batch queue is full");
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            logger.info("Interrupt exception has been raised, ", e);
        }
    }

    private void persistBatchLoop() {
        long lastInsertTimeInMillis = System.currentTimeMillis();
        try {
            while (running) {
                PersistRecord persistRecord =
                        targetLogBlockingQueue.poll(persistBatchTimeOutInMillis, TimeUnit.MILLISECONDS);
                if (persistRecord != null) {
                    targets.add(persistRecord);
                }
                if (targets.size() >= batchSize
                        || System.currentTimeMillis() - lastInsertTimeInMillis >= persistBatchTimeOutInMillis) {
                    if (!targets.isEmpty()) {
                        postgresWriter.insertBatch(targets.stream().map(PersistRecord::trackedLogRecord).toList());
                        targets.forEach(record -> record.callBack.run());
                        logger.info("persisted batch size: " + targets.size());
                        targets.clear();
                    }
                    lastInsertTimeInMillis = System.currentTimeMillis();
                }
            }
        } catch (InterruptedException e) {
            logger.info("Interrupt exception has been raised, ", e);
        }
    }

    @Override
    public void close() {
        if (persistLoopThread != null) {
            persistLoopThread.interrupt();
        }
    }

    /**
     * A tracked log record together with the callback to invoke once it has been persisted.
     */
    public record PersistRecord(Runnable callBack, TrackedLogRecord trackedLogRecord) {

    }
}
