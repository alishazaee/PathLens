package ir.pathlens.alerting.client;

import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.alerting.model.Rule;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.client.ApiCallException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches rules fetched from the alerting server, periodically syncing in the background.
 */
public class RulesCache implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RulesCache.class);

    private volatile RulesCacheSnapshot snapShot = new RulesCacheSnapshot(Map.of(), Map.of(), Map.of());

    private final RulesClient client;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile long currentRevisionNumber;
    private volatile boolean running = true;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final int maxInitialDelayInMillis;
    private final int minInitialDelayInMillis;

    public RulesCache(RulesClient client, int minInitialDelayInMillis, int maxInitialDelayInMillis) {
        Validate.isTrue(maxInitialDelayInMillis > minInitialDelayInMillis,
                "the max delay value must be greater than min delay value.");
        this.client = client;
        currentRevisionNumber = 0;
        this.maxInitialDelayInMillis = maxInitialDelayInMillis;
        this.minInitialDelayInMillis = minInitialDelayInMillis;
    }

    public void submitBackgroundTask() {
        executor.submit(this::runSyncLoop);
        logger.info("Rules cache started");
    }

    public synchronized void sync() throws ApiCallException {
        long serverSideRevisionNumber = client.getRevisionNumber();
        if (currentRevisionNumber < serverSideRevisionNumber) {
            List<Rule> rules = client.getAllActiveRules();
            Map<IdentityWrapper, Set<UUID>> updatedRuleCache = rules.stream()
                    .collect(Collectors.groupingBy(
                            Rule::identity, Collectors.mapping(Rule::id, Collectors.toSet())));
            Map<UUID, String> updatedEnterIntoRegionGeometryCache = rules.stream()
                    .filter(r -> r.ruleType() == RuleType.Enter)
                    .collect(Collectors.toMap(Rule::id, Rule::geometryWkt));
            Map<UUID, String> updatedLeavingTheRegionGeometryCache = rules.stream()
                    .filter(r -> r.ruleType() == RuleType.Exit)
                    .collect(Collectors.toMap(Rule::id, Rule::geometryWkt));
            snapShot = new RulesCacheSnapshot(
                    Map.copyOf(updatedRuleCache),
                    Map.copyOf(updatedEnterIntoRegionGeometryCache),
                    Map.copyOf(updatedLeavingTheRegionGeometryCache));
            currentRevisionNumber = serverSideRevisionNumber;
        }
    }

    public RulesCacheSnapshot snapshot() {
        return snapShot;
    }

    @Override
    public void close() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                logger.info("the Cache shutdown was not graceful !!");
            } else {
                logger.info("cache was turned off gracefully.");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        client.close();
    }

    private void runSyncLoop() {
        while (running) {
            try {
                long initialDelay = random.nextInt(minInitialDelayInMillis, maxInitialDelayInMillis);
                try {
                    TimeUnit.MILLISECONDS.sleep(initialDelay);
                } catch (InterruptedException e) {
                    logger.error("Interrupted during initial delay", e);
                    Thread.currentThread().interrupt();

                    return;
                }
                sync();
            } catch (ApiCallException ex) {
                logger.error("Unable to sync rules", ex);
            }
        }
    }

    /**
     * Immutable snapshot of the rule cache.
     */
    public record RulesCacheSnapshot(
            Map<IdentityWrapper, Set<UUID>> rulesCache,
            Map<UUID, String> geometryEnterIntoAreaCache,
            Map<UUID, String> geometryLeavingTheAreaCache

    ) {
        public Optional<Set<UUID>> getRulesIdsByIdentity(IdentityWrapper identityWrapper) {
            return Optional.ofNullable(rulesCache.get(identityWrapper));
        }

        public Optional<String> getEnterIntoRegionRuleGeometryWktByRuleId(UUID ruleId) {
            return Optional.ofNullable(geometryEnterIntoAreaCache.get(ruleId));
        }

        public Optional<String> getLeavingRegionRuleGeometryWktByRuleId(UUID ruleId) {
            return Optional.ofNullable(geometryLeavingTheAreaCache.get(ruleId));
        }
    }
}
