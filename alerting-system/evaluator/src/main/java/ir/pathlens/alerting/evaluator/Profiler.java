package ir.pathlens.alerting.evaluator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Exposes metrics for rule evaluation results.
 */
public class Profiler {
    public static final String VIOLATED_LOGS_METRIC_COUNTER_NAME = "rules.violated";
    public static final String NON_VIOLATED_LOGS_METRIC_COUNTER_NAME = "rules.non_violated";
    public static final String MATCHED_LOGS_METRIC_COUNTER_NAME = "logs.matched";

    private final Counter violatedRulesCounter;
    private final Counter nonViolatedRulesCounter;
    private final Counter matchedLogsCounter;

    public Profiler(MeterRegistry meterRegistry) {
        violatedRulesCounter = Counter.builder(VIOLATED_LOGS_METRIC_COUNTER_NAME)
                .description("Number of violated rules")
                .register(meterRegistry);
        nonViolatedRulesCounter = Counter.builder(NON_VIOLATED_LOGS_METRIC_COUNTER_NAME)
                .description("Number of non-violated rules")
                .register(meterRegistry);
        matchedLogsCounter = Counter.builder(MATCHED_LOGS_METRIC_COUNTER_NAME)
                .description("Total number of logs matched with rules")
                .register(meterRegistry);
    }

    public void recordViolatedRules(int count) {
        violatedRulesCounter.increment(count);
    }

    public void recordNonViolatedRules(int count) {
        nonViolatedRulesCounter.increment(count);
    }

    public void recordMatchedLog() {
        matchedLogsCounter.increment();
    }
}
