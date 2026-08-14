package ir.pathlens.processor;

import static ir.pathlens.processor.MetricName.CAMERA_LOGS_ERROR;
import static ir.pathlens.processor.MetricName.CAMERA_LOGS_TOTAL;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import ir.pathlens.proto.CameraLogProto.Error;

/**
 * Records processing metrics for transformed camera logs.
 */
public class Profiler {
    private final MeterRegistry meterRegistry;

    public Profiler(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void profile(TransformResult transformResult) {
        for (Error error : transformResult.getErrors()) {
            Counter.builder(CAMERA_LOGS_ERROR)
                    .tags(
                            "error", error.toString()
                    ).register(meterRegistry).increment();
        }
        Counter.builder(CAMERA_LOGS_TOTAL)
                .tags(
                        "error_type", transformResult.getErrorType() == null
                                ? "NO_ERROR" : transformResult.getErrorType().name(),
                        "location_enriched", String.valueOf(transformResult.isLocationEnriched()),
                        "parsable", String.valueOf(transformResult.isParsable())
                ).register(meterRegistry).increment();
    }

}
