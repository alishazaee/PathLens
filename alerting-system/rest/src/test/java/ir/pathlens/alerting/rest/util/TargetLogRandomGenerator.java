package ir.pathlens.alerting.rest.util;

import ir.pathlens.GeometryUtils;
import ir.pathlens.alerting.model.RuleType;
import ir.pathlens.proto.TargetLogProto.Location;
import ir.pathlens.proto.TargetLogProto.TargetLog;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates random target logs for tests.
 */
public class TargetLogRandomGenerator {
    private static final Random random = new Random();
    private final UUID ruleId;
    private final RuleType ruleType;
    private final String wktGeometry;

    public TargetLogRandomGenerator(UUID ruleId, RuleType ruleType, String wktGeometry) {
        this.ruleId = ruleId;
        this.ruleType = ruleType;
        this.wktGeometry = wktGeometry;
    }

    public List<TargetLog> getRandomTargets(int insideCount, int outSideCount) throws Exception {
        List<TargetLog.Builder> targetLogs = new ArrayList<>();

        if (ruleType == RuleType.Enter) {
            addTargets(targetLogs, insideCount, true, true);
            addTargets(targetLogs, outSideCount, false, false);
        } else if (ruleType == RuleType.Exit) {
            addTargets(targetLogs, insideCount, true, false);
            addTargets(targetLogs, outSideCount, false, true);
        } else {
            throw new AssertionError("the rule type is not valid");
        }
        targetLogs.stream().filter(TargetLog.Builder::getViolated).findFirst()
                .ifPresent(target -> target.setShouldNotify(true));
        Collections.shuffle(targetLogs);
        return targetLogs.stream().map(TargetLog.Builder::build).toList();
    }

    private void addTargets(
            List<TargetLog.Builder> targetLogs,
            int count,
            boolean isInside,
            boolean violated
    ) throws Exception {

        for (int i = 0; i < count; i++) {
            GeometryUtils.LatLon latLon = isInside ? GeometryUtils.getRandomPointInsideWkt(wktGeometry)
                    : GeometryUtils.getRandomPointOutsideWkt(wktGeometry);

            targetLogs.add(createTargetLog(latLon, violated));
        }
    }

    private TargetLog.Builder createTargetLog(GeometryUtils.LatLon latLon, boolean violated) {
        return TargetLog.newBuilder()
                .setLocation(Location.newBuilder()
                        .setLatitude(latLon.latitude())
                        .setLongitude(latLon.longitude())
                        .build())
                .setViolated(violated)
                .setTimestamp(LocalDateTime.now().plusSeconds(random.nextInt(1, 65535)).toEpochSecond(ZoneOffset.UTC))
                .setRuleId(ruleId.toString())
                .setShouldNotify(false);
    }
}
