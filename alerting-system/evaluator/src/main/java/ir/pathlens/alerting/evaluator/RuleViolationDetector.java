package ir.pathlens.alerting.evaluator;

import ir.pathlens.alerting.client.RulesCache;
import ir.pathlens.alerting.client.RulesCache.RulesCacheSnapshot;
import ir.pathlens.alerting.model.IdentityType;
import ir.pathlens.alerting.model.IdentityWrapper;
import ir.pathlens.proto.CameraLogProto.Log;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Detects rule violations by checking if log events fall inside or outside geographic regions defined by alerting
 * rules. This class is not thread-safe.
 */
public class RuleViolationDetector {
    private RulesCacheSnapshot ruleSnapshot;
    private Point point;
    private final WKTReader wktReader = new WKTReader();
    private final RulesCache rulesCache;
    private final GeometryFactory geometryFactory = new GeometryFactory();

    public RuleViolationDetector(RulesCache rulesCache) {
        this.rulesCache = rulesCache;
    }

    public Result findViolatedRules(Log log) {
        ruleSnapshot = rulesCache.snapshot();
        point = createGeometryPoint(log.getLocation().getLatitude(), log.getLocation().getLongitude());
        return evaluate(log);
    }

    private Result evaluate(Log log) {
        Result plateResult = checkViolation(new IdentityWrapper(IdentityType.PlateNumber, log.getPlateNumber()));
        Result phoneResult = checkViolation(new IdentityWrapper(IdentityType.PhoneNumber, log.getPhoneNumber()));
        return plateResult.merge(phoneResult);
    }

    private Result checkViolation(IdentityWrapper identity) {
        Set<UUID> violatedRules = new HashSet<>();
        Set<UUID> nonViolatedRules = new HashSet<>();

        Optional<Set<UUID>> matchedRules = ruleSnapshot.getRulesIdsByIdentity(identity);
        if (matchedRules.isEmpty()) {
            return new Result(violatedRules, nonViolatedRules);
        }

        for (UUID ruleId : matchedRules.get()) {
            boolean enterViolated = isGeofenceConditionViolated(ruleSnapshot::getEnterIntoRegionRuleGeometryWktByRuleId,
                    ruleId, false);
            boolean leaveViolated = isGeofenceConditionViolated(ruleSnapshot::getLeavingRegionRuleGeometryWktByRuleId,
                    ruleId, true);

            if (enterViolated || leaveViolated) {
                violatedRules.add(ruleId);
            } else {
                nonViolatedRules.add(ruleId);
            }
        }
        return new Result(violatedRules, nonViolatedRules);
    }

    private boolean isGeofenceConditionViolated(Function<UUID, Optional<String>> evaluator, UUID ruleId,
                                                boolean shouldBeInside) {
        Optional<String> wktGeometry = evaluator.apply(ruleId);
        if (wktGeometry.isEmpty()) {
            return false;
        }
        Geometry geometry;
        try {
            geometry = wktReader.read(wktGeometry.get());
        } catch (ParseException e) {
            throw new AssertionError("Unexpected error, parse failed", e);
        }
        boolean isInside = geometry.contains(point);
        return isInside != shouldBeInside;
    }

    private Point createGeometryPoint(double latitude, double longitude) {
        return geometryFactory.createPoint(new Coordinate(longitude, latitude));
    }

    /**
     * Holds the result of rule evaluation, separating violated from non-violated rules.
     */
    public record Result(Set<UUID> violatedRules, Set<UUID> nonViolatedRules) {
        public Result merge(Result other) {
            Set<UUID> violated = new HashSet<>(violatedRules);
            violated.addAll(other.violatedRules);

            Set<UUID> nonViolated = new HashSet<>(nonViolatedRules);
            nonViolated.addAll(other.nonViolatedRules);
            nonViolated.removeAll(violated);

            return new Result(violated, nonViolated);
        }
    }

}
