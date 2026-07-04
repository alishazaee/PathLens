package ir.pathlens.alerting.model;

/**
 * Filter rules based on different fields.
 */
public record RuleFilter(
        String title,
        Boolean isActive
) {
}
