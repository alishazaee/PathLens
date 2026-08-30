package ir.pathlens.alerting.model;

/**
 * API path constants for the alerting endpoints.
 */
public class ApiPathConstants {

    public static final String BASE_API = "";
    public static final String RULES_BASE = BASE_API + "/rules";
    public static final String NOTIFICATIONS_BASE = BASE_API + "/notifications";
    public static final String TARGET_LOGS_BASE = BASE_API + "/target-log";
    public static final String INTERNAL_RULES_BASE = BASE_API + "/internal/rules";

    public static final String CREATE_RULE_PATH = RULES_BASE;
    public static final String SEARCH_RULES_PATH = RULES_BASE;
    public static final String GET_RULE_PATH = RULES_BASE + "/{id}";
    public static final String UPDATE_RULE_PATH = RULES_BASE + "/{id}";
    public static final String DELETE_RULE_PATH = RULES_BASE + "/{id}";
    public static final String ACTIVATE_RULE_PATH = RULES_BASE + "/{id}/activate";
    public static final String DEACTIVATE_RULE_PATH = RULES_BASE + "/{id}/deactivate";

    public static final String GET_ACTIVE_RULES_PATH = INTERNAL_RULES_BASE + "/active";
    public static final String GET_RULES_REVISION_PATH = INTERNAL_RULES_BASE + "/revision";

    public static final String SEARCH_NOTIFICATIONS_PATH = NOTIFICATIONS_BASE;
    public static final String GET_NOTIFICATION_PATH = NOTIFICATIONS_BASE + "/{id}";
    public static final String SET_NOTIFICATION_SEEN_PATH = NOTIFICATIONS_BASE + "/{id}/seen";

    public static final String SEARCH_TARGET_LOGS_PATH = TARGET_LOGS_BASE;

    public static String buildPath(String template, Object... values) {
        String result = template;
        for (Object value : values) {
            result = result.replaceFirst("\\{[^/]+\\}", value.toString());
        }
        return result;
    }
}
