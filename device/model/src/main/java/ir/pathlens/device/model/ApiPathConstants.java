package ir.pathlens.device.model;

/**
 * Holds the REST API path constants for the device service.
 */
public class ApiPathConstants {
    public static final String BASE_API = "";

    public static final String CREATE_DEVICE_PATH = BASE_API + "/devices";
    public static final String GET_DEVICES_PATH = BASE_API + "/devices";
    public static final String GET_DEVICE_PATH = BASE_API + "/devices/{id}";
    public static final String UPDATE_DEVICE_PATH = BASE_API + "/devices/{id}";
    public static final String DELETE_DEVICE_PATH = BASE_API + "/devices/{id}";
    public static final String GET_REVISION_NUMBER = BASE_API + "/devices/revision";

    public static final String CREATE_LOCATIONS_PATH = BASE_API + "/locations";
    public static final String GET_LOCATIONS_PATH = BASE_API + "/locations";
    public static final String GET_LOCATION_PATH = BASE_API + "/locations/{siteId}";
    public static final String UPDATE_LOCATION_PATH = BASE_API + "/locations/{siteId}";
    public static final String DELETE_LOCATION_PATH = BASE_API + "/locations/{siteId}";

    public static String buildPath(String template, Object... values) {
        String result = template;
        for (Object value : values) {
            result = result.replaceFirst("\\{[^/]+\\}", value.toString());
        }
        return result;
    }
}

