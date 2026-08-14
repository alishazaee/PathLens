package ir.pathlens.device.model;

/**
 * The current status of a device.
 */
public enum DeviceStatus {
    ACTIVE("active"),
    INACTIVE("inactive");

    private final String value;

    DeviceStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String value) {
        if (value == null) {
            return false;
        }
        for (DeviceStatus status : values()) {
            if (status.value.equals(value)) {
                return true;
            }
        }
        return false;
    }
}

