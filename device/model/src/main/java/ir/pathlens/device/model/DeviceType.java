package ir.pathlens.device.model;

/**
 * The type of camera for different kinds of traffic cameras and etc.
 */
public enum DeviceType {
    RED_LIGHT_CAMERA("red-light"),
    SPEED_CAMERA("speed"),
    TOLL_CAMERA("toll");

    private final String value;

    DeviceType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static boolean isValid(String value) {
        if (value == null) {
            return false;
        }
        for (DeviceType type : values()) {
            if (type.value.equals(value)) {
                return true;
            }
        }
        return false;
    }
}

