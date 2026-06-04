package ir.pathlens.camera;

/**
 * Contains the constant variables that is specific for camera logs.
 */
public class Constants {

    public static final String DELIMITER = "|";
    public static final int RECORD_SIZE = 5;

    public static final int PLATE_NUMBER_INDEX = 0;
    public static final int SRC_IP_ADDRESS_FIELD_INDEX = 1;
    public static final int TIMESTAMP_FIELD_INDEX = 2;
    public static final int PHONE_NUMBER_FIELD_INDEX = 3;
    public static final int IP_VERSION_INDEX = 4;

    public enum IpVersion {
        IPV4("v4"),
        IPV6("v6");

        private final String value;

        IpVersion(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static boolean isValid(String value) {
            if (value == null) {
                return false;
            }
            for (IpVersion version : IpVersion.values()) {
                if (version.getValue().equals(value)) {
                    return true;
                }
            }
            return false;
        }

        public static IpVersion fromValue(String value) {
            for (IpVersion version : IpVersion.values()) {
                if (version.getValue().equals(value)) {
                    return version;
                }
            }
            throw new IllegalArgumentException("Unknown IP version: " + value);
        }
    }
}
