package ir.pathlens.camera;

import ir.pathlens.proto.CameraLogProto;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Validates camera raw string records. The validations include ip and phone number.
 */
public class CameraLogValidator {

    public static void validateNumericField(String fieldValue, CameraLogProto.Log.Builder builder,
            CameraLogProto.Error error, Consumer<String> setter) {
        if (isNumeric(fieldValue) && !fieldValue.equals("0")) {
            setter.accept(fieldValue);
        } else {
            builder.addErrorSummary(error);
        }
    }

    public static boolean isNumeric(String str) {
        return str != null && str.trim().matches("\\d+");
    }

    public static boolean isValidPhoneNumber(String phoneNumber) {
        Pattern pattern = Pattern.compile("^\\+[1-9]{1,2}\\d{10}$");
        return pattern.matcher(phoneNumber).matches();
    }

    public static boolean isIpv4Valid(String ipv4) {
        if (ipv4 == null || ipv4.isEmpty()) {
            return false;
        }
        try {
            InetAddress address = InetAddress.getByName(ipv4);
            return address instanceof Inet4Address;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    public static boolean isIpv6Valid(String ipv6) {
        if (ipv6 == null || ipv6.isEmpty()) {
            return false;
        }
        try {
            InetAddress address = InetAddress.getByName(ipv6);
            return address instanceof Inet6Address;
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
