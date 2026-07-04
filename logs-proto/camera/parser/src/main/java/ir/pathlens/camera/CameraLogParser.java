package ir.pathlens.camera;

import static ir.pathlens.camera.CameraLogValidator.isValidPhoneNumber;
import static ir.pathlens.camera.CameraLogValidator.validateNumericField;
import static ir.pathlens.camera.Constants.DELIMITER;
import static ir.pathlens.camera.Constants.IP_VERSION_INDEX;
import static ir.pathlens.camera.Constants.IpVersion;
import static ir.pathlens.camera.Constants.PHONE_NUMBER_FIELD_INDEX;
import static ir.pathlens.camera.Constants.PLATE_NUMBER_INDEX;
import static ir.pathlens.camera.Constants.RECORD_SIZE;
import static ir.pathlens.camera.Constants.SRC_IP_ADDRESS_FIELD_INDEX;
import static ir.pathlens.camera.Constants.TIMESTAMP_FIELD_INDEX;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_IP_VERSION;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_PHONE_NUMBER;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_PLATE_NUMBER;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SRC_IPV4;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SRC_IPV6;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_TIMESTAMP;
import static ir.pathlens.proto.CameraLogProto.Error.RECORD_SIZE_INCORRECT;

import ir.pathlens.proto.CameraLogProto;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Gets raw camera log record in string and parses it into {@link CameraLogProto.Log}.
 */
public class CameraLogParser {

    private final CameraLogProto.Log.Builder builder;
    private final String rawLog;

    public CameraLogParser(CameraLogProto.Log.Builder builder, String rawLog) {
        this.builder = builder;
        this.rawLog = rawLog;
    }

    public void parse() {
        String[] fields = rawLog.split("\\" + DELIMITER);

        if (fields.length != RECORD_SIZE) {
            builder.addErrorSummary(RECORD_SIZE_INCORRECT);
            setErrorType();
            return;
        }

        if (!fields[PLATE_NUMBER_INDEX].isEmpty()) {
            builder.setPlateNumber(fields[PLATE_NUMBER_INDEX]);
        } else {
            builder.addErrorSummary(INVALID_PLATE_NUMBER);
        }

        if (fields[IP_VERSION_INDEX].isEmpty() || !IpVersion.isValid(fields[IP_VERSION_INDEX])) {
            builder.addErrorSummary(INVALID_IP_VERSION);
        } else {
            String srcIp = fields[SRC_IP_ADDRESS_FIELD_INDEX];
            IpVersion version = IpVersion.fromValue(fields[IP_VERSION_INDEX]);
            if (version.equals(IpVersion.IPV4)) {
                fillIpField(srcIp, builder::setIpv4SrcAddr, CameraLogValidator::isIpv4Valid, INVALID_SRC_IPV4);
            } else if (version.equals(IpVersion.IPV6)) {
                fillIpField(srcIp, builder::setIpv6SrcAddr, CameraLogValidator::isIpv6Valid, INVALID_SRC_IPV6);
            }
        }

        validateNumericField(fields[TIMESTAMP_FIELD_INDEX], builder, INVALID_TIMESTAMP,
                value -> builder.setTimestamp(Long.parseLong(value)));

        if (isValidPhoneNumber(fields[PHONE_NUMBER_FIELD_INDEX])) {
            builder.setPhoneNumber(fields[PHONE_NUMBER_FIELD_INDEX]);
        } else {
            builder.addErrorSummary(INVALID_PHONE_NUMBER);
        }

        setErrorType();
    }

    private void fillIpField(String ipAddress, Consumer<String> setter, Predicate<String> ipValidator,
            CameraLogProto.Error errorSummary) {
        if (ipValidator.test(ipAddress)) {
            setter.accept(ipAddress);
        } else {
            builder.addErrorSummary(errorSummary);
        }
    }

    private void setErrorType() {
        builder.setRawRecord(rawLog);
        if (builder.getErrorSummaryList().contains(INVALID_TIMESTAMP)
                || builder.getErrorSummaryList().contains(INVALID_PLATE_NUMBER)
                || builder.getErrorSummaryList().contains(RECORD_SIZE_INCORRECT)
                || builder.getErrorSummaryList().contains(INVALID_PHONE_NUMBER)) {
            builder.setErrorType(CameraLogProto.ErrorType.HARD);
        } else if (!builder.getErrorSummaryList().isEmpty()) {
            builder.setErrorType(CameraLogProto.ErrorType.SOFT);
        }
    }
}
