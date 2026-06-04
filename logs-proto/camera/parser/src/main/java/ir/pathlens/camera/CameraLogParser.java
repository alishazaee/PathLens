package ir.pathlens.camera;


import ir.pathlens.proto.CameraLogProto;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static ir.pathlens.camera.CameraLogValidator.isValidPhoneNumber;
import static ir.pathlens.camera.CameraLogValidator.validateNumericField;
import static ir.pathlens.camera.Constants.*;
import static ir.pathlens.proto.CameraLogProto.Error.*;

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
        String[] fields = rawLog.split("\\" + Constants.DELIMITER);

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
            } else if (version.equals(IpVersion.IPV6)){
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
