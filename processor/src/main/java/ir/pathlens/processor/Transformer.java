package ir.pathlens.processor;

import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SERIAL_NUMBER;
import static ir.pathlens.proto.CameraLogProto.ErrorType.HARD;

import ir.pathlens.camera.CameraLogParser;
import ir.pathlens.device.cache.DeviceCache;
import ir.pathlens.proto.CameraLogProto.Error;
import ir.pathlens.proto.CameraLogProto.Log;
import ir.pathlens.proto.RawLogProto;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms raw camera logs into enriched camera logs using the device cache.
 */
public class Transformer {

    private static final Logger logger = LoggerFactory.getLogger(Transformer.class);
    private final Enricher enricher;

    public Transformer(DeviceCache deviceCache) {
        this.enricher = new Enricher(deviceCache);
    }

    public TransformResult transform(RawLogProto.Log rawLog) {
        String filePath = rawLog.getFilePath();
        String fileName = extractFileName(filePath);

        if (fileName.isEmpty()) {
            logger.error("Filename is empty for record: {}", rawLog);
            throw new AssertionError("Filename cannot be empty");
        }

        Log.Builder builder = Log.newBuilder().setFilePath(filePath);
        CameraLogParser logParser = new CameraLogParser(builder, rawLog.getRecord());
        logParser.parse();

        TransformResult result = new TransformResult();

        if (builder.getErrorType() == HARD) {
            logInvalidRecord(builder.getErrorSummaryList());
            return result
                    .addErrors(builder.getErrorSummaryList())
                    .setErrorType(HARD)
                    .setParsable(false)
                    .setLocationEnriched(false)
                    .setLog(builder.build().toByteArray());
        }

        String serial = rawLog.getDeviceSerialNumber();
        if (serial.isEmpty()) {
            logger.warn("Invalid serial number, serial: {}", serial);
            builder.setErrorType(HARD)
                    .addErrorSummary(INVALID_SERIAL_NUMBER)
                    .setRawRecord(rawLog.getRecord());
            return result
                    .addError(INVALID_SERIAL_NUMBER)
                    .setErrorType(HARD)
                    .setParsable(true)
                    .setLocationEnriched(false)
                    .setLog(builder.build().toByteArray());
        }

        enricher.enrich(builder, serial);

        if (builder.getErrorType() == HARD) {
            logInvalidRecord(builder.getErrorSummaryList());
            return result
                    .addErrors(builder.getErrorSummaryList())
                    .setErrorType(HARD)
                    .setParsable(true)
                    .setLocationEnriched(false)
                    .setLog(builder.setRawRecord(rawLog.getRecord()).build().toByteArray());
        }

        if (builder.hasErrorType()) {
            result.setErrorType(builder.getErrorType());
        }

        return result
                .addErrors(builder.getErrorSummaryList())
                .setParsable(true)
                .setLocationEnriched(true)
                .setLog(builder.build().toByteArray());
    }

    private String extractFileName(String filePath) {
        int lastSlash = filePath.lastIndexOf('/');
        if (lastSlash == -1 || lastSlash == filePath.length() - 1) {
            return "";
        }
        return filePath.substring(lastSlash + 1);
    }

    private void logInvalidRecord(List<Error> errors) {
        logger.info("Invalid record, errors: {}", errors);
    }
}
