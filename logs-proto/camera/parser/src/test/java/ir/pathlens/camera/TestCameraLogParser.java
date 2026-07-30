package ir.pathlens.camera;

import static ir.pathlens.camera.Constants.DELIMITER;
import static ir.pathlens.camera.Constants.IpVersion;
import static ir.pathlens.camera.Constants.PHONE_NUMBER_FIELD_INDEX;
import static ir.pathlens.camera.Constants.PLATE_NUMBER_INDEX;
import static ir.pathlens.camera.Constants.SRC_IP_ADDRESS_FIELD_INDEX;
import static ir.pathlens.camera.Constants.TIMESTAMP_FIELD_INDEX;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_PHONE_NUMBER;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_PLATE_NUMBER;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SRC_IPV4;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SRC_IPV6;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_TIMESTAMP;
import static ir.pathlens.proto.CameraLogProto.Error.RECORD_SIZE_INCORRECT;
import static ir.pathlens.proto.CameraLogProto.ErrorType.HARD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ir.pathlens.generator.RawLogGenerator;
import ir.pathlens.proto.CameraLogProto;
import ir.pathlens.proto.RawLogProto;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CameraLogParser}.
 */
public class TestCameraLogParser {

    @Test
    public void testParsingWithNoError() {
        for (Constants.IpVersion ipVersion : IpVersion.values()) {
            List<RawLogProto.Log> randomRawLogs = RawLogGenerator.randomLogs(10, ipVersion);
            for (RawLogProto.Log randomRawLog : randomRawLogs) {
                CameraLogProto.Log.Builder log = CameraLogProto.Log.newBuilder();
                CameraLogParser parser = new CameraLogParser(log, randomRawLog.getRecord());
                parser.parse();
                assertEquals(0, log.getErrorSummaryList().size(), log.getErrorSummaryList().toString());
            }
        }
    }

    @Test
    public void testInvalidIpAddresses() {
        Map<CameraLogProto.Error, IpVersion> ipversionToErrorMap = Map.of(
                INVALID_SRC_IPV4, IpVersion.IPV4,
                INVALID_SRC_IPV6, IpVersion.IPV6
        );
        for (Entry<CameraLogProto.Error, IpVersion> entry : ipversionToErrorMap.entrySet()) {
            RawLogProto.Log.Builder randomRawLog = RawLogGenerator.randomLog(entry.getValue());
            CameraLogProto.Log.Builder log = CameraLogProto.Log.newBuilder();
            String[] rawLogFields = randomRawLog.getRecord().split("\\" + DELIMITER);
            rawLogFields[SRC_IP_ADDRESS_FIELD_INDEX] = "chert";
            CameraLogParser parser = new CameraLogParser(log, String.join(DELIMITER, rawLogFields));
            parser.parse();
            assertEquals(1, log.getErrorSummaryList().size(), log.getErrorSummaryList().toString());
            assertTrue(log.getErrorSummaryList().contains(entry.getKey()), log.getErrorSummaryList().toString());
        }
    }

    @Test
    public void testHardErrors() {
        Map<Integer, CameraLogProto.Error> indexToErrorMap = Map.of(
                TIMESTAMP_FIELD_INDEX, INVALID_TIMESTAMP,
                PLATE_NUMBER_INDEX, INVALID_PLATE_NUMBER,
                PHONE_NUMBER_FIELD_INDEX, INVALID_PHONE_NUMBER
        );
        for (Entry<Integer, CameraLogProto.Error> entry : indexToErrorMap.entrySet()) {
            RawLogProto.Log.Builder randomRawLog = RawLogGenerator.randomLog(Constants.IpVersion.IPV4);
            CameraLogProto.Log.Builder log = CameraLogProto.Log.newBuilder();
            String[] rawLogFields = randomRawLog.getRecord().split("\\" + DELIMITER);
            rawLogFields[entry.getKey()] = "";
            CameraLogParser parser = new CameraLogParser(log, String.join(DELIMITER, rawLogFields));
            parser.parse();
            assertEquals(1, log.getErrorSummaryList().size(), log.getErrorSummaryList().toString());
            assertTrue(log.getErrorSummaryList().contains(entry.getValue()));
            assertEquals(HARD, log.getErrorType());
        }
    }

    @Test
    public void testRecordSizeInvalid() {
        CameraLogProto.Log.Builder log = CameraLogProto.Log.newBuilder();
        CameraLogParser parser = new CameraLogParser(log, "");
        parser.parse();
        assertEquals(1, log.getErrorSummaryList().size(), log.getErrorSummaryList().toString());
        assertTrue(log.getErrorSummaryList().contains(RECORD_SIZE_INCORRECT));
        assertEquals(HARD, log.getErrorType());
    }
}
