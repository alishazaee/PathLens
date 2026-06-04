package ir.pathlens.generator;

import ir.pathlens.camera.Constants.IpVersion;
import ir.pathlens.proto.RawLogProto.Log;
import ir.pathlens.proto.RawLogProto.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a random raw Log.
 */
public class RawLogGenerator {

    private static ThreadLocalRandom random = ThreadLocalRandom.current();

    private RawLogGenerator() {
    }

    public static List<Log> randomLogs(int numberOfLogs, IpVersion ipVersion) {
        List<Log> logs = new ArrayList<>(numberOfLogs);
        for (int i = 0; i < numberOfLogs; i++) {
            Log log = randomLog(ipVersion).build();
            logs.add(log);
        }
        return logs;
    }

    public static Log.Builder randomLog() {
        return randomLog(IpVersion.values()[random.nextInt(IpVersion.values().length)]);
    }

    public static Log.Builder randomLog(IpVersion ipVersion) {
        if (ipVersion == IpVersion.IPV4 || ipVersion == IpVersion.IPV6) {
            return Log.newBuilder()
                    .setDeviceSerialNumber(UUID.randomUUID().toString())
                    .setLogType(randomLogType())
                    .setFilePath(randomFilePath())
                    .setRecord(CameraLogGenerator.randomLog(ipVersion).toString());
        }
        throw new AssertionError("ip version is invalid");
    }

    public static Type randomLogType() {
        return Type.values()[random.nextInt(0, Type.values().length - 1)];
    }

    public static String randomFilePath() {
        return "/tmp/file/camera-zone-" + random.nextInt(1, 100);
    }
}