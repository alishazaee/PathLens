package ir.pathlens.generator;

import static ir.pathlens.camera.Constants.IP_VERSION_INDEX;
import static ir.pathlens.camera.Constants.IpVersion;
import static ir.pathlens.camera.Constants.IpVersion.IPV4;
import static ir.pathlens.camera.Constants.IpVersion.IPV6;
import static ir.pathlens.camera.Constants.PHONE_NUMBER_FIELD_INDEX;
import static ir.pathlens.camera.Constants.PLATE_NUMBER_INDEX;
import static ir.pathlens.camera.Constants.RECORD_SIZE;
import static ir.pathlens.camera.Constants.SRC_IP_ADDRESS_FIELD_INDEX;
import static ir.pathlens.camera.Constants.TIMESTAMP_FIELD_INDEX;

import ir.pathlens.proto.CameraLogProto;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a random {@link  CameraLogProto} Log.
 */
public class CameraLogGenerator {

    private static final ThreadLocalRandom random = ThreadLocalRandom.current();
    private String srcIpv4;
    private String srcIpv6;
    private String plateNumber;
    private IpVersion ipVersion;
    private String phoneNumber;
    private long timestamp;

    private CameraLogGenerator() {

    }

    public static CameraLogGenerator randomLog() {
        return randomLog(IpVersion.values()[random.nextInt(IpVersion.values().length)]);
    }

    public static CameraLogGenerator randomLog(IpVersion ipVersion) {
        CameraLogGenerator generator = new CameraLogGenerator();
        switch (ipVersion) {
            case IPV4 -> generator.srcIpv4 = randomIpv4();
            case IPV6 -> generator.srcIpv6 = randomIpv6();
            default -> throw new IllegalStateException("the ip version is not valid");
        }

        generator.plateNumber = UUID.randomUUID().toString();
        generator.phoneNumber = randomPhoneNumber();
        generator.timestamp = System.currentTimeMillis() - random.nextInt(10000, 36000000);
        generator.ipVersion = ipVersion;
        return generator;
    }

    public String getSrcIpv4() {
        return srcIpv4;
    }

    public CameraLogGenerator setSrcIpv4(String srcIpv4) {
        this.srcIpv4 = srcIpv4;
        return this;
    }

    public String getSrcIpv6() {
        return srcIpv6;
    }

    public CameraLogGenerator setSrcIpv6(String srcIpv6) {
        this.srcIpv6 = srcIpv6;
        return this;
    }

    public String getPlateNumber() {
        return plateNumber;
    }

    public CameraLogGenerator setPlateNumber(String plateNumber) {
        this.plateNumber = plateNumber;
        return this;
    }

    public IpVersion getIpVersion() {
        return ipVersion;
    }

    public CameraLogGenerator setIpVersion(IpVersion ipVersion) {
        this.ipVersion = ipVersion;
        return this;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public CameraLogGenerator setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public CameraLogGenerator setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public CameraLogProto.Log.Builder generateLogBuilder() {
        CameraLogProto.Log.Builder log = CameraLogProto.Log.newBuilder()
                .setTimestamp(timestamp)
                .setPlateNumber(plateNumber)
                .setPhoneNumber(randomPhoneNumber());
        if (ipVersion == IPV4) {
            log.setIpv4SrcAddr(srcIpv4);
        } else if (ipVersion == IPV6) {
            log.setIpv6SrcAddr(srcIpv6);
        }
        return log;
    }

    @Override
    public String toString() {
        String[] fields = new String[RECORD_SIZE];
        if (ipVersion == IPV4) {
            fields[SRC_IP_ADDRESS_FIELD_INDEX] = srcIpv4;
        } else if (ipVersion == IPV6) {
            fields[SRC_IP_ADDRESS_FIELD_INDEX] = srcIpv6;
        }

        fields[PLATE_NUMBER_INDEX] = plateNumber;
        fields[PHONE_NUMBER_FIELD_INDEX] = phoneNumber;
        fields[TIMESTAMP_FIELD_INDEX] = String.valueOf(timestamp);
        fields[IP_VERSION_INDEX] = String.valueOf(ipVersion.getValue());
        return String.join("|", fields);

    }

    private static String randomIpv4() {
        byte[] bytes = new byte[4];
        ThreadLocalRandom.current().nextBytes(bytes);
        try {
            return InetAddress.getByAddress(bytes).getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static String randomIpv6() {
        byte[] bytes = new byte[16];
        ThreadLocalRandom.current().nextBytes(bytes);
        try {
            return InetAddress.getByAddress(bytes).getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static String randomPhoneNumber() {
        StringBuilder numberInStr = new StringBuilder("+98");
        for (int i = 0; i < 10; i++) {
            numberInStr.append(ThreadLocalRandom.current().nextInt(0, 10));
        }
        return numberInStr.toString();
    }
}
