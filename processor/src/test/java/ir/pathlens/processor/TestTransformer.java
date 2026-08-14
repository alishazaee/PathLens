package ir.pathlens.processor;

import static ir.pathlens.proto.CameraLogProto.Error.DEVICE_NOT_FOUND;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SERIAL_NUMBER;
import static ir.pathlens.proto.CameraLogProto.ErrorType.HARD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;
import ir.pathlens.device.cache.DeviceCache;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.rest.controller.MockDeviceController;
import ir.pathlens.generator.RawLogGenerator;
import ir.pathlens.processor.configs.ApplicationConfig;
import ir.pathlens.processor.configs.DeviceCacheConfig;
import ir.pathlens.proto.CameraLogProto;
import ir.pathlens.proto.RawLogProto.Log;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTransformer {

    private final List<String> sensorSerialNumbers = List.of("SN1", "SN2", "SN3");

    protected DeviceCache deviceCache;
    protected static final MockDeviceController mockDeviceController = createMockDeviceController();
    protected ApplicationConfig config;

    private static MockDeviceController createMockDeviceController() {
        try {
            return new MockDeviceController();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start mock device server", e);
        }
    }


    @BeforeEach
    void setup() throws ApiCallException {
        config = ConfigReader.loadConfig(Path.of("src/test/resources/application.yml"));
        config.getDeviceCacheConfig().setBaseUrl(mockDeviceController.getBaseUrl());
        DeviceCacheConfig cacheConfig = config.getDeviceCacheConfig();

        DeviceClient client = new DeviceClient(cacheConfig.getBaseUrl());
        deviceCache = new DeviceCache(client, cacheConfig.getMinInitialDelayInMillis(),
                cacheConfig.getMaxInitialDelayInMillis(), cacheConfig.getSyncIntervalInMillis());

        new DeviceRestProvisioner(mockDeviceController).populateMockServerWithDevices(sensorSerialNumbers);
        deviceCache.sync();
    }

    @Test
    void testCorrectTransformation() {
        Transformer transformer = new Transformer(deviceCache);
        String sensorSerialNumber = sensorSerialNumbers.get(new Random().nextInt(sensorSerialNumbers.size()));
        Log rawLog = RawLogGenerator.randomLog().setDeviceSerialNumber(sensorSerialNumber).build();

        TransformResult transformResult = transformer.transform(rawLog);
        assertTrue(transformResult.isLocationEnriched());
        assertTrue(transformResult.isParsable());
        assertEquals(0, transformResult.getErrors().size());
        assertNull(transformResult.getErrorType());
        CameraLogProto.Log cameraLog;
        try {
            cameraLog = CameraLogProto.Log.parseFrom(transformResult.getLog());
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("unexpected error happened in parsing log " + e.getMessage());
        }
        Optional<DeviceResponseDto> device = deviceCache.findByDeviceSerialNumber(sensorSerialNumber);
        if (device.isEmpty()) {
            throw new IllegalStateException("The device cache functionality is not as expected");
        }
        assertEquals(device.get().serialNumber(), cameraLog.getDevice().getSerialNumber());
        assertEquals(device.get().deviceType().name(), cameraLog.getDevice().getDeviceType().name());
        assertEquals(device.get().status().name(), cameraLog.getDevice().getDeviceStatus().name());
        assertEquals(device.get().deviceLocationDto().latitude().floatValue(), cameraLog.getLocation().getLatitude());
        assertEquals(device.get().deviceLocationDto().longitude().floatValue(), cameraLog.getLocation().getLongitude());
        assertEquals(device.get().deviceLocationDto().city(), cameraLog.getLocation().getCity());
        assertEquals(device.get().deviceLocationDto().country(), cameraLog.getLocation().getCountry());
        assertEquals(device.get().deviceLocationDto().site(), cameraLog.getLocation().getSiteId());
    }

    @Test
    void testIncorrectFilePath() {
        Transformer transformer = new Transformer(deviceCache);
        String sensorSerialNumber = sensorSerialNumbers.get(new Random().nextInt(sensorSerialNumbers.size()));
        Log rawLog = RawLogGenerator.randomLog().setDeviceSerialNumber(sensorSerialNumber)
                .setFilePath("/sample/").build();
        assertThrows(AssertionError.class, () -> transformer.transform(rawLog));
    }

    @Test
    void testIncorrectSerialNumber() {
        Transformer transformer = new Transformer(deviceCache);
        Log rawLog = RawLogGenerator.randomLog().setDeviceSerialNumber("").build();
        TransformResult transformResult = transformer.transform(rawLog);
        assertEquals(HARD, transformResult.getErrorType());
        assertEquals(List.of(INVALID_SERIAL_NUMBER), transformResult.getErrors());
        assertNotNull(transformResult.getLog());
    }

    @Test
    void testHardErrorTransformResult() {
        Transformer transformer = new Transformer(deviceCache);
        Log rawLog = RawLogGenerator.randomLog().setDeviceSerialNumber("INVALID_SN").build();
        TransformResult transformResult = transformer.transform(rawLog);
        assertEquals(HARD, transformResult.getErrorType());
        assertEquals(List.of(DEVICE_NOT_FOUND), transformResult.getErrors());
        assertNotNull(transformResult.getLog());
    }

}
