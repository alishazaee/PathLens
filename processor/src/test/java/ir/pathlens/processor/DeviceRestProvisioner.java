package ir.pathlens.processor;

import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.rest.controller.MockDeviceController;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class DeviceRestProvisioner {

    private final MockDeviceController mockDeviceController;

    public DeviceRestProvisioner(MockDeviceController mockDeviceController) {
        this.mockDeviceController = mockDeviceController;
    }

    public void populateMockServerWithDevices(List<String> sensorIds) {
        AtomicInteger id = new AtomicInteger(1);

        for (String sensorId : sensorIds) {
            int deviceId = id.getAndIncrement();
            mockDeviceController.addNewMockDevice(new DeviceResponseDto(
                    deviceId,
                    sensorId,
                    DeviceType.SPEED_CAMERA,
                    DeviceStatus.ACTIVE,
                    new LocationResponseDto("SITE-" + deviceId, "IRAN", "TEHRAN", 35.68f, 51.38f)));
        }
    }
}
