package ir.pathlens.processor;

import static ir.pathlens.proto.CameraLogProto.Error.DEVICE_NOT_FOUND;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_CITY;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_COUNTRY;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_DEVICE_TYPE;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_LATITUDE;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_LONGITUDE;
import static ir.pathlens.proto.CameraLogProto.Error.INVALID_SITE_ID;
import static ir.pathlens.proto.CameraLogProto.ErrorType.HARD;
import static ir.pathlens.proto.CameraLogProto.ErrorType.SOFT;

import ir.pathlens.device.cache.DeviceCache;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.proto.CameraLogProto;
import ir.pathlens.proto.CameraLogProto.Device;
import ir.pathlens.proto.CameraLogProto.DeviceType;
import ir.pathlens.proto.CameraLogProto.Location;
import ir.pathlens.proto.CameraLogProto.Log;
import java.util.Optional;

/**
 * Enriches a camera log with device and location data from the device cache.
 */
public class Enricher {

    private final DeviceCache deviceCache;

    public Enricher(DeviceCache deviceCache) {
        this.deviceCache = deviceCache;
    }

    public void enrich(Log.Builder builder, String serialNumber) {

        Optional<DeviceResponseDto> deviceOpt =
                deviceCache.findByDeviceSerialNumber(serialNumber);

        if (deviceOpt.isEmpty()) {
            builder.setErrorType(HARD).addErrorSummary(DEVICE_NOT_FOUND);
            return;
        }

        DeviceResponseDto deviceInfo = deviceOpt.get();

        enrichDeviceFields(builder, deviceInfo, serialNumber);
        enrichLocationFields(builder, deviceInfo);

    }

    private void enrichDeviceFields(Log.Builder builder, DeviceResponseDto deviceInfo, String serialNumber) {
        Device.Builder deviceBuilder = Device.newBuilder();
        deviceBuilder.setSerialNumber(serialNumber);
        if (deviceInfo.deviceType() == null) {
            softError(builder, INVALID_DEVICE_TYPE);
        } else {
            deviceBuilder.setDeviceType(mapDeviceType(deviceInfo.deviceType()));
        }

        deviceBuilder.setDeviceStatus(mapDeviceStatus(deviceInfo.status()));

        builder.setDevice(deviceBuilder.build());
    }

    private void enrichLocationFields(Log.Builder builder, DeviceResponseDto deviceInfo) {

        LocationResponseDto location = deviceInfo.deviceLocationDto();
        Location.Builder locationBuilder = Location.newBuilder();

        if (location.site() == null) {
            hardError(builder, INVALID_SITE_ID);
            return;
        }
        locationBuilder.setSiteId(location.site());

        if (location.latitude() == null) {
            hardError(builder, INVALID_LATITUDE);
            return;
        }
        locationBuilder.setLatitude(location.latitude());

        if (location.longitude() == null) {
            hardError(builder, INVALID_LONGITUDE);
            return;
        }
        locationBuilder.setLongitude(location.longitude());

        if (location.city() == null) {
            softError(builder, INVALID_CITY);
        } else {
            locationBuilder.setCity(location.city());
        }

        if (location.country() == null) {
            softError(builder, INVALID_COUNTRY);
        } else {
            locationBuilder.setCountry(location.country());
        }

        builder.setLocation(locationBuilder.build());
    }

    private void softError(Log.Builder builder, CameraLogProto.Error error) {
        builder.setErrorType(SOFT).addErrorSummary(error);
    }

    private void hardError(Log.Builder builder, CameraLogProto.Error error) {
        builder.setErrorType(HARD).addErrorSummary(error);
    }

    private CameraLogProto.DeviceStatus mapDeviceStatus(DeviceStatus status) {
        return switch (status) {
            case ACTIVE -> CameraLogProto.DeviceStatus.ACTIVE;
            case INACTIVE -> CameraLogProto.DeviceStatus.INACTIVE;
        };
    }

    private CameraLogProto.DeviceType mapDeviceType(ir.pathlens.device.model.DeviceType type) {
        return switch (type) {
            case SPEED_CAMERA -> DeviceType.SPEED_CAMERA;
            case TOLL_CAMERA -> DeviceType.TOLL_CAMERA;
            case RED_LIGHT_CAMERA -> DeviceType.RED_LIGHT_CAMERA;
        };
    }
}
