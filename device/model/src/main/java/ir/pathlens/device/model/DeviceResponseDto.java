package ir.pathlens.device.model;

/**
 * Data transfer object representing a device returned by the APIs.
 */
public record DeviceResponseDto(
        int id,
        String serialNumber,
        DeviceType deviceType,
        DeviceStatus status,
        LocationResponseDto deviceLocationDto) {

}
