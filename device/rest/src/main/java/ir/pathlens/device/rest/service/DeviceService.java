package ir.pathlens.device.rest.service;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceFilter;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.rest.db.tables.records.DeviceRecord;
import ir.pathlens.device.rest.db.tables.records.LocationsRecord;
import ir.pathlens.device.rest.repository.DeviceRepository;
import ir.pathlens.device.rest.repository.LocationRepository;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/**
 * The service layer defined for CRUD operations on device info.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class DeviceService {

    private final DeviceRepository deviceRepository;
    private final LocationRepository locationRepository;

    public DeviceResponseDto createDevice(DeviceCreateRequestDto request) {
        if (deviceRepository.findBySerialNumber(request.serialNumber()).isPresent()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "the device is already present: " + request.serialNumber());
        }
        LocationsRecord location = locationRepository.findById(request.siteId())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.BAD_REQUEST, "Location not found"));
        DeviceRecord saved = deviceRepository.insert(request);

        return toDto(saved, location);
    }

    public DeviceResponseDto getDevice(int id) {
        DeviceRecord record = deviceRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Device not found"));
        LocationsRecord location = locationRepository.findById(record.getSiteId())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));

        return toDto(record, location);
    }

    public long getRevisionNumber() {
        Optional<LocalDateTime> updatedAt = deviceRepository.findMaxUpdatedAt();
        if (updatedAt.isEmpty()) {
            return 0;
        }
        LocalDateTime localDateTime = updatedAt.get();
        ZoneId tehranZone = ZoneId.of("Asia/Tehran");
        ZoneOffset offset = tehranZone.getRules().getOffset(localDateTime);
        return localDateTime.toEpochSecond(offset);
    }

    public Page<DeviceResponseDto> getPaginatedDevices(DeviceFilter filter, Pageable pageable) {
        List<DeviceRecord> records = deviceRepository.findAll(filter, pageable);
        long total = deviceRepository.count(filter);
        Map<String, LocationsRecord> locations = locationRepository.findAllByIds(
                records.stream().map(DeviceRecord::getSiteId).collect(Collectors.toSet()));

        List<DeviceResponseDto> content = records.stream()
                .map(record -> toDto(record, locations.get(record.getSiteId())))
                .toList();
        PageImpl<DeviceResponseDto> page = new PageImpl<>(content, pageable, total);
        return new Page<>(page.getContent(), page.getNumber(), page.getSize(), page.getTotalElements(),
                page.getTotalPages());
    }

    public void deleteDevice(int id) {
        deviceRepository.deleteById(id);
    }

    private static DeviceResponseDto toDto(DeviceRecord record, LocationsRecord location) {
        LocationResponseDto locationDto = new LocationResponseDto(
                location.getSiteId(), location.getCountry(), location.getCity(),
                location.getLatitude() == null ? null : location.getLatitude().floatValue(),
                location.getLongitude() == null ? null : location.getLongitude().floatValue());

        return new DeviceResponseDto(
                record.getId(), record.getSerialNumber(),
                record.getDeviceType() == null ? null : DeviceType.valueOf(record.getDeviceType()),
                record.getStatus() == null ? null : DeviceStatus.valueOf(record.getStatus()),
                locationDto);
    }
}
