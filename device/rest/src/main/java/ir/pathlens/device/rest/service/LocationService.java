package ir.pathlens.device.rest.service;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.device.model.LocationResponseDto;
import ir.pathlens.device.rest.db.tables.records.DeviceRecord;
import ir.pathlens.device.rest.db.tables.records.LocationsRecord;
import ir.pathlens.device.rest.repository.DeviceRepository;
import ir.pathlens.device.rest.repository.LocationRepository;
import java.util.List;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/**
 * The service layer defined for CRUD operations on device location.
 */
@Service
@RequiredArgsConstructor
@Transactional
public class LocationService {

    private final LocationRepository locationRepository;
    private final DeviceRepository deviceRepository;

    public LocationResponseDto createLocation(LocationCreateDto request) {
        if (locationRepository.existsById(request.site())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Location already exists: " + request.site());
        }

        LocationsRecord saved = locationRepository.insertIfAbsent(request);
        if (saved == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Location already exists: " + request.site());
        }

        return toDto(saved);
    }

    public LocationResponseDto getLocation(String siteId) {
        LocationsRecord record = locationRepository.findById(siteId).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found: " + siteId));

        return toDto(record);
    }

    public void deleteLocation(String siteId) {
        List<DeviceRecord> devices = deviceRepository.findBySiteId(siteId);
        if (!devices.isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "These devices use this location: " + devices.stream()
                            .map(DeviceRecord::getSerialNumber)
                            .collect(Collectors.joining(", ")));
        }
        locationRepository.deleteById(siteId);
    }

    public Page<LocationResponseDto> getPaginatedLocations(Pageable pageable) {
        List<LocationsRecord> records = locationRepository.findAll(pageable);
        long total = locationRepository.count();

        PageImpl<LocationResponseDto> page =
                new PageImpl<>(records.stream().map(LocationService::toDto).toList(), pageable, total);
        return new Page<>(page.getContent(), page.getNumber(), page.getSize(), page.getTotalElements(),
                page.getTotalPages());
    }

    private static LocationResponseDto toDto(LocationsRecord record) {
        return new LocationResponseDto(
                record.getSiteId(), record.getCountry(), record.getCity(),
                record.getLatitude() == null ? null : record.getLatitude().floatValue(),
                record.getLongitude() == null ? null : record.getLongitude().floatValue());
    }
}
