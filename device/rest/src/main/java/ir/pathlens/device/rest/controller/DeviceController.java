package ir.pathlens.device.rest.controller;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.model.ApiPathConstants;
import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceFilter;
import ir.pathlens.device.model.DeviceResponseDto;
import ir.pathlens.device.rest.service.DeviceService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for device management.
 */
@RestController
@RequiredArgsConstructor
public class DeviceController {

    private final DeviceService deviceService;

    @PostMapping(ApiPathConstants.CREATE_DEVICE_PATH)
    @ResponseStatus(HttpStatus.CREATED)
    public DeviceResponseDto createDevice(@RequestBody @Valid DeviceCreateRequestDto request) {
        return deviceService.createDevice(request);
    }

    @GetMapping(ApiPathConstants.GET_DEVICE_PATH)
    public DeviceResponseDto getDevice(@PathVariable int id) {
        return deviceService.getDevice(id);
    }

    @GetMapping(ApiPathConstants.GET_REVISION_NUMBER)
    public Long getRevisionNumber() {
        return deviceService.getRevisionNumber();
    }

    @GetMapping(ApiPathConstants.GET_DEVICES_PATH)
    public Page<DeviceResponseDto> getDevices(@ParameterObject DeviceFilter filter,
            @ParameterObject Pageable pageable) {
        return deviceService.getPaginatedDevices(filter, pageable);
    }

    @DeleteMapping(ApiPathConstants.DELETE_DEVICE_PATH)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteDevice(@PathVariable int id) {
        deviceService.deleteDevice(id);
    }
}

