import { deviceClient, toQueryString } from "./client";
import type {
  DeviceCreateRequestDto,
  DeviceResponseDto,
  DeviceStatus,
  DeviceType,
  DeviceUpdateRequestDto,
  Page,
} from "./types";

export interface DeviceListParams {
  page?: number;
  size?: number;
  serialNumber?: string;
  type?: DeviceType;
  justActiveDevices?: boolean;
}

export const devicesApi = {
  list: (params: DeviceListParams = {}) =>
    deviceClient.get<Page<DeviceResponseDto>>(`/devices${toQueryString(params)}`),
  get: (id: number) => deviceClient.get<DeviceResponseDto>(`/devices/${id}`),
  create: (dto: DeviceCreateRequestDto) => deviceClient.post<DeviceResponseDto>("/devices", dto),
  update: (id: number, dto: DeviceUpdateRequestDto) =>
    deviceClient.put<DeviceResponseDto>(`/devices/${id}`, dto),
  remove: (id: number) => deviceClient.delete<void>(`/devices/${id}`),
};

export type { DeviceStatus, DeviceType };
