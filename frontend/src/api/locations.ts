import { deviceClient, toQueryString } from "./client";
import type { LocationCreateDto, LocationResponseDto, LocationUpdateDto, Page } from "./types";

export interface LocationListParams {
  page?: number;
  size?: number;
}

export const locationsApi = {
  list: (params: LocationListParams = {}) =>
    deviceClient.get<Page<LocationResponseDto>>(`/locations${toQueryString(params)}`),
  get: (site: string) => deviceClient.get<LocationResponseDto>(`/locations/${encodeURIComponent(site)}`),
  create: (dto: LocationCreateDto) => deviceClient.post<LocationResponseDto>("/locations", dto),
  update: (site: string, dto: LocationUpdateDto) =>
    deviceClient.put<LocationResponseDto>(`/locations/${encodeURIComponent(site)}`, dto),
  remove: (site: string) => deviceClient.delete<void>(`/locations/${encodeURIComponent(site)}`),
};
