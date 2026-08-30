export interface Page<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}

export interface FieldError {
  field: string;
  message: string;
}

export interface ApiErrorBody {
  timestamp: string;
  status: number;
  error: string;
  message: string;
  path: string;
  fieldErrors: FieldError[];
}

export type DeviceType = "RED_LIGHT_CAMERA" | "SPEED_CAMERA" | "TOLL_CAMERA";
export type DeviceStatus = "ACTIVE" | "INACTIVE";

export const DEVICE_TYPES: DeviceType[] = ["RED_LIGHT_CAMERA", "SPEED_CAMERA", "TOLL_CAMERA"];
export const DEVICE_STATUSES: DeviceStatus[] = ["ACTIVE", "INACTIVE"];

export interface LocationResponseDto {
  site: string;
  country: string | null;
  city: string | null;
  latitude: number | null;
  longitude: number | null;
}

export interface LocationCreateDto {
  site: string;
  country: string;
  city: string;
  latitude?: number | null;
  longitude?: number | null;
}

export interface LocationUpdateDto {
  country: string;
  city: string;
  latitude?: number | null;
  longitude?: number | null;
}

export interface DeviceResponseDto {
  id: number;
  serialNumber: string;
  deviceType: DeviceType;
  status: DeviceStatus;
  deviceLocationDto: LocationResponseDto;
}

export interface DeviceCreateRequestDto {
  serialNumber: string;
  type: DeviceType;
  status: DeviceStatus;
  siteId: string;
}

export interface DeviceUpdateRequestDto {
  type: DeviceType;
  status: DeviceStatus;
  siteId: string;
}

export type IdentityType = "PhoneNumber" | "PlateNumber";
export type RuleType = "Enter" | "Exit";

export const IDENTITY_TYPES: IdentityType[] = ["PhoneNumber", "PlateNumber"];
export const RULE_TYPES: RuleType[] = ["Enter", "Exit"];

export interface IdentityWrapper {
  identityType: IdentityType;
  identityValue: string;
}

export interface Rule {
  id: string;
  title: string;
  geometryWkt: string;
  expiresAt: string;
  identity: IdentityWrapper;
  isActive: boolean;
  ruleType: RuleType;
  isViolated: boolean;
  createdAt: string;
}

export interface RuleCreateDto {
  title?: string | null;
  geometryWkt: string;
  expiresAt: string;
  identity: IdentityWrapper;
  ruleType: RuleType;
}

export type RuleUpdateDto = RuleCreateDto;

export interface Notification {
  id: string;
  createdAt: string;
  message: string;
  ruleId: string;
  seen: boolean;
  isActive: boolean;
}
