import { ApiError } from "./client";

export function fieldErrorMap(error: unknown): Record<string, string> {
  if (error instanceof ApiError) {
    return Object.fromEntries(error.fieldErrors.map((f) => [f.field, f.message]));
  }
  return {};
}

export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message;
  }
  if (error instanceof Error) {
    return error.message;
  }
  return "An unexpected error occurred";
}
