import type { ApiErrorBody, FieldError } from "./types";

export class ApiError extends Error {
  readonly status: number;
  readonly fieldErrors: FieldError[];

  constructor(message: string, status: number, fieldErrors: FieldError[] = []) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.fieldErrors = fieldErrors;
  }
}

export function toQueryString(params: object): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    search.set(key, String(value));
  }
  const qs = search.toString();
  return qs ? `?${qs}` : "";
}

async function request<T>(baseUrl: string, path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });

  if (response.status === 204) {
    return undefined as T;
  }

  const rawBody = await response.text();
  const parsed = rawBody ? safeJsonParse(rawBody) : undefined;

  if (!response.ok) {
    const errorBody = parsed as ApiErrorBody | undefined;
    throw new ApiError(
      errorBody?.message ?? response.statusText ?? "Request failed",
      response.status,
      errorBody?.fieldErrors ?? [],
    );
  }

  return parsed as T;
}

function safeJsonParse(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return { message: text };
  }
}

function makeClient(baseUrl: string) {
  return {
    get: <T>(path: string) => request<T>(baseUrl, path, { method: "GET" }),
    post: <T>(path: string, body?: unknown) =>
      request<T>(baseUrl, path, { method: "POST", body: body !== undefined ? JSON.stringify(body) : undefined }),
    put: <T>(path: string, body?: unknown) =>
      request<T>(baseUrl, path, { method: "PUT", body: body !== undefined ? JSON.stringify(body) : undefined }),
    patch: <T>(path: string) => request<T>(baseUrl, path, { method: "PATCH" }),
    delete: <T>(path: string) => request<T>(baseUrl, path, { method: "DELETE" }),
  };
}

// Same-origin paths. In production these are proxied by nginx (see frontend/nginx.conf.template)
// straight to the device-rest / alerting-rest Services, whose hostnames are supplied per
// environment via ConfigMap - never baked into this bundle. In dev, Vite's dev-server proxy
// (vite.config.ts) forwards them to whatever backend URLs are set in .env.local.
export const deviceClient = makeClient("/api/device");
export const alertingClient = makeClient("/api/alerting");
