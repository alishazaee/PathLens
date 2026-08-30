import { alertingClient, toQueryString } from "./client";
import type { Notification, Page } from "./types";

export interface NotificationListParams {
  page?: number;
  size?: number;
  seen?: boolean;
  ruleId?: string;
  createdAfter?: string;
}

export const notificationsApi = {
  list: (params: NotificationListParams = {}) =>
    alertingClient.get<Page<Notification>>(`/notifications${toQueryString(params)}`),
  get: (id: string) => alertingClient.get<Notification>(`/notifications/${id}`),
  markSeen: (id: string) => alertingClient.patch<Notification>(`/notifications/${id}/seen`),
};
