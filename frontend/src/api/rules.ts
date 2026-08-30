import { alertingClient, toQueryString } from "./client";
import type { Page, Rule, RuleCreateDto, RuleUpdateDto } from "./types";

export interface RuleListParams {
  page?: number;
  size?: number;
  title?: string;
  isActive?: boolean;
}

export const rulesApi = {
  list: (params: RuleListParams = {}) => alertingClient.get<Page<Rule>>(`/rules${toQueryString(params)}`),
  get: (id: string) => alertingClient.get<Rule>(`/rules/${id}`),
  create: (dto: RuleCreateDto) => alertingClient.post<Rule>("/rules", dto),
  update: (id: string, dto: RuleUpdateDto) => alertingClient.put<Rule>(`/rules/${id}`, dto),
  remove: (id: string) => alertingClient.delete<void>(`/rules/${id}`),
  activate: (id: string) => alertingClient.patch<Rule>(`/rules/${id}/activate`),
  deactivate: (id: string) => alertingClient.patch<Rule>(`/rules/${id}/deactivate`),
};
