import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { EventDto, EventQueryParams, StatsDto, EventCountsDto } from '../models/event.model';
import {
  AlertRule, AlertRuleUpsertRequest, AlertStateSnapshot, AlertHistoryEntry,
  AlertSilence, AlertPreviewResult, MaintenanceWindow,
} from '../models/alert.model';
import { NodeDto } from '../models/node.model';
import { RetentionDto, RetentionRunResult } from '../models/retention.model';
import { DiagnosticsDto } from '../models/diagnostics.model';
import { ApiKeyDto, CreatedApiKeyDto, OAuthDomainDto, UserDto } from '../models/auth.model';
import { CompareTracesDto, LatencyServiceDto, SpanDto, SpanQueryParams, TraceQueryRequest, TraceRowDto, TraceStatsDto } from '../models/span.model';
import { MetricSeriesDto, MetricCatalogDto, MetricQueryRequest, HeatmapDto, ExemplarDto, MetricExprRequest } from '../models/metric.model';
import { SearchHistoryDto } from '../models/search-history.model';
import { AuthService } from './auth.service';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private http = inject(HttpClient);
  private auth = inject(AuthService);

  private tokenParam(): string {
    const t = this.auth.token;
    return t ? `?access_token=${encodeURIComponent(t)}` : '';
  }

  /** Stream historical events via SSE. Emits each EventDto as it arrives,
   *  completes when the server sends "event: done". */
  streamEvents(params: EventQueryParams = {}): Observable<EventDto> {
    return new Observable<EventDto>(subscriber => {
      const p = new URLSearchParams();
      if (params.filter)  p.set('filter', params.filter);
      if (params.from)    p.set('from', params.from);
      if (params.to)      p.set('to', params.to);
      if (params.count)   p.set('count', String(params.count));
      if (params.dir)     p.set('dir', params.dir);
      if (params.afterId) p.set('afterId', params.afterId);
      if (params.afterTs !== undefined) p.set('afterTs', String(params.afterTs));
      if (params.levels)  p.set('levels', params.levels);
      const token = this.auth.token;
      if (token) p.set('access_token', token);
      const es = new EventSource(`/api/events?${p.toString()}`);
      es.onmessage = event => {
        try { subscriber.next(JSON.parse(event.data) as EventDto); } catch { /* ignore */ }
      };
      es.addEventListener('done', () => { es.close(); subscriber.complete(); });
      es.onerror = () => { es.close(); subscriber.error(new Error('Failed to load events')); };
      return () => es.close();
    });
  }

  getStats(): Observable<StatsDto> {
    return this.http.get<StatsDto>('/api/stats');
  }

  getPropertyNames(): Observable<string[]> {
    return this.http.get<string[]>('/api/events/props');
  }

  getServiceNames(days = 7): Observable<string[]> {
    return this.http.get<string[]>(`/api/events/services?days=${days}`);
  }

  /**
   * Per-service and per-level event counts bucketed over time (Dashboard "Log events" chart).
   * The backend counts from event headers only, so the whole window is scanned cheaply — there
   * is no longer a `limit` to tune.
   */
  getEventCounts(params: { from?: string; to?: string; bucket?: number; service?: string } = {}): Observable<EventCountsDto> {
    const p = new URLSearchParams();
    if (params.from)    p.set('from',    params.from);
    if (params.to)      p.set('to',      params.to);
    if (params.bucket)  p.set('bucket',  String(params.bucket));
    if (params.service) p.set('service', params.service);
    return this.http.get<EventCountsDto>(`/api/events/counts?${p.toString()}`);
  }

  // ── Alerts ───────────────────────────────────────────────────────────────
  getAlerts(): Observable<AlertRule[]> {
    return this.http.get<AlertRule[]>('/api/alerts');
  }
  createAlert(req: AlertRuleUpsertRequest): Observable<AlertRule> {
    return this.http.post<AlertRule>('/api/alerts', req);
  }
  updateAlert(id: string, req: AlertRuleUpsertRequest): Observable<AlertRule> {
    return this.http.put<AlertRule>(`/api/alerts/${id}`, req);
  }
  deleteAlert(id: string): Observable<void> {
    return this.http.delete<void>(`/api/alerts/${id}`);
  }
  getAlertState(): Observable<AlertStateSnapshot[]> {
    return this.http.get<AlertStateSnapshot[]>('/api/alerts/state');
  }
  getAlertHistory(limit = 200): Observable<AlertHistoryEntry[]> {
    return this.http.get<AlertHistoryEntry[]>(`/api/alerts/history?limit=${limit}`);
  }
  getAlertSilences(): Observable<AlertSilence[]> {
    return this.http.get<AlertSilence[]>('/api/alerts/silences');
  }
  createAlertSilence(ruleId: string, minutes: number, reason?: string): Observable<AlertSilence> {
    return this.http.post<AlertSilence>('/api/alerts/silences', { ruleId, minutes, reason });
  }
  deleteAlertSilence(id: string): Observable<void> {
    return this.http.delete<void>(`/api/alerts/silences/${id}`);
  }
  getMaintenance(): Observable<MaintenanceWindow[]> {
    return this.http.get<MaintenanceWindow[]>('/api/alerts/maintenance');
  }
  createMaintenance(w: MaintenanceWindow): Observable<MaintenanceWindow> {
    return this.http.post<MaintenanceWindow>('/api/alerts/maintenance', w);
  }
  deleteMaintenance(id: string): Observable<void> {
    return this.http.delete<void>(`/api/alerts/maintenance/${id}`);
  }
  previewAlert(req: AlertRuleUpsertRequest): Observable<AlertPreviewResult> {
    return this.http.post<AlertPreviewResult>('/api/alerts/preview', req);
  }
  testAlert(req: AlertRuleUpsertRequest): Observable<{ sent: number }> {
    return this.http.post<{ sent: number }>('/api/alerts/test', req);
  }
  ackAlert(id: string): Observable<void> {
    return this.http.post<void>(`/api/alerts/${id}/ack`, {});
  }
  unackAlert(id: string): Observable<void> {
    return this.http.delete<void>(`/api/alerts/${id}/ack`);
  }

  // ── Search history (per-user) ───────────────────────────────────────────────
  getSearchHistory(): Observable<SearchHistoryDto> {
    return this.http.get<SearchHistoryDto>('/api/search-history');
  }
  recordSearch(query: string): Observable<void> {
    return this.http.post<void>('/api/search-history', { query });
  }
  pinSearch(query: string, pinned: boolean): Observable<void> {
    return this.http.put<void>('/api/search-history/pin', { query, pinned });
  }
  deleteSearch(query: string): Observable<void> {
    return this.http.delete<void>(`/api/search-history?query=${encodeURIComponent(query)}`);
  }

  getNodes(): Observable<NodeDto[]> {
    return this.http.get<NodeDto[]>('/api/nodes');
  }

  getRetention(): Observable<RetentionDto> {
    return this.http.get<RetentionDto>('/api/retention');
  }

  putRetention(dto: RetentionDto): Observable<RetentionDto> {
    return this.http.put<RetentionDto>('/api/retention', dto);
  }

  runRetention(): Observable<RetentionRunResult> {
    return this.http.post<RetentionRunResult>('/api/retention/run', null);
  }

  getDiagnostics(): Observable<DiagnosticsDto> {
    return this.http.get<DiagnosticsDto>('/api/diagnostics');
  }

  streamLive(filter?: string): Observable<EventDto> {
    return new Observable<EventDto>(subscriber => {
      const p = new URLSearchParams();
      if (filter) p.set('filter', filter);
      const token = this.auth.token;
      if (token) p.set('access_token', token);
      const es = new EventSource(`/api/events/live?${p.toString()}`);
      es.onmessage = event => {
        try {
          subscriber.next(JSON.parse(event.data) as EventDto);
        } catch { /* ignore parse errors */ }
      };
      es.onerror = () => subscriber.error(new Error('SSE connection lost'));
      return () => es.close();
    });
  }

  // ── Users ──────────────────────────────────────────────────────────────────
  getUsers(): Observable<UserDto[]>           { return this.http.get<UserDto[]>('/api/users'); }
  getUser(id: string): Observable<UserDto>    { return this.http.get<UserDto>(`/api/users/${encodeURIComponent(id)}`); }
  createUser(username: string, password: string, role: string): Observable<UserDto> {
    return this.http.post<UserDto>('/api/users', { username, password, role });
  }
  createOAuthUser(email: string, displayName: string, provider: string, role: string): Observable<UserDto> {
    return this.http.post<UserDto>('/api/users/oauth', { email, displayName, provider, role });
  }
  updateUserRole(id: string, role: string): Observable<void> {
    return this.http.patch<void>(`/api/users/${id}/role`, { role });
  }
  updateUser(id: string, displayName: string, role: string, permissions?: number): Observable<void> {
    return this.http.patch<void>(`/api/users/${encodeURIComponent(id)}`, { displayName, role, permissions });
  }
  changeUserPassword(id: string, password: string): Observable<void> {
    return this.http.patch<void>(`/api/users/${encodeURIComponent(id)}/password`, { password });
  }
  deleteUser(id: string): Observable<void>    { return this.http.delete<void>(`/api/users/${id}`); }

  // ── OAuth domain allowlist ──────────────────────────────────────────────────
  getOAuthDomains(): Observable<OAuthDomainDto[]> {
    return this.http.get<OAuthDomainDto[]>('/api/users/oauth-domains');
  }
  createOAuthDomain(domain: string, provider: string, role: string): Observable<OAuthDomainDto> {
    return this.http.post<OAuthDomainDto>('/api/users/oauth-domains', { domain, provider, role });
  }
  deleteOAuthDomain(id: string): Observable<void> {
    return this.http.delete<void>(`/api/users/oauth-domains/${encodeURIComponent(id)}`);
  }

  // ── API Keys ───────────────────────────────────────────────────────────────
  getApiKeys(): Observable<ApiKeyDto[]>       { return this.http.get<ApiKeyDto[]>('/api/auth/keys'); }
  createApiKey(name: string, description: string, permissions: number, key?: string): Observable<CreatedApiKeyDto> {
    return this.http.post<CreatedApiKeyDto>('/api/auth/keys', {
      name, description, permissions, key: key || null,
    });
  }
  deleteApiKey(id: string): Observable<void>  { return this.http.delete<void>(`/api/auth/keys/${id}`); }

  // ── Traces ─────────────────────────────────────────────────────────────────
  getTraceStats(from: string, to?: string): Observable<TraceStatsDto> {
    const p = new URLSearchParams({ from });
    if (to) p.set('to', to);
    return this.http.get<TraceStatsDto>(`/api/traces/stats?${p.toString()}`);
  }

  searchTraces(params: SpanQueryParams = {}): Observable<TraceRowDto[]> {
    const p = new URLSearchParams();
    if (params.from)           p.set('from',           params.from);
    if (params.to)             p.set('to',             params.to);
    if (params.service)        p.set('service',        params.service);
    if (params.spanName)       p.set('name',           params.spanName);
    if (params.status)         p.set('status',         params.status);
    if (params.minDurationMs)  p.set('minDurationMs',  String(params.minDurationMs));
    if (params.maxDurationMs)  p.set('maxDurationMs',  String(params.maxDurationMs));
    if (params.httpStatus)     p.set('httpStatus',     params.httpStatus);
    if (params.limit)          p.set('limit',          String(params.limit));
    return this.http.get<TraceRowDto[]>(`/api/traces?${p.toString()}`);
  }

  getTrace(traceId: string): Observable<SpanDto[]> {
    return this.http.get<SpanDto[]>(`/api/traces/${encodeURIComponent(traceId)}`);
  }

  queryTraces(req: TraceQueryRequest): Observable<TraceRowDto[]> {
    return this.http.post<TraceRowDto[]>('/api/traces/query', req);
  }

  getSpanLogs(spanId: string, from?: string, to?: string): Observable<EventDto[]> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to)   p.set('to', to);
    const qs = p.toString();
    return this.http.get<EventDto[]>(`/api/spans/${encodeURIComponent(spanId)}/logs${qs ? '?' + qs : ''}`);
  }

  /** All logs correlated to a trace (filtered on @tr). Primary trace↔logs view. */
  getTraceLogs(traceId: string, from?: string, to?: string): Observable<EventDto[]> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to)   p.set('to', to);
    const qs = p.toString();
    return this.http.get<EventDto[]>(`/api/traces/${encodeURIComponent(traceId)}/logs${qs ? '?' + qs : ''}`);
  }

  getFlamegraph(traceId: string): Observable<any> {
    return this.http.get<any>(`/api/traces/${encodeURIComponent(traceId)}/flamegraph`);
  }

  compareTraces(a: string, b: string): Observable<CompareTracesDto> {
    return this.http.get<CompareTracesDto>(
      `/api/traces/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
  }

  getLatency(from?: string, to?: string, service?: string): Observable<LatencyServiceDto[]> {
    const p = new URLSearchParams();
    if (from)    p.set('from',    from);
    if (to)      p.set('to',      to);
    if (service) p.set('service', service);
    return this.http.get<LatencyServiceDto[]>(`/api/traces/latency?${p.toString()}`);
  }

  getServiceGraph(from?: string, to?: string): Observable<{ nodes: any[]; edges: any[] }> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to)   p.set('to', to);
    return this.http.get<{ nodes: any[]; edges: any[] }>(`/api/traces/service-graph?${p.toString()}`);
  }

  // ── Metrics ────────────────────────────────────────────────────────────────
  getMetricNames(prefix?: string): Observable<string[]> {
    const p = prefix ? `?prefix=${encodeURIComponent(prefix)}` : '';
    return this.http.get<string[]>(`/api/metrics/names${p}`);
  }

  /** Full metric catalog with type/unit/labels/cardinality/last-seen. */
  getMetricCatalog(search?: string): Observable<MetricCatalogDto[]> {
    const p = search ? `?search=${encodeURIComponent(search)}` : '';
    return this.http.get<MetricCatalogDto[]>(`/api/metrics/catalog${p}`);
  }

  getMetricLabelKeys(name: string): Observable<string[]> {
    return this.http.get<string[]>(`/api/metrics/${encodeURIComponent(name)}/labels`);
  }

  getMetricLabelValues(name: string, key: string): Observable<string[]> {
    return this.http.get<string[]>(
      `/api/metrics/${encodeURIComponent(name)}/labels/${encodeURIComponent(key)}/values`);
  }

  /** Server-side typed aggregation (rate/quantile/sum-by/topk). */
  queryMetricAgg(req: MetricQueryRequest): Observable<MetricSeriesDto[]> {
    return this.http.post<MetricSeriesDto[]>('/api/metrics/query', req);
  }

  /** Binary metric expression (A op B → single series). */
  queryMetricExpr(req: MetricExprRequest): Observable<MetricSeriesDto> {
    return this.http.post<MetricSeriesDto>('/api/metrics/expr', req);
  }

  getMetricHeatmap(name: string, from?: string, to?: string, step?: string,
                   filters?: Record<string, string>): Observable<HeatmapDto> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to)   p.set('to',   to);
    if (step) p.set('step', step);
    if (filters) {
      const f = Object.entries(filters).map(([k, v]) => `${k}:${v}`).join(',');
      if (f) p.set('filters', f);
    }
    const qs = p.toString();
    return this.http.get<HeatmapDto>(`/api/metrics/${encodeURIComponent(name)}/heatmap${qs ? '?' + qs : ''}`);
  }

  /** Exemplars (sampled measurements linked to traces) for a metric. */
  getMetricExemplars(name: string, from?: string, to?: string,
                     filters?: Record<string, string>, limit = 200): Observable<ExemplarDto[]> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to)   p.set('to',   to);
    if (filters) {
      const f = Object.entries(filters).map(([k, v]) => `${k}:${v}`).join(',');
      if (f) p.set('filters', f);
    }
    p.set('limit', String(limit));
    return this.http.get<ExemplarDto[]>(`/api/metrics/${encodeURIComponent(name)}/exemplars?${p.toString()}`);
  }

  /** Raw series (no aggregation). */
  queryMetric(name: string, from?: string, to?: string, step?: string): Observable<MetricSeriesDto[]> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to)   p.set('to',   to);
    if (step) p.set('step', step);
    const qs = p.toString();
    return this.http.get<MetricSeriesDto[]>(`/api/metrics/${encodeURIComponent(name)}${qs ? '?' + qs : ''}`);
  }
}
