import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { EventDto, EventQueryParams, StatsDto } from '../models/event.model';
import { AlertRule, AlertRuleUpsertRequest } from '../models/alert.model';
import { NodeDto } from '../models/node.model';
import { RetentionDto, RetentionRunResult } from '../models/retention.model';
import { DiagnosticsDto } from '../models/diagnostics.model';
import { ApiKeyDto, CreatedApiKeyDto, UserDto } from '../models/auth.model';
import { SpanDto, SpanQueryParams, TraceRowDto, TraceStatsDto } from '../models/span.model';
import { MetricSeriesDto } from '../models/metric.model';
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

  getSignals(): Observable<AlertRule[]> {
    return this.http.get<AlertRule[]>('/api/signals');
  }

  createSignal(req: AlertRuleUpsertRequest): Observable<AlertRule> {
    return this.http.post<AlertRule>('/api/signals', req);
  }

  updateSignal(id: string, req: AlertRuleUpsertRequest): Observable<AlertRule> {
    return this.http.put<AlertRule>(`/api/signals/${id}`, req);
  }

  deleteSignal(id: string): Observable<void> {
    return this.http.delete<void>(`/api/signals/${id}`);
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
  createUser(username: string, password: string, role: string): Observable<UserDto> {
    return this.http.post<UserDto>('/api/users', { username, password, role });
  }
  deleteUser(id: string): Observable<void>    { return this.http.delete<void>(`/api/users/${id}`); }

  // ── API Keys ───────────────────────────────────────────────────────────────
  getApiKeys(): Observable<ApiKeyDto[]>       { return this.http.get<ApiKeyDto[]>('/api/auth/keys'); }
  createApiKey(name: string, key?: string): Observable<CreatedApiKeyDto> {
    return this.http.post<CreatedApiKeyDto>('/api/auth/keys', { name, key: key || null });
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

  // ── Metrics ────────────────────────────────────────────────────────────────
  getMetricNames(prefix?: string): Observable<string[]> {
    const p = prefix ? `?prefix=${encodeURIComponent(prefix)}` : '';
    return this.http.get<string[]>(`/api/metrics/names${p}`);
  }

  queryMetric(name: string, from?: string, to?: string, step?: string): Observable<MetricSeriesDto[]> {
    const p = new URLSearchParams({ metric: name });
    if (from) p.set('from', from);
    if (to)   p.set('to',   to);
    if (step) p.set('step', step);
    return this.http.get<MetricSeriesDto[]>(`/api/metrics?${p.toString()}`);
  }
}
