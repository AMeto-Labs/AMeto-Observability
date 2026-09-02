import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, filter, map } from 'rxjs';
import { EventDto, EventQueryParams, StatsDto, EventCountsDto, AggregationDto } from '../models/event.model';
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
import { SearchHistoryDto, SearchScope } from '../models/search-history.model';
import { StreamEndDto, StreamFrame } from '../models/stream.model';
import { UpdateStatusDto } from '../models/update.model';
import { AuthService } from './auth.service';
import { appPath } from '../../shared/utils/app-url';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private http = inject(HttpClient);
  private auth = inject(AuthService);

  /**
   * Opens an SSE stream authorised by a single-use ticket instead of putting the
   * JWT in the URL (which would leak into proxy logs / history). Fetches the
   * ticket over the normal bearer-authenticated POST, then opens EventSource with
   * `?ticket=`. Returns a teardown that cancels the pending fetch and closes the
   * stream.
   */
  private openTicketedSse(
    path: string,
    params: URLSearchParams,
    wire: (es: EventSource) => void,
    onError: () => void,
  ): () => void {
    let es: EventSource | undefined;
    let cancelled = false;
    const sub = this.http.post<{ ticket: string }>('/api/auth/sse-ticket', {}).subscribe({
      next: ({ ticket }) => {
        if (cancelled) return;
        params.set('ticket', ticket);
        // EventSource is not an HttpClient call, so the interceptor never sees it — and a
        // relative URL would resolve against the current route, not against <base>.
        es = new EventSource(`${appPath(path)}?${params.toString()}`);
        wire(es);
      },
      error: () => { this.auth.verifySession(); onError(); },
    });
    return () => { cancelled = true; sub.unsubscribe(); es?.close(); };
  }

  /**
   * The shape every SSE-backed list on this client has: a ticketed EventSource, one JSON
   * object per `message` frame, a terminal `done`, and a terminal `query-error` carrying the
   * server's own sentence. Written once because the fourth copy is where copies start to
   * disagree — the third already differed only in its strings.
   *
   * <p>Emits {@link StreamFrame}s, not bare rows, because `done` is not only a signal to stop
   * listening: it carries WHY the stream ended (read out, or stopped at the row ceiling, and
   * whether part of the window was lost on the way). That payload used to be discarded here —
   * the `done` handler took no argument — so the same truncated window reached the user as a
   * red banner at one row ceiling and as nothing at all at another, purely because of which
   * limit the request happened to carry. A caller that only wants rows says so with
   * {@link rowsOnly}; a caller that must explain itself reads the ending frame.</p>
   *
   * @param opts.completeOnDone  false for an endless stream (the live tail never sends `done`).
   *        Such a stream never emits an `end` frame either — there is no ending to describe.
   * @param opts.explainSilentFailure
   *        Optional: consulted when the stream failed having delivered NOTHING, where the real
   *        reason is unreadable to the browser. Returns a canceller. Only the log streams have
   *        somewhere to ask.
   */
  private streamJson<T>(
    path: string,
    params: URLSearchParams,
    opts: {
      completeOnDone?: boolean;
      queryErrorMessage: string;
      connectionMessage: string;
      explainSilentFailure?: (report: (message: string) => void) => () => void;
    },
  ): Observable<StreamFrame<T>> {
    return new Observable<StreamFrame<T>>(subscriber => {
      // Copied per subscription: openTicketedSse writes `ticket` into it, and two
      // subscriptions to the same Observable must not share (or overwrite) one ticket.
      const p = new URLSearchParams(params);
      let delivered = false;
      let explain: (() => void) | undefined;
      const teardown = this.openTicketedSse(path, p,
        es => {
          es.onmessage = event => {
            delivered = true;
            try {
              subscriber.next({ kind: 'row', row: JSON.parse(event.data) as T });
            } catch { /* ignore parse errors */ }
          };
          if (opts.completeOnDone !== false)
            es.addEventListener('done', event => {
              es.close();
              // The ending goes out BEFORE the completion, so a subscriber reading it in its
              // `complete` handler has already been given it. Never inside a try around the
              // parse: a payload this client cannot read is an ending it cannot describe, not
              // an ending that did not happen, and the stream must still complete normally.
              subscriber.next({ kind: 'end', end: this.sseEndPayload(event) });
              subscriber.complete();
            });
          // Sent by the server when a query fails AFTER the stream opened (its budget ran
          // out, a segment could not be read). Not named 'error': EventSource dispatches
          // its own connection failures under that name.
          es.addEventListener('query-error', event => {
            es.close();
            subscriber.error(new Error(this.sseErrorMessage(event, opts.queryErrorMessage)));
          });
          es.onerror = () => {
            es.close();
            // SSE bypasses authInterceptor — re-check the session so a stale token logs out.
            this.auth.verifySession();
            if (delivered || !opts.explainSilentFailure) {
              subscriber.error(new Error(opts.connectionMessage));
              return;
            }
            // Nothing arrived: most likely the request was REFUSED (bad filter, bad date,
            // server saturated) — and EventSource cannot read the body of a non-200, so
            // ask instead of guessing.
            explain = opts.explainSilentFailure(msg => subscriber.error(new Error(msg)));
          };
        },
        () => subscriber.error(new Error(opts.connectionMessage)));
      return () => { explain?.(); teardown(); };
    });
  }

  /** Stream historical events via SSE. Emits each EventDto as it arrives,
   *  completes when the server sends "event: done". */
  streamEvents(params: EventQueryParams = {}): Observable<EventDto> {
    const p = new URLSearchParams();
    if (params.filter) p.set('filter', params.filter);
    if (params.from) p.set('from', params.from);
    if (params.to) p.set('to', params.to);
    if (params.count) p.set('count', String(params.count));
    if (params.dir) p.set('dir', params.dir);
    if (params.afterId) p.set('afterId', params.afterId);
    if (params.afterTs !== undefined) p.set('afterTs', String(params.afterTs));
    if (params.levels) p.set('levels', params.levels);
    return this.rowsOnly(this.streamJson<EventDto>('/api/events', p, {
      queryErrorMessage:  'Search failed',
      connectionMessage:  'Failed to load events',
      explainSilentFailure: report =>
        this.explainQueryFailure(params, 'Failed to load events', report),
    }));
  }

  /**
   * Reads the terminal `done` frame's payload. An empty object is the honest answer to every
   * way of not knowing — a frame with no data, a server old enough to send a bare `data: {}`
   * (which both log streams still do), a payload that is not JSON, or one that is JSON but not
   * an object. All four mean the same thing to a caller: this ending did not explain itself,
   * and every field is therefore absent rather than false.
   *
   * <p>An ARRAY is rejected with the rest, and not because reading one would throw — it would
   * not. `[1,2,3].reason` is simply undefined, so it would behave like an empty account while
   * being typed as something it is not, which is the kind of near-miss that survives until
   * someone adds a field and it stops being a near-miss.</p>
   */
  private sseEndPayload(event: Event): StreamEndDto {
    const data = (event as MessageEvent).data;
    if (typeof data !== 'string') return {};
    try {
      const parsed: unknown = JSON.parse(data);
      return parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
        ? parsed as StreamEndDto
        : {};
    } catch {
      return {};
    }
  }

  /**
   * Drops the ending frame, leaving the rows. For the two log streams, whose callers have
   * nowhere to put an ending: `streamEvents` still emits rows and completes on `done`, and
   * `streamLive` — which registers no `done` handler at all — still never completes.
   */
  private rowsOnly<T>(source: Observable<StreamFrame<T>>): Observable<T> {
    return source.pipe(
      filter((f): f is { kind: 'row'; row: T } => f.kind === 'row'),
      map(f => f.row),
    );
  }

  /** Reads the server's message out of a `query-error` frame, falling back to a default. */
  private sseErrorMessage(event: Event, fallback: string): string {
    const data = (event as MessageEvent).data;
    if (typeof data !== 'string') return fallback;
    try {
      const parsed = JSON.parse(data) as { error?: string };
      return parsed.error?.trim() || fallback;
    } catch {
      return fallback;
    }
  }

  /**
   * Asks why a stream that delivered nothing failed. A browser cannot read the body of a
   * non-200 SSE response, so the reason — a filter typo, a bad date — is only reachable
   * by asking again over a normal request. When the query itself is fine the failure was
   * operational (the server refused it while at its search limit, or the connection
   * broke), and the message says so instead of blaming the query.
   *
   * <p>Skipped entirely once the session is gone: the GET would be a guaranteed 401 and
   * would trigger a second logout on top of the one verifySession already started.</p>
   *
   * @returns a canceller for the in-flight request, so a torn-down stream leaves nothing running.
   */
  private explainQueryFailure(
    params: { filter?: string; from?: string; to?: string },
    connectionMessage: string,
    report: (message: string) => void,
  ): () => void {
    if (!this.auth.isAuthenticated()) { report(connectionMessage); return () => {}; }

    const p = new URLSearchParams();
    if (params.filter) p.set('filter', params.filter);
    if (params.from)   p.set('from', params.from);
    if (params.to)     p.set('to', params.to);

    const busy = 'The server is busy or unreachable. Try again in a moment.';
    const sub = this.http.get(`/api/events/validate?${p.toString()}`).subscribe({
      next:  () => report(busy),
      error: (err: { status?: number; error?: { error?: string } }) =>
        report(err?.status === 400
          ? (err.error?.error?.trim() || connectionMessage)
          : busy),
    });
    return () => sub.unsubscribe();
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
    if (params.from) p.set('from', params.from);
    if (params.to) p.set('to', params.to);
    if (params.bucket) p.set('bucket', String(params.bucket));
    if (params.service) p.set('service', params.service);
    return this.http.get<EventCountsDto>(`/api/events/counts?${p.toString()}`);
  }

  /**
   * Runs a `select … group by …` query. Plain JSON rather than SSE: the answer is a table with
   * its own columns, which cannot arrive one event at a time.
   */
  aggregate(params: { filter: string; from?: string; to?: string }): Observable<AggregationDto> {
    const p = new URLSearchParams();
    p.set('filter', params.filter);
    if (params.from) p.set('from', params.from);
    if (params.to)   p.set('to',   params.to);
    return this.http.get<AggregationDto>(`/api/events/aggregate?${p.toString()}`);
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
  getSearchHistory(scope: SearchScope): Observable<SearchHistoryDto> {
    return this.http.get<SearchHistoryDto>(`/api/search-history?scope=${scope}`);
  }
  recordSearch(query: string, scope: SearchScope): Observable<void> {
    return this.http.post<void>('/api/search-history', { query, scope });
  }
  pinSearch(query: string, pinned: boolean, scope: SearchScope): Observable<void> {
    return this.http.put<void>('/api/search-history/pin', { query, pinned, scope });
  }
  deleteSearch(query: string, scope: SearchScope): Observable<void> {
    return this.http.delete<void>(`/api/search-history?query=${encodeURIComponent(query)}&scope=${scope}`);
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

  // ── Software updates (Settings → Updates, admin) ─────────────────────────
  getUpdateStatus(): Observable<UpdateStatusDto> {
    return this.http.get<UpdateStatusDto>('/api/system/update');
  }
  checkForUpdates(): Observable<UpdateStatusDto> {
    return this.http.post<UpdateStatusDto>('/api/system/update/check', {});
  }
  /** Phase 1: download + verify the installer (progress via getUpdateStatus polling). */
  downloadUpdate(): Observable<{ message: string }> {
    return this.http.post<{ message: string }>('/api/system/update/download', {});
  }
  /** Phase 2 — explicit approval: run the verified installer (the server restarts). */
  applyUpdate(): Observable<{ message: string }> {
    return this.http.post<{ message: string }>('/api/system/update/apply', {});
  }

  /**
   * Opens the live tail. `levels` is the same comma-separated list the search sends — the
   * level selector used to apply to the history and be dropped as soon as the tail started,
   * so switching to live quietly widened the view back to every level.
   */
  streamLive(params: { filter?: string; levels?: string } = {}): Observable<EventDto> {
    const { filter, levels } = params;
    const p = new URLSearchParams();
    if (filter) p.set('filter', filter);
    if (levels) p.set('levels', levels);
    return this.rowsOnly(this.streamJson<EventDto>('/api/events/live', p, {
      // The tail has no end: it never sends `done`, and must never complete on its own.
      completeOnDone:     false,
      // It reports failure the same way the search does: a poll that blew its budget, a
      // server too busy to keep the tail fed, a failure after the stream opened.
      queryErrorMessage:  'Live tail stopped',
      connectionMessage:  'SSE connection lost',
      explainSilentFailure: report =>
        this.explainQueryFailure({ filter }, 'SSE connection lost', report),
    }));
  }

  // ── Users ──────────────────────────────────────────────────────────────────
  getUsers(): Observable<UserDto[]> { return this.http.get<UserDto[]>('/api/users'); }
  getUser(id: string): Observable<UserDto> { return this.http.get<UserDto>(`/api/users/${encodeURIComponent(id)}`); }
  createUser(username: string, password: string, role: string): Observable<UserDto> {
    return this.http.post<UserDto>('/api/users', { username, password, role });
  }
  createOAuthUser(email: string, displayName: string, provider: string, role: string, permissions?: number): Observable<UserDto> {
    return this.http.post<UserDto>('/api/users/oauth', { email, displayName, provider, role, permissions });
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
  deleteUser(id: string): Observable<void> { return this.http.delete<void>(`/api/users/${id}`); }

  // ── OAuth domain allowlist ──────────────────────────────────────────────────
  getOAuthDomains(): Observable<OAuthDomainDto[]> {
    return this.http.get<OAuthDomainDto[]>('/api/users/oauth-domains');
  }
  createOAuthDomain(domain: string, provider: string, role: string, permissions?: number): Observable<OAuthDomainDto> {
    return this.http.post<OAuthDomainDto>('/api/users/oauth-domains', { domain, provider, role, permissions });
  }
  updateOAuthDomain(id: string, role: string, permissions: number): Observable<void> {
    return this.http.patch<void>(`/api/users/oauth-domains/${encodeURIComponent(id)}`, { role, permissions });
  }
  deleteOAuthDomain(id: string): Observable<void> {
    return this.http.delete<void>(`/api/users/oauth-domains/${encodeURIComponent(id)}`);
  }

  // ── API Keys ───────────────────────────────────────────────────────────────
  getApiKeys(): Observable<ApiKeyDto[]> { return this.http.get<ApiKeyDto[]>('/api/auth/keys'); }
  createApiKey(name: string, description: string, permissions: number, key?: string): Observable<CreatedApiKeyDto> {
    return this.http.post<CreatedApiKeyDto>('/api/auth/keys', {
      name, description, permissions, key: key || null,
    });
  }
  updateApiKey(id: string, patch: { name?: string; description?: string; permissions?: number }): Observable<void> {
    return this.http.patch<void>(`/api/auth/keys/${encodeURIComponent(id)}`, patch);
  }
  deleteApiKey(id: string): Observable<void> { return this.http.delete<void>(`/api/auth/keys/${id}`); }

  // ── Traces ─────────────────────────────────────────────────────────────────
  getTraceStats(from: string, to?: string): Observable<TraceStatsDto> {
    const p = new URLSearchParams({ from });
    if (to) p.set('to', to);
    return this.http.get<TraceStatsDto>(`/api/traces/stats?${p.toString()}`);
  }

  /**
   * The filter-bar query string, shared by the buffered GET and the SSE stream so the two
   * cannot drift into disagreeing about what the same filter bar means. Note that `spanName`
   * goes over the wire as `name`, and that the falsy guards drop a literal 0 for the duration
   * bounds — kept deliberately: a 0 ms floor selects everything, so sending it changes nothing.
   * The row limit is NOT set here; the two endpoints spell it differently (`limit` vs `max`).
   */
  private traceListParams(params: SpanQueryParams): URLSearchParams {
    const p = new URLSearchParams();
    if (params.from) p.set('from', params.from);
    if (params.to) p.set('to', params.to);
    if (params.service) p.set('service', params.service);
    if (params.spanName) p.set('name', params.spanName);
    if (params.status) p.set('status', params.status);
    if (params.minDurationMs) p.set('minDurationMs', String(params.minDurationMs));
    if (params.maxDurationMs) p.set('maxDurationMs', String(params.maxDurationMs));
    if (params.httpStatus) p.set('httpStatus', params.httpStatus);
    return p;
  }

  searchTraces(params: SpanQueryParams = {}): Observable<TraceRowDto[]> {
    const p = this.traceListParams(params);
    if (params.limit) p.set('limit', String(params.limit));
    return this.http.get<TraceRowDto[]>(`/api/traces?${p.toString()}`);
  }

  /**
   * The filter-bar trace list, streamed row by row (newest first) instead of buffered into one
   * response. Same filters, same order, same rows as searchTraces — the list simply fills as
   * the server finds them rather than after it has found them all.
   *
   * <p>Keeps the ending frame (unlike the log streams): the list header has to say WHY it
   * stopped, and counting rows against the `max` it asked for cannot tell "I read the whole
   * window" from "your ceiling stopped me" from "your ceiling stopped me AND I could not read
   * part of the window".</p>
   */
  streamTraceList(params: SpanQueryParams & { max?: number } = {}): Observable<StreamFrame<TraceRowDto>> {
    const p = this.traceListParams(params);
    if (params.max) p.set('max', String(params.max));
    return this.streamJson<TraceRowDto>('/api/traces/stream', p, {
      queryErrorMessage: 'Search failed',
      connectionMessage: 'Failed to load traces',
    });
  }

  /**
   * Streams the rows of a TraceQL query. GET, with the query text in `?ql=`: EventSource
   * cannot POST and cannot carry a body, so the POST /api/traces/query shape is not available
   * here. A parse error arrives as a `query-error` frame on a 200 — a 400 would reach the
   * browser as an information-free connection failure with nothing to show the user.
   */
  streamTraceQuery(req: { query: string; from?: string; to?: string; max?: number })
    : Observable<StreamFrame<TraceRowDto>> {
    const p = new URLSearchParams();
    p.set('ql', req.query);
    if (req.from) p.set('from', req.from);
    if (req.to) p.set('to', req.to);
    if (req.max) p.set('max', String(req.max));
    return this.streamJson<TraceRowDto>('/api/traces/query/stream', p, {
      queryErrorMessage: 'Query error',
      connectionMessage: 'Failed to run query',
    });
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
    if (to) p.set('to', to);
    const qs = p.toString();
    return this.http.get<EventDto[]>(`/api/spans/${encodeURIComponent(spanId)}/logs${qs ? '?' + qs : ''}`);
  }

  /** All logs correlated to a trace (filtered on @tr). Primary trace↔logs view. */
  getTraceLogs(traceId: string, from?: string, to?: string): Observable<EventDto[]> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to) p.set('to', to);
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
    if (from) p.set('from', from);
    if (to) p.set('to', to);
    if (service) p.set('service', service);
    return this.http.get<LatencyServiceDto[]>(`/api/traces/latency?${p.toString()}`);
  }

  getServiceGraph(from?: string, to?: string): Observable<{ nodes: any[]; edges: any[] }> {
    const p = new URLSearchParams();
    if (from) p.set('from', from);
    if (to) p.set('to', to);
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
    if (to) p.set('to', to);
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
    if (to) p.set('to', to);
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
    if (to) p.set('to', to);
    if (step) p.set('step', step);
    const qs = p.toString();
    return this.http.get<MetricSeriesDto[]>(`/api/metrics/${encodeURIComponent(name)}${qs ? '?' + qs : ''}`);
  }
}
