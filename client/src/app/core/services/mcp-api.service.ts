import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { Observable, TimeoutError, map } from 'rxjs';
import {
  McpActivityPage, McpActivityQuery, McpFieldError, McpLimits, McpOfferedTool, McpOverview, McpPreview,
  McpProfile, McpProfileInput, McpProfilePatch, McpPublishedTool, McpRule, McpRuleInput, McpSummary, McpToolInput,
  McpToolList, McpToolTestResult, McpUsage,
} from '../models/mcp.model';
import { parseExactJson } from '../../shared/utils/exact-json';

/** What `PUT /api/mcp/settings` takes; a field left out keeps its value. */
export type McpSettingsPatch = Partial<McpLimits> & {
  enabled?: boolean;
  /** Detector ids to leave switched on. */
  detectors?: string[];
  /** Names of the built-in tools switched off for the whole server. */
  disabledTools?: string[];
};

/**
 * The settings API of Settings → AI / MCP (`/api/mcp`, docs/MCP.md). Its own service rather
 * than more methods on ApiService: nothing outside that one section calls it.
 */
@Injectable({ providedIn: 'root' })
export class McpApiService {
  private readonly http = inject(HttpClient);

  getOverview(): Observable<McpOverview> {
    return this.http.get<McpOverview>('/api/mcp');
  }

  /** Answers the whole section again, as it stands after the change. */
  updateSettings(patch: McpSettingsPatch): Observable<McpOverview> {
    return this.http.put<McpOverview>('/api/mcp/settings', patch);
  }

  getSummary(hours = 24): Observable<McpSummary> {
    return this.http.get<McpSummary>('/api/mcp/summary', { params: { hours } });
  }

  rotateSecret(): Observable<{ secretCreatedAt: string }> {
    return this.http.post<{ secretCreatedAt: string }>('/api/mcp/secret/rotate', {});
  }

  // ── Profiles ────────────────────────────────────────────────────────────────

  getProfiles(): Observable<McpProfile[]> {
    return this.http.get<McpProfile[]>('/api/mcp/profiles');
  }

  /** The key is in this answer and nowhere else, ever. */
  createProfile(input: McpProfileInput): Observable<{ profile: McpProfile; key: string }> {
    return this.http.post<{ profile: McpProfile; key: string }>('/api/mcp/profiles', input);
  }

  /** Changes the fields `input` names; the server keeps the others as they are. */
  updateProfile(id: string, input: McpProfilePatch): Observable<McpProfile> {
    return this.http.put<McpProfile>(`/api/mcp/profiles/${encodeURIComponent(id)}`, input);
  }

  rotateProfileKey(id: string): Observable<{ key: string }> {
    return this.http.post<{ key: string }>(`/api/mcp/profiles/${encodeURIComponent(id)}/rotate-key`, {});
  }

  deleteProfile(id: string): Observable<void> {
    return this.http.delete<void>(`/api/mcp/profiles/${encodeURIComponent(id)}`);
  }

  /** The tools the profile's key is offered, as `tools/list` answers it. */
  getOfferedTools(profileId: string): Observable<{ tools: McpOfferedTool[] }> {
    return this.http.get<{ tools: McpOfferedTool[] }>(`/api/mcp/profiles/${encodeURIComponent(profileId)}/tools`);
  }

  // ── Field rules ─────────────────────────────────────────────────────────────

  getRules(): Observable<McpRule[]> {
    return this.http.get<McpRule[]>('/api/mcp/rules');
  }

  createRule(rule: McpRuleInput): Observable<McpRule> {
    return this.http.post<McpRule>('/api/mcp/rules', rule);
  }

  updateRule(id: string, rule: McpRuleInput): Observable<McpRule> {
    return this.http.put<McpRule>(`/api/mcp/rules/${encodeURIComponent(id)}`, rule);
  }

  deleteRule(id: string): Observable<void> {
    return this.http.delete<void>(`/api/mcp/rules/${encodeURIComponent(id)}`);
  }

  /**
   * The most recent event matching `filter`, as stored and as an answer would carry it. Read as
   * text and parsed here: the stored event is the log's own, and a 64-bit value in it — hidden
   * from the answer, so shown nowhere else — keeps its digits (see `parseExactJson`).
   */
  preview(body: { filter?: string; mode?: string; profileId?: string }): Observable<McpPreview> {
    return this.http.post('/api/mcp/preview', body, { responseType: 'text' })
      .pipe(map(text => parseExactJson(text) as McpPreview));
  }

  // ── Tools ───────────────────────────────────────────────────────────────────

  getTools(): Observable<McpToolList> {
    return this.http.get<McpToolList>('/api/mcp/tools');
  }

  createTool(tool: McpToolInput): Observable<McpPublishedTool> {
    return this.http.post<McpPublishedTool>('/api/mcp/tools', tool);
  }

  updateTool(id: string, tool: McpToolInput): Observable<McpPublishedTool> {
    return this.http.put<McpPublishedTool>(`/api/mcp/tools/${encodeURIComponent(id)}`, tool);
  }

  deleteTool(id: string): Observable<void> {
    return this.http.delete<void>(`/api/mcp/tools/${encodeURIComponent(id)}`);
  }

  /** Runs a definition — saved or not — as the profile would get it. Saves and records nothing. */
  testTool(body: { tool: McpToolInput; id?: string; profileId: string; arguments: Record<string, unknown> }): Observable<McpToolTestResult> {
    return this.http.post<McpToolTestResult>('/api/mcp/tools/test', body);
  }

  getUsage(days = 7): Observable<McpUsage> {
    return this.http.get<McpUsage>('/api/mcp/tools/usage', { params: { days } });
  }

  // ── Activity ────────────────────────────────────────────────────────────────

  getActivity(query: McpActivityQuery = {}): Observable<McpActivityPage> {
    let params = new HttpParams();
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined && value !== null && value !== '') params = params.set(key, String(value));
    }
    return this.http.get<McpActivityPage>('/api/mcp/activity', { params });
  }
}

/**
 * The server's own sentence for a refused change, and the field it names: every `400` of this
 * API carries both. A request that was given up on waiting says so. Anything else — a dropped
 * connection, a `500` — gets `fallback`.
 */
export function mcpError(e: unknown, fallback: string): McpFieldError {
  if (e instanceof HttpErrorResponse) {
    // A request read as text (the preview) has its refusal as text too.
    const body = (typeof e.error === 'string' ? jsonOrNull(e.error) : e.error) as Partial<McpFieldError> | null;
    if (body && typeof body.error === 'string' && body.error) {
      return { error: body.error, field: typeof body.field === 'string' ? body.field : undefined };
    }
    if (e.status === 403) return { error: 'Only an administrator can change this.' };
  }
  if (e instanceof TimeoutError) return { error: 'The server did not answer in time.' };
  return { error: fallback };
}

function jsonOrNull(text: string): unknown {
  try { return JSON.parse(text); } catch { return null; }
}
