import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { HttpErrorResponse, provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { TimeoutError } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { McpOverview, McpProfile, McpProfileInput, McpPublishedTool, McpToolInput } from '../../../../core/models/mcp.model';
import { mcpError } from '../../../../core/services/mcp-api.service';
import { McpSectionStore, SETTINGS_CHANGE_TIMEOUT_MS } from './mcp.store';

const OVERVIEW: McpOverview = {
  path: '/mcp',
  enabled: true,
  secretCreatedAt: '2026-11-10T08:00:00Z',
  limits: { responseBudgetTokens: 8000, maxWindowMinutes: 10080, maxEvents: 500, callsPerMinute: 60, maxConcurrent: 2 },
  detectors: [
    { id: 'cardNumber', name: 'Card number', on: true },
    { id: 'email', name: 'Email address', on: true },
    { id: 'iban', name: 'IBAN', on: false },
  ],
  tools: [
    { name: 'logs_overview', title: 'Log overview', description: 'The shape of a window.', takesFreeFilter: false, enabled: true },
    { name: 'logs_search', title: 'Log search', description: 'Events by filter.', takesFreeFilter: true, enabled: true },
  ],
};

const P1: McpProfile = {
  id: 'p1', name: 'Payments team', description: '', keyPreview: '7a3f09c1…',
  toolsMode: 'builtInAndPublished', tools: ['logs_overview', 'logs_search'], services: [], maskingMode: 'standard',
  freeFilters: true, maxEvents: 200, maxWindowMinutes: 1440, callsPerMinute: 30, publishedTools: ['t1'],
  createdBy: 'admin', createdAt: '2026-11-10T08:00:00Z', lastCallAt: null,
};

const T1: McpPublishedTool = {
  id: 't1', name: 'payment_declines', description: 'Declined payments of one merchant.',
  parameters: [], filter: "@l = 'Error'", fields: [], defaultWindowMinutes: 60, maxEvents: 200, order: 'newest',
  published: true, profiles: ['p1'], createdBy: 'admin', createdAt: '2026-11-10T08:00:00Z', updatedAt: '2026-11-10T08:00:00Z',
  inputSchema: { properties: { from: {}, to: {}, cursor: {} } },
};

const SUMMARY = {
  hours: 24, partial: false, calls: 1284, profiles: 5, events: 48310, medianEvents: 38, masked: 212406,
  maskedBy: { rules: 199662, strict: 0, detectors: 12744 }, statuses: { OK: 1270, Refused: 7 },
  refusals: { limit: 5, masking: 2 }, rules: { r1: 18204 }, detectors: { email: 1902 },
};

const PROFILE_INPUT: McpProfileInput = {
  name: 'Support', description: '', toolsMode: 'builtInAndPublished', tools: ['logs_search'], services: [],
  maskingMode: 'standard', freeFilters: true, maxEvents: 200, maxWindowMinutes: 1440, callsPerMinute: 30,
};

const TOOL_INPUT: McpToolInput = {
  name: T1.name, description: T1.description, parameters: [], filter: T1.filter, fields: [],
  defaultWindowMinutes: 60, maxEvents: 200, order: 'newest', published: true, profiles: [],
};

const BUSY = { status: 503, statusText: 'Service Unavailable' };

/**
 * The store of Settings → AI / MCP on its own, against a mocked API: what a change leaves in
 * the state, in which order changes reach the server, and what a read that fails may and may
 * not take with it.
 */
describe('Settings → AI / MCP — the store', () => {
  let http: HttpTestingController;
  let store: InstanceType<typeof McpSectionStore>;

  /** Lets the promises a change is made of run up to its next request. */
  const turn = (): Promise<void> => new Promise<void>(resolve => setTimeout(resolve));

  beforeEach(async () => {
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([]),
        McpSectionStore,
      ],
    });
    http = TestBed.inject(HttpTestingController);
    store = TestBed.inject(McpSectionStore);

    const loading = store.load();
    http.expectOne('/api/mcp').flush(OVERVIEW);
    http.expectOne('/api/mcp/profiles').flush([P1]);
    http.expectOne('/api/mcp/rules').flush([]);
    http.expectOne('/api/mcp/tools').flush({ builtIn: OVERVIEW.tools, published: [T1] });
    await loading;
    http.expectOne(r => r.url === '/api/mcp/summary').flush(SUMMARY);
    http.expectOne(r => r.url === '/api/mcp/tools/usage').flush({ days: 7, partial: false, calls: { logs_search: 2961 } });
    http.expectOne(r => r.url.startsWith('/api/events/services')).flush([]);
  });

  // Every request a case causes is one it has accounted for — and a case that has not fails
  // alone: the test bed is taken down here whatever the answer, so the next one starts clean.
  afterEach(() => {
    vi.useRealTimers();
    try {
      http.verify();
    } finally {
      TestBed.resetTestingModule();
    }
  });

  /** The read of both lists that follows a change, answered. */
  function answerLists(profiles: McpProfile[], tools: McpPublishedTool[]): void {
    http.expectOne('/api/mcp/profiles').flush(profiles);
    http.expectOne('/api/mcp/tools').flush({ builtIn: OVERVIEW.tools, published: tools });
  }

  /** The same read, failing: the profiles are refused, which abandons the tools. */
  function failLists(): void {
    http.expectOne('/api/mcp/profiles').flush({ error: 'The store is busy.' }, BUSY);
    expect(http.expectOne('/api/mcp/tools').cancelled).toBe(true);
  }

  // ── The switches ────────────────────────────────────────────────────────────

  it('sends two tool switches one after the other, the second with the first one in it', async () => {
    const first = store.setBuiltInTool('logs_search', false);
    const second = store.setBuiltInTool('logs_overview', false);
    await turn();

    // One request, not two: the second list is not worked out until the first is answered.
    const put1 = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put1.request.body).toEqual({ disabledTools: ['logs_search'] });
    put1.flush({ ...OVERVIEW, tools: OVERVIEW.tools.map(t => ({ ...t, enabled: t.name !== 'logs_search' })) });
    await first;
    await turn();

    const put2 = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put2.request.body).toEqual({ disabledTools: ['logs_overview', 'logs_search'] });
    put2.flush({ ...OVERVIEW, tools: OVERVIEW.tools.map(t => ({ ...t, enabled: false })) });
    await second;

    expect(store.overview()?.tools.map(t => t.enabled)).toEqual([false, false]);
  });

  it('sends two detector switches one after the other, the second with the first one in it', async () => {
    const first = store.setDetector('email', false);
    const second = store.setDetector('iban', true);
    await turn();

    const put1 = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put1.request.body).toEqual({ detectors: ['cardNumber'] });
    put1.flush({ ...OVERVIEW, detectors: OVERVIEW.detectors.map(d => ({ ...d, on: d.id === 'cardNumber' })) });
    await first;
    await turn();

    const put2 = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put2.request.body).toEqual({ detectors: ['cardNumber', 'iban'] });
    put2.flush({ ...OVERVIEW, detectors: OVERVIEW.detectors.map(d => ({ ...d, on: d.id !== 'email' })) });
    await second;

    expect(store.overview()?.detectors.filter(d => d.on).map(d => d.id)).toEqual(['cardNumber', 'iban']);
  });

  it('a switch the server refuses rejects with the response, and does not hold up the next one', async () => {
    const refused = store.setDetector('email', false);
    const next = store.setDetector('iban', true);
    await turn();
    http.expectOne({ method: 'PUT', url: '/api/mcp/settings' }).flush({ error: 'Only an administrator can change this.' }, { status: 403, statusText: 'Forbidden' });
    await expect(refused).rejects.toBeInstanceOf(HttpErrorResponse);
    await turn();

    // Worked out from the state as the refusal left it: email is still on.
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put.request.body).toEqual({ detectors: ['cardNumber', 'email', 'iban'] });
    put.flush({ ...OVERVIEW, detectors: OVERVIEW.detectors.map(d => ({ ...d, on: true })) });
    await next;
  });

  it('gives up on a switch the server never answers, reads the section again, and lets the next one go', async () => {
    vi.useFakeTimers();
    const hung = store.setDetector('email', false);
    // Watched from the start: the change is refused long before the case looks at it.
    const given = expect(hung).rejects.toBeInstanceOf(TimeoutError);
    const next = store.setDetector('iban', true);
    await vi.advanceTimersByTimeAsync(0);
    const put1 = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put1.request.body).toEqual({ detectors: ['cardNumber'] });

    // No answer: the switch behind it waits — up to the limit, and no longer.
    await vi.advanceTimersByTimeAsync(SETTINGS_CHANGE_TIMEOUT_MS - 1);
    expect(http.match({ method: 'PUT', url: '/api/mcp/settings' })).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(put1.cancelled).toBe(true);
    // Given up on is not refused: the server may have made the change. The section is read
    // again, and what the server has — the email detector off — is what the next change starts from.
    http.expectOne('/api/mcp').flush({ ...OVERVIEW, detectors: OVERVIEW.detectors.map(d => ({ ...d, on: d.id === 'cardNumber' })) });
    await given;
    expect(mcpError(await hung.catch((e: unknown) => e), 'The detector could not be switched.').error)
      .toBe('The server did not answer in time.');

    await vi.advanceTimersByTimeAsync(0);
    const put2 = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put2.request.body).toEqual({ detectors: ['cardNumber', 'iban'] });
    put2.flush({ ...OVERVIEW, detectors: OVERVIEW.detectors.map(d => ({ ...d, on: d.id !== 'email' })) });
    await next;
    expect(store.overview()?.detectors.filter(d => d.on).map(d => d.id)).toEqual(['cardNumber', 'iban']);
  });

  // ── A key is shown once ─────────────────────────────────────────────────────

  it('holds a new profile\'s key before the lists are read again, and whatever becomes of that read', async () => {
    const p2: McpProfile = { ...P1, id: 'p2', name: 'Support', keyPreview: '11aa22bb…', publishedTools: ['t1'] };
    const creating = store.createProfile({ ...PROFILE_INPUT, publishedTools: ['t1'] });
    // On its way the key is something a reload would lose already.
    expect(store.anyDirty()).toBe(true);
    http.expectOne({ method: 'POST', url: '/api/mcp/profiles' }).flush({ profile: p2, key: 'the-key-shown-once' });

    // Nothing else has been answered yet — and the key is here, with the name of its profile.
    const created = await creating;
    expect(created.id).toBe('p2');
    expect(store.keys()).toEqual([{ key: 'the-key-shown-once', profile: 'Support' }]);
    // The profile is in the list from the answer alone, with the other end of its grant.
    expect(store.profiles().map(p => p.id)).toEqual(['p1', 'p2']);
    expect(store.tools()[0].profiles).toEqual(['p1', 'p2']);

    failLists();
    expect(store.stale()).toBe(true);
    expect(store.profiles().map(p => p.id)).toEqual(['p1', 'p2']);
    expect(store.keys().map(k => k.key)).toEqual(['the-key-shown-once']);

    // "Read again" is the way out of that state.
    store.refreshLists();
    answerLists([P1, p2], [{ ...T1, profiles: ['p1', 'p2'] }]);
    expect(store.stale()).toBe(false);

    // Taken down, the key is gone, and nothing is left that a reload would lose.
    expect(store.anyDirty()).toBe(true);
    store.acknowledgeKey();
    expect(store.keys()).toEqual([]);
    expect(store.anyDirty()).toBe(false);
  });

  it('holds a rotated key though the profiles cannot be read again: the old key is already dead', async () => {
    const rotating = store.rotateKey('p1');
    http.expectOne({ method: 'POST', url: '/api/mcp/profiles/p1/rotate-key' }).flush({ key: 'the-new-key' });
    await rotating;
    expect(store.keys()).toEqual([{ key: 'the-new-key', profile: 'Payments team' }]);

    failLists();
    // The preview in the list is the old key's; the store says the list may be behind.
    expect(store.stale()).toBe(true);
    expect(store.profiles()[0].keyPreview).toBe('7a3f09c1…');
    expect(store.keys().map(k => k.key)).toEqual(['the-new-key']);
  });

  it('a request for a key that is refused leaves nothing on its way and nothing to show', async () => {
    const rotating = store.rotateKey('p1');
    expect(store.keysOnTheirWay()).toBe(1);
    http.expectOne({ method: 'POST', url: '/api/mcp/profiles/p1/rotate-key' })
      .flush({ error: 'Only an administrator can change this.' }, { status: 403, statusText: 'Forbidden' });
    await expect(rotating).rejects.toBeInstanceOf(HttpErrorResponse);
    expect(store.keysOnTheirWay()).toBe(0);
    expect(store.keys()).toEqual([]);
    expect(store.anyDirty()).toBe(false);
  });

  // ── A change is in the state from its own answer ────────────────────────────

  it('a saved profile is in both lists from its answer, and stays saved when the read after it fails', async () => {
    const saved: McpProfile = { ...P1, name: 'Payments developers', publishedTools: [] };
    // A change names the fields it changes; the server keeps the rest.
    const saving = store.updateProfile('p1', { name: 'Payments developers', publishedTools: [] });
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(put.request.body).toEqual({ name: 'Payments developers', publishedTools: [] });
    put.flush(saved);
    // Answered with the profile as the server kept it.
    expect(await saving).toEqual(saved);

    expect(store.profiles()).toEqual([saved]);
    // The other end of the grant: the tool no longer names the profile.
    expect(store.tools()[0].profiles).toEqual([]);
    failLists();
    expect(store.profiles()).toEqual([saved]);
    expect(store.stale()).toBe(true);
  });

  it('a saved tool is in both lists from its answer: a profile gains and loses the grant with it', async () => {
    const withdrawn: McpPublishedTool = { ...T1, profiles: [] };
    const saving = store.saveTool('t1', TOOL_INPUT);
    http.expectOne({ method: 'PUT', url: '/api/mcp/tools/t1' }).flush(withdrawn);
    expect(await saving).toEqual(withdrawn);
    expect(store.profiles()[0].publishedTools).toEqual([]);
    answerLists([{ ...P1, publishedTools: [] }], [withdrawn]);

    const t2: McpPublishedTool = { ...T1, id: 't2', name: 'refunds', profiles: ['p1'] };
    const creating = store.saveTool(null, { ...TOOL_INPUT, name: 'refunds', profiles: ['p1'] });
    http.expectOne({ method: 'POST', url: '/api/mcp/tools' }).flush(t2);
    expect((await creating).id).toBe('t2');
    expect(store.tools().map(t => t.id)).toEqual(['t1', 't2']);
    expect(store.profiles()[0].publishedTools).toEqual(['t2']);
    failLists();
    expect(store.tools().map(t => t.id)).toEqual(['t1', 't2']);
  });

  it('a deleted tool leaves every profile, and a deleted profile every tool, from the answer alone', async () => {
    const removingTool = store.deleteTool('t1');
    http.expectOne({ method: 'DELETE', url: '/api/mcp/tools/t1' }).flush(null, { status: 204, statusText: 'No Content' });
    await removingTool;
    expect(store.tools()).toEqual([]);
    expect(store.profiles()[0].publishedTools).toEqual([]);
    failLists();

    const removingProfile = store.deleteProfile('p1');
    http.expectOne({ method: 'DELETE', url: '/api/mcp/profiles/p1' }).flush(null, { status: 204, statusText: 'No Content' });
    await removingProfile;
    expect(store.profiles()).toEqual([]);
    answerLists([], []);
    expect(store.stale()).toBe(false);
  });

  it('a change the server refuses rejects with the response and leaves the lists as they were', async () => {
    const saving = store.updateProfile('p1', PROFILE_INPUT);
    http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' })
      .flush({ error: 'Name is required.', field: 'name' }, { status: 400, statusText: 'Bad Request' });
    await expect(saving).rejects.toBeInstanceOf(HttpErrorResponse);
    // No read follows a change that did not happen.
    expect(store.profiles()).toEqual([P1]);
    expect(store.stale()).toBe(false);
  });

  it('abandons the read that follows one change when the next change has been answered', async () => {
    const renamed: McpProfile = { ...P1, name: 'Payments developers' };
    const first = store.updateProfile('p1', { ...PROFILE_INPUT, name: 'Payments developers' });
    http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' }).flush(renamed);
    await first;
    const stale = [http.expectOne('/api/mcp/profiles'), http.expectOne('/api/mcp/tools')];

    const second = store.deleteTool('t1');
    http.expectOne({ method: 'DELETE', url: '/api/mcp/tools/t1' }).flush(null, { status: 204, statusText: 'No Content' });
    await second;

    // The older picture — taken before the tool was deleted — can no longer land on the newer one.
    expect(stale.map(r => r.cancelled)).toEqual([true, true]);
    answerLists([{ ...renamed, publishedTools: [] }], []);
    expect(store.tools()).toEqual([]);
  });

  // ── The figures ─────────────────────────────────────────────────────────────

  it('reads the figures again in place of a read still on its way, and keeps what it has when one fails', () => {
    store.refreshFigures();
    const first = [http.expectOne(r => r.url === '/api/mcp/summary'), http.expectOne(r => r.url === '/api/mcp/tools/usage')];
    store.refreshFigures();
    expect(first.map(r => r.cancelled)).toEqual([true, true]);

    http.expectOne(r => r.url === '/api/mcp/summary').flush({ error: 'Too many searches are running.' }, BUSY);
    http.expectOne(r => r.url === '/api/mcp/tools/usage').flush({ days: 7, partial: false, calls: { logs_search: 3000 } });

    // The summary that could not be read is still the last one that could.
    expect(store.summary()).toEqual(SUMMARY);
    expect(store.usage()).toEqual({ logs_search: 3000 });
  });
});
