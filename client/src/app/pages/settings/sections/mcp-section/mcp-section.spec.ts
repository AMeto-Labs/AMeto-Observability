import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { Router, provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { beforeEach, describe, expect, it } from 'vitest';

import { McpOverview, McpProfile, McpPublishedTool, McpRule } from '../../../../core/models/mcp.model';
import { SettingsDirtyService } from '../../settings-dirty.service';
import { McpSectionComponent } from './mcp-section';

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

const PROFILES: McpProfile[] = [{
  id: 'p1', name: 'Payments team', description: 'Claude Code for the payments developers', keyPreview: '7a3f09c1…',
  toolsMode: 'builtInAndPublished', tools: ['logs_overview', 'logs_search'], services: ['payments-api'], maskingMode: 'standard',
  freeFilters: true, maxEvents: 200, maxWindowMinutes: 1440, callsPerMinute: 30, publishedTools: ['t1'],
  createdBy: 'admin', createdAt: '2026-11-10T08:00:00Z', lastCallAt: null,
}];

const RULES: McpRule[] = [{ id: 'r1', field: 'CardNumber', services: [], action: 'hash' }];

const TOOLS: McpPublishedTool[] = [{
  id: 't1', name: 'payment_declines', description: 'Declined payments of one merchant.',
  parameters: [{ name: 'merchantId', type: 'string', required: true, description: '', values: [], default: null }],
  filter: 'MerchantId = {merchantId}', fields: [], defaultWindowMinutes: 60, maxEvents: 200, order: 'newest',
  published: true, profiles: ['p1'], createdBy: 'admin', createdAt: '2026-11-10T08:00:00Z', updatedAt: '2026-11-10T08:00:00Z',
  inputSchema: { properties: { merchantId: {}, from: {}, to: {}, cursor: {} }, required: ['merchantId'] },
}];

const SUMMARY = {
  hours: 24, partial: false, calls: 1284, profiles: 5, events: 48310, medianEvents: 38, masked: 212406,
  maskedBy: { rules: 199662, strict: 0, detectors: 12744 }, statuses: { OK: 1270, Refused: 7 },
  refusals: { limit: 5, masking: 2 }, rules: { r1: 18204 }, detectors: { email: 1902 },
};

/**
 * Settings → AI / MCP as one piece: the section with its store and its tabs, against a mocked
 * API. What is pinned here is what crosses a boundary — what the section asks the server for,
 * what it sends on a change, and what it tells the settings page about unsaved input.
 */
describe('Settings → AI / MCP', () => {
  let http: HttpTestingController;
  let dirty: SettingsDirtyService;
  let router: Router;

  beforeEach(() => {
    localStorage.clear();
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([]),
        SettingsDirtyService,
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider({ ...icons }) },
      ],
    });
    http = TestBed.inject(HttpTestingController);
    dirty = TestBed.inject(SettingsDirtyService);
    router = TestBed.inject(Router);
  });

  /**
   * Waits for the view the way a person would: a change here is a promise (the store awaits the
   * server's answer) followed by effects, and `whenStable` alone returns between the two.
   */
  async function settle(f: ComponentFixture<unknown>): Promise<void> {
    for (let turn = 0; turn < 3; turn++) {
      await f.whenStable();
      await new Promise<void>(resolve => setTimeout(resolve));
    }
    await f.whenStable();
  }

  /** Answers the four reads the section opens with, then the figures it asks for afterwards. */
  async function land(f: ComponentFixture<McpSectionComponent>, overview: McpOverview = OVERVIEW): Promise<void> {
    await settle(f);
    http.expectOne('/api/mcp').flush(overview);
    http.expectOne('/api/mcp/profiles').flush(PROFILES);
    http.expectOne('/api/mcp/rules').flush(RULES);
    http.expectOne('/api/mcp/tools').flush({ builtIn: overview.tools, published: TOOLS });
    await settle(f);
    http.expectOne(r => r.url === '/api/mcp/summary').flush(SUMMARY);
    http.expectOne(r => r.url === '/api/mcp/tools/usage').flush({ days: 7, partial: false, calls: { logs_search: 2961, payment_declines: 1204 } });
    http.expectOne(r => r.url.startsWith('/api/events/services')).flush(['payments-api', 'acquiring-gw']);
    await settle(f);
  }

  async function open(tab: string): Promise<ComponentFixture<McpSectionComponent>> {
    await router.navigate([], { queryParams: { mcp: tab } });
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);
    return f;
  }

  const text = (f: ComponentFixture<unknown>): string => (f.nativeElement as HTMLElement).textContent ?? '';
  const el = <T extends HTMLElement>(f: ComponentFixture<unknown>, selector: string): T => {
    const found = (f.nativeElement as HTMLElement).querySelector<T>(selector);
    if (!found) throw new Error(`nothing matches ${selector}`);
    return found;
  };
  const type = (input: HTMLInputElement, value: string): void => {
    input.value = value;
    input.dispatchEvent(new Event('input'));
  };

  it('opens on Connection, with the endpoint and the last day — and nothing unsaved', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);

    expect(el(f, '.m-tab.active').textContent).toContain('Connection');
    expect(text(f)).toContain(location.origin + '/mcp');
    // The tiles are the summary's figures, set the section's way.
    expect(text(f)).toContain('1 284');
    expect(text(f)).toContain('from 5 profiles');
    expect(text(f)).toContain('median 38 per call');
    expect(text(f)).toContain('94 % by field rules');
    expect(text(f)).toContain('5 by limits, 2 by masking');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('says why when the section cannot be read, and reads again when asked', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await settle(f);
    http.expectOne('/api/mcp').flush({ error: 'The store is busy.' }, { status: 503, statusText: 'Service Unavailable' });
    // The other three reads were abandoned with the first one's failure.
    for (const req of http.match(() => true)) if (!req.cancelled) req.flush([]);
    await settle(f);

    expect(el(f, '[role=alert]').textContent).toContain('The store is busy.');

    el<HTMLButtonElement>(f, '[role=alert] button').click();
    await land(f);
    expect(text(f)).toContain('MCP endpoint');
  });

  it('a limit that was typed and not saved is unsaved input of the settings page', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);

    const events = el<HTMLInputElement>(f, 'input[aria-label="Events per call"]');
    type(events, '300');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // Saving sends the limits and nothing else; the server's answer is what the form then shows.
    const save = [...(f.nativeElement as HTMLElement).querySelectorAll('button')].find(b => b.textContent?.includes('Save limits'))!;
    save.click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put.request.body).toEqual({ responseBudgetTokens: 8000, maxWindowMinutes: 10080, maxEvents: 300, callsPerMinute: 60, maxConcurrent: 2 });
    put.flush({ ...OVERVIEW, limits: { ...OVERVIEW.limits, maxEvents: 300 } });
    await settle(f);

    expect(el<HTMLInputElement>(f, 'input[aria-label="Events per call"]').value).toBe('300');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('lists the profiles with what each key is offered, and an edited card is unsaved until cancelled', async () => {
    const f = await open('profiles');

    expect(el(f, '.m-tab.active').textContent).toContain('Profiles');
    const row = el(f, '[role=row].pick');
    expect(row.textContent).toContain('Payments team');
    expect(row.textContent).toContain('7a3f09c1…');
    expect(row.textContent).toContain('2 built-in + 1 published');
    expect(row.textContent).toContain('never');
    expect(dirty.dirtyTabsList()).toEqual([]);

    const name = el<HTMLInputElement>(f, 'input[placeholder="Payments team"]');
    expect(name.value).toBe('Payments team');
    type(name, 'Payments developers');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    [...(f.nativeElement as HTMLElement).querySelectorAll('button')].find(b => b.textContent?.trim() === 'Cancel')!.click();
    await settle(f);
    expect(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]').value).toBe('Payments team');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('saving a profile sends its settings and its granted tools as one list', async () => {
    const f = await open('profiles');
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    // The published tool's switch: off withdraws it from this profile.
    el<HTMLButtonElement>(f, 'button.m-sw[aria-label="payment_declines"]').click();
    await settle(f);

    [...(f.nativeElement as HTMLElement).querySelectorAll('button')].find(b => b.textContent?.trim() === 'Save')!.click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(put.request.body).toEqual({
      name: 'Payments developers', description: 'Claude Code for the payments developers', toolsMode: 'builtInAndPublished',
      tools: ['logs_overview', 'logs_search'], services: ['payments-api'], maskingMode: 'standard', freeFilters: true,
      maxEvents: 200, maxWindowMinutes: 1440, callsPerMinute: 30, publishedTools: [],
    });
    const saved = { ...PROFILES[0], name: 'Payments developers', publishedTools: [] };
    put.flush(saved);
    await settle(f);
    // Profiles and tools name each other, so both are read again after a change to either.
    http.expectOne('/api/mcp/profiles').flush([saved]);
    http.expectOne('/api/mcp/tools').flush({ builtIn: OVERVIEW.tools, published: [{ ...TOOLS[0], profiles: [] }] });
    await settle(f);

    expect(el(f, '[role=row].pick').textContent).toContain('Payments developers');
    expect(el(f, '[role=row].pick').textContent).toContain('2 built-in');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('switching a built-in tool off sends the names switched off for the whole server', async () => {
    const f = await open('tools');
    http.expectOne('/api/mcp/profiles/p1/tools').flush({ tools: [] });
    await settle(f);

    const toggle = el<HTMLButtonElement>(f, 'button.m-sw[aria-label="logs_search for every profile"]');
    expect(toggle.getAttribute('aria-checked')).toBe('true');
    toggle.click();
    await settle(f);

    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put.request.body).toEqual({ disabledTools: ['logs_search'] });
    put.flush({ ...OVERVIEW, tools: OVERVIEW.tools.map(t => (t.name === 'logs_search' ? { ...t, enabled: false } : t)) });
    await settle(f);
    // What a profile is offered has changed with it, so the panel asks again.
    http.expectOne('/api/mcp/profiles/p1/tools').flush({ tools: [] });
    await settle(f);

    expect(el(f, 'button.m-sw[aria-label="logs_search for every profile"]').getAttribute('aria-checked')).toBe('false');
    // The published tool is listed with its own arguments, without the window every tool takes.
    expect(text(f)).toContain('payment_declines');
    expect(text(f)).toContain('1 204');
  });

  it('reads the record of calls for the last day and takes the picked call apart', async () => {
    const f = await open('activity');
    const get = http.expectOne(r => r.url === '/api/mcp/activity');
    expect(get.request.params.get('count')).toBe('50');
    expect(get.request.params.has('profile')).toBe(false);
    // The window is the last 24 hours, give or take the time the test took.
    const from = new Date(get.request.params.get('from')!).getTime();
    expect(Math.abs(Date.now() - 24 * 3_600_000 - from)).toBeLessThan(60_000);
    get.flush({
      more: false,
      calls: [{
        id: '42', at: '2026-11-16T09:14:31.000Z', afterTs: '639000000000000000', tool: 'logs_search', profile: 'Payments team',
        profileId: 'p1', status: 'Refused', events: 0, masked: 0, tokens: 60, elapsedMs: 1,
        arguments: '{"filter":"CardNumber like \'4%\'"}', refusal: 'CardNumber is a hidden field.', refusalKind: 'masking',
        filter: "CardNumber like '4%'", client: 'claude-code 2.1 · MCP 2026-07-28', maskedBy: null,
      }],
    });
    for (const req of http.match(() => true)) req.flush(req.request.url.includes('summary') ? SUMMARY : { days: 7, partial: false, calls: {} });
    await settle(f);

    const row = el(f, '[role=row].pick');
    expect(row.textContent).toContain('logs_search');
    expect(row.textContent).toContain('Refused');
    // A refused call returned nothing, which is not the same as returning zero events.
    expect(row.textContent).toContain('—');
    expect(text(f)).toContain('Refused by the masking rules');
    expect(text(f)).toContain('CardNumber is a hidden field.');
    expect(text(f)).toContain('claude-code 2.1 · MCP 2026-07-28');
    expect(text(f)).toContain('Showing 1 of 1 284 calls');
  });
});
