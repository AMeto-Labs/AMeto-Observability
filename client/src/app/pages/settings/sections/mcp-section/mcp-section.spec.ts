import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { HttpRequest, provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { Router, provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { McpCall, McpOverview, McpPreview, McpProfile, McpPublishedTool, McpRule } from '../../../../core/models/mcp.model';
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

/** A second profile, for the cases that walk a list of them. */
const SUPPORT: McpProfile = {
  ...PROFILES[0], id: 'p2', name: 'Support', description: '', keyPreview: '11aa22bb…', services: [], publishedTools: [],
};

const RULES: McpRule[] = [{ id: 'r1', field: 'CardNumber', services: [], action: 'hash' }];

const TOOLS: McpPublishedTool[] = [{
  id: 't1', name: 'payment_declines', description: 'Declined payments of one merchant.',
  parameters: [{ name: 'merchantId', type: 'string', required: true, description: '', values: [], default: null }],
  filter: 'MerchantId = {merchantId}', fields: [], defaultWindowMinutes: 60, maxEvents: 200, order: 'newest',
  published: true, profiles: ['p1'], createdBy: 'admin', createdAt: '2026-11-10T08:00:00Z', updatedAt: '2026-11-10T08:00:00Z',
  inputSchema: { properties: { merchantId: {}, from: {}, to: {}, cursor: {} }, required: ['merchantId'] },
}];

/** A second tool, granted to nobody. */
const REFUNDS: McpPublishedTool = {
  ...TOOLS[0], id: 't2', name: 'refunds', description: 'Refunds of one merchant.', profiles: [],
};

const SUMMARY = {
  hours: 24, partial: false, calls: 1284, profiles: 5, events: 48310, medianEvents: 38, masked: 212406,
  maskedBy: { rules: 199662, strict: 0, detectors: 12744 }, statuses: { OK: 1270, Refused: 7 },
  refusals: { limit: 5, masking: 2 }, rules: { r1: 18204 }, detectors: { email: 1902 },
};

const USAGE = { days: 7, partial: false, calls: { logs_search: 2961, payment_declines: 1204 } };

/** One event as Standard mode answers it: the card number is a token, in the property and in the exception. */
const PREVIEW: McpPreview = {
  found: true,
  stored: {
    '@t': '2026-11-16T09:14:31.0000000+00:00', '@mt': 'Payment {PaymentId} declined', '@l': 'Error', '@service': 'payments-api',
    '@x': { type: 'System.InvalidOperationException', message: 'card 4111111111111111 refused', stack: '   at Pay() in Pay.cs:line 12' },
    props: { PaymentId: 'pay_1', CardNumber: '4111111111111111' },
  },
  sent: {
    template: 'Payment {PaymentId} declined',
    properties: [{ name: 'PaymentId', value: 'pay_1' }, { name: 'CardNumber', value: 'h:91c0e2aa5d17' }],
    exception: { type: 'System.InvalidOperationException', message: 'card h:91c0e2aa5d17 refused', stack: '   at Pay() in Pay.cs:line 12' },
  },
  masked: { total: 2, rules: { CardNumber: 1 }, strict: {}, detectors: { cardNumber: 1 } },
};

/** The same event under Strict: the payment's id is a token as well. */
const PREVIEW_STRICT: McpPreview = {
  ...PREVIEW,
  sent: {
    ...PREVIEW.sent!,
    properties: [{ name: 'PaymentId', value: 'h:0123456789ab' }, { name: 'CardNumber', value: 'h:91c0e2aa5d17' }],
  },
  masked: { total: 3, rules: { CardNumber: 1 }, strict: { PaymentId: 1 }, detectors: { cardNumber: 1 } },
};

const call = (id: string, status: string, more: Partial<McpCall> = {}): McpCall => ({
  id, at: '2026-11-16T09:14:31.000Z', afterTs: '639000000000000000', tool: 'logs_search', profile: 'Payments team',
  profileId: 'p1', status, events: 3, masked: 1, tokens: 60, elapsedMs: 12, arguments: '{}', refusal: null, refusalKind: null,
  filter: null, client: 'claude-code 2.1', maskedBy: null, ...more,
});

const BUSY = { status: 503, statusText: 'Service Unavailable' };
const NO_CONTENT = { status: 204, statusText: 'No Content' };

const isActivity = (r: HttpRequest<unknown>): boolean => r.url === '/api/mcp/activity';
const isSummary = (r: HttpRequest<unknown>): boolean => r.url === '/api/mcp/summary';
const isUsage = (r: HttpRequest<unknown>): boolean => r.url === '/api/mcp/tools/usage';
const isOffered = (r: HttpRequest<unknown>): boolean => /^\/api\/mcp\/profiles\/[^/]+\/tools$/.test(r.url);

/** What the server holds when a case begins. */
interface World {
  overview?: McpOverview;
  profiles?: McpProfile[];
  tools?: McpPublishedTool[];
}

/**
 * Settings → AI / MCP as one piece: the section with its store and its tabs, against a mocked
 * API. What is pinned here is what crosses a boundary — what the section asks the server for,
 * what it sends on a change, and what it tells the settings page about unsaved input.
 */
describe('Settings → AI / MCP', () => {
  let http: HttpTestingController;
  let dirty: SettingsDirtyService;
  let router: Router;

  // Nothing pinned here depends on what localStorage holds, so nothing here clears it: under
  // Node 26 every spec file shares one storage file (--localstorage-file), and a clear here
  // would empty it under the specs that do keep something there — the session's tokens, the
  // Logs page's switches.
  beforeEach(() => {
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

  afterEach(() => {
    vi.unstubAllGlobals();
    // Every request a case caused is one it has accounted for: a read nobody expected — the
    // figures again on every filter, a second change behind a first — fails the case. And only
    // that case: the test bed is taken down here whatever the answer, so the next one starts clean.
    try {
      http.verify();
    } finally {
      TestBed.resetTestingModule();
    }
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
  async function land(f: ComponentFixture<McpSectionComponent>, world: World = {}): Promise<void> {
    const overview = world.overview ?? OVERVIEW;
    await settle(f);
    http.expectOne('/api/mcp').flush(overview);
    http.expectOne('/api/mcp/profiles').flush(world.profiles ?? PROFILES);
    http.expectOne('/api/mcp/rules').flush(RULES);
    http.expectOne('/api/mcp/tools').flush({ builtIn: overview.tools, published: world.tools ?? TOOLS });
    await settle(f);
    http.expectOne(isSummary).flush(SUMMARY);
    http.expectOne(isUsage).flush(USAGE);
    http.expectOne(r => r.url.startsWith('/api/events/services')).flush(['payments-api', 'acquiring-gw']);
    await settle(f);
  }

  async function open(tab: string, world: World = {}, tool?: string): Promise<ComponentFixture<McpSectionComponent>> {
    await router.navigate([], { queryParams: tool ? { mcp: tab, tool } : { mcp: tab } });
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f, world);
    return f;
  }

  const text = (f: ComponentFixture<unknown>): string => (f.nativeElement as HTMLElement).textContent ?? '';
  const all = <T extends HTMLElement>(f: ComponentFixture<unknown>, selector: string): T[] =>
    [...(f.nativeElement as HTMLElement).querySelectorAll<T>(selector)];
  const el = <T extends HTMLElement>(f: ComponentFixture<unknown>, selector: string): T => {
    const found = (f.nativeElement as HTMLElement).querySelector<T>(selector);
    if (!found) throw new Error(`nothing matches ${selector}`);
    return found;
  };
  /** The button that says `label`, inside `within` when two parts of the page have one. */
  const button = (f: ComponentFixture<unknown>, label: string, within = ''): HTMLButtonElement => {
    const found = all<HTMLButtonElement>(f, `${within} button`).find(b => b.textContent?.trim() === label);
    if (!found) throw new Error(`no button says "${label}"${within ? ' in ' + within : ''}`);
    return found;
  };
  const type = (input: HTMLInputElement | HTMLTextAreaElement, value: string): void => {
    input.value = value;
    input.dispatchEvent(new Event('input'));
  };
  const choose = (select: HTMLSelectElement, value: string): void => {
    select.value = value;
    select.dispatchEvent(new Event('change'));
  };
  const press = (target: HTMLElement, key: string): void => {
    target.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true }));
  };

  /** Goes to another tab of the section the way a person does. */
  async function show(f: ComponentFixture<unknown>, label: string): Promise<void> {
    const tab = all<HTMLButtonElement>(f, '.m-tab').find(b => b.textContent?.trim() === label);
    if (!tab) throw new Error(`no tab says "${label}"`);
    tab.click();
    await settle(f);
  }

  /** The read of both lists that follows a change to either, answered. */
  async function lists(f: ComponentFixture<unknown>, profiles: McpProfile[], tools: McpPublishedTool[]): Promise<void> {
    await settle(f);
    http.expectOne('/api/mcp/profiles').flush(profiles);
    http.expectOne('/api/mcp/tools').flush({ builtIn: OVERVIEW.tools, published: tools });
    await settle(f);
  }

  /** The same read, failing: the profiles are refused, which abandons the tools. */
  async function failLists(f: ComponentFixture<unknown>): Promise<void> {
    await settle(f);
    http.expectOne('/api/mcp/profiles').flush({ error: 'The store is busy.' }, BUSY);
    expect(http.expectOne('/api/mcp/tools').cancelled).toBe(true);
    await settle(f);
  }

  /**
   * The Tools tab asks which tools a profile is offered whenever anything that list depends on
   * moves. The cases that pass through that tab on their way are not about its panel: they
   * answer whatever it has asked and go on.
   */
  async function offered(f: ComponentFixture<unknown>): Promise<void> {
    await settle(f);
    for (const req of http.match(isOffered)) if (!req.cancelled) req.flush({ tools: [] });
    await settle(f);
  }

  // ── The section ─────────────────────────────────────────────────────────────

  it('opens on Connection, with the endpoint and the last day — and nothing unsaved', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);

    expect(el(f, '.m-tab.active').textContent).toContain('Connection');
    expect(text(f)).toContain(location.origin + '/mcp');
    // The tiles are the summary's figures, set the section's way: thousands apart by a no-break space.
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
    http.expectOne('/api/mcp').flush({ error: 'The store is busy.' }, BUSY);
    // The other three reads were abandoned with the first one's failure.
    expect(http.match(() => true).map(r => r.cancelled)).toEqual([true, true, true]);
    await settle(f);

    expect(el(f, '[role=alert]').textContent).toContain('The store is busy.');

    el<HTMLButtonElement>(f, '[role=alert] button').click();
    await land(f);
    expect(text(f)).toContain('MCP endpoint');
  });

  // ── Connection ──────────────────────────────────────────────────────────────

  it('a limit that was typed and not saved is unsaved input of the settings page', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);

    const events = el<HTMLInputElement>(f, 'input[aria-label="Events per call"]');
    type(events, '300');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // Saving sends the limits and nothing else; the server's answer is what the form then shows.
    button(f, 'Save limits').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put.request.body).toEqual({ responseBudgetTokens: 8000, maxWindowMinutes: 10080, maxEvents: 300, callsPerMinute: 60, maxConcurrent: 2 });
    put.flush({ ...OVERVIEW, limits: { ...OVERVIEW.limits, maxEvents: 300 } });
    await settle(f);

    expect(el<HTMLInputElement>(f, 'input[aria-label="Events per call"]').value).toBe('300');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a number field cleared to be retyped stays empty, and an empty one is refused on save', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);

    const events = el<HTMLInputElement>(f, 'input[aria-label="Events per call"]');
    type(events, '');
    await settle(f);
    // Empty — not a zero written into the field under the cursor.
    expect(events.value).toBe('');
    expect(events.getAttribute('aria-invalid')).toBe('true');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // Nothing is sent (the case would fail on an unexpected request); the reason is tied to the field.
    button(f, 'Save limits').click();
    await settle(f);
    expect(el(f, '#mcp-limits-error').textContent).toContain('Every limit is a whole number above zero.');
    expect(events.getAttribute('aria-describedby')).toBe('mcp-limits-error');
    // The fields that are fine are not marked.
    expect(el(f, 'input[aria-label="AI searches at once"]').hasAttribute('aria-invalid')).toBe(false);

    type(events, '250');
    await settle(f);
    expect(events.value).toBe('250');
    expect(events.hasAttribute('aria-invalid')).toBe(false);
  });

  it('a window that is no whole number of hours is shown as one the form can save', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f, { overview: { ...OVERVIEW, limits: { ...OVERVIEW.limits, maxWindowMinutes: 90 } } });

    // Ninety minutes, as saved: the field is not marked, and nothing is unsaved because of it.
    const window = el<HTMLInputElement>(f, 'input[aria-label="Widest time window"]');
    expect(window.value).toBe('90 min');
    expect(window.classList.contains('invalid')).toBe(false);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  // ── Profiles ────────────────────────────────────────────────────────────────

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

    button(f, 'Cancel').click();
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

    button(f, 'Save').click();
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

    // The list shows the change from its own answer, before anything is read again.
    expect(el(f, '[role=row].pick').textContent).toContain('Payments developers');
    expect(el(f, '[role=row].pick').textContent).toContain('2 built-in');
    expect(el(f, '[role=row].pick').textContent).not.toContain('published');
    expect(dirty.dirtyTabsList()).toEqual([]);
    // Profiles and tools name each other, so both are read again after a change to either.
    await lists(f, [saved], [{ ...TOOLS[0], profiles: [] }]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  /** Withdraws `payment_declines` from the profile in the tool's own editor, and saves the tool. */
  async function withdrawInEditor(f: ComponentFixture<unknown>, toolsAfter: McpPublishedTool[]): Promise<void> {
    await show(f, 'Tools');
    await offered(f);
    el<HTMLButtonElement>(f, 'button[aria-label="Edit payment_declines"]').click();
    await settle(f);
    el<HTMLButtonElement>(f, 'button[aria-label="Withdraw from Payments team"]').click();
    await settle(f);
    button(f, 'Save', 'app-mcp-tool-editor').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/tools/t1' });
    expect(put.request.body.profiles).toEqual([]);
    put.flush(toolsAfter[0]);
    await lists(f, [{ ...PROFILES[0], publishedTools: [] }], toolsAfter);
    await offered(f);
  }

  it('a card left open does not grant back the tool that the tool\'s editor withdrew', async () => {
    const f = await open('profiles');
    const grant = (): HTMLElement => el(f, 'app-mcp-profiles-tab button.m-sw[aria-label="payment_declines"]');
    expect(grant().getAttribute('aria-checked')).toBe('true');

    // The Profiles tab stays mounted, with its card, while the tool is edited elsewhere.
    const withdrawn = { ...TOOLS[0], profiles: [] };
    await withdrawInEditor(f, [withdrawn]);

    // Back on the card: it has followed the profile, and nothing in it is unsaved.
    await show(f, 'Profiles');
    expect(grant().getAttribute('aria-checked')).toBe('false');
    expect(dirty.dirtyTabsList()).toEqual([]);

    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const save = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(save.request.body.name).toBe('Payments developers');
    // The save is about the name. The grants were not touched here, so they are not sent at
    // all — the server keeps them as the editor left them.
    expect(save.request.body).not.toHaveProperty('publishedTools');
    const saved = { ...PROFILES[0], name: 'Payments developers', publishedTools: [] };
    save.flush(saved);
    await lists(f, [saved], [withdrawn]);
    await offered(f);
  });

  it('a card with unsaved switches keeps them, laid over the grants as they are now', async () => {
    const f = await open('profiles', { tools: [TOOLS[0], REFUNDS] });
    const grant = (name: string): HTMLButtonElement => el(f, `app-mcp-profiles-tab button.m-sw[aria-label="${name}"]`);
    // The user grants a second tool, and does not save yet …
    grant('refunds').click();
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // … and withdraws the first one in that tool's own editor.
    const withdrawn = { ...TOOLS[0], profiles: [] };
    await withdrawInEditor(f, [withdrawn, REFUNDS]);

    await show(f, 'Profiles');
    expect(grant('payment_declines').getAttribute('aria-checked')).toBe('false');
    expect(grant('refunds').getAttribute('aria-checked')).toBe('true');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // What is sent is the grants as they are now with the user's own switch on top: not the
    // list the card was opened with, which would grant the withdrawn tool back.
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const save = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(save.request.body.publishedTools).toEqual(['t2']);
    const saved = { ...PROFILES[0], publishedTools: ['t2'] };
    save.flush(saved);
    await lists(f, [saved], [withdrawn, { ...REFUNDS, profiles: ['p1'] }]);
    await offered(f);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a tool deleted meanwhile leaves the card, and is not sent with it', async () => {
    const f = await open('profiles');
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);

    await show(f, 'Tools');
    await offered(f);
    el<HTMLButtonElement>(f, 'button[aria-label="Delete payment_declines"]').click();
    await settle(f);
    button(f, 'Delete tool', '.modal').click();
    await settle(f);
    http.expectOne({ method: 'DELETE', url: '/api/mcp/tools/t1' }).flush(null, NO_CONTENT);
    await lists(f, [{ ...PROFILES[0], publishedTools: [] }], []);
    await offered(f);

    await show(f, 'Profiles');
    expect(all(f, 'app-mcp-profiles-tab button.m-sw[aria-label="payment_declines"]')).toEqual([]);
    // What was typed is still there, and still the only thing unsaved.
    expect(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]').value).toBe('Payments developers');
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const save = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    // The id of a tool that is gone would be refused by the server: "There is no published tool with the id".
    expect(save.request.body).not.toHaveProperty('publishedTools');
    const saved = { ...PROFILES[0], name: 'Payments developers', publishedTools: [] };
    save.flush(saved);
    await lists(f, [saved], []);
    await offered(f);
  });

  it('a profile the server will not delete leaves what was typed in its card unsaved', async () => {
    const f = await open('profiles');
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    button(f, 'Delete profile', 'app-mcp-profiles-tab .card').click();
    await settle(f);
    button(f, 'Delete profile', '.modal').click();
    await settle(f);
    http.expectOne({ method: 'DELETE', url: '/api/mcp/profiles/p1' })
      .flush({ error: 'Only an administrator can change this.' }, { status: 403, statusText: 'Forbidden' });
    await settle(f);

    expect(el(f, '#mcp-profile-error').textContent).toContain('Only an administrator can change this.');
    // The profile is still there and so is the edit: leaving the page must still ask.
    expect(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]').value).toBe('Payments developers');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
    expect(button(f, 'Save', 'app-mcp-profiles-tab').disabled).toBe(false);
  });

  it('shows a rotated key though the list cannot be read again, and says the list may be behind', async () => {
    const f = await open('profiles');
    button(f, 'Rotate', 'app-mcp-profiles-tab .card').click();
    await settle(f);
    button(f, 'Rotate key', '.modal').click();
    await settle(f);
    http.expectOne({ method: 'POST', url: '/api/mcp/profiles/p1/rotate-key' }).flush({ key: 'the-rotated-key-shown-once' });
    // The old key stopped working with that answer. The read that follows fails.
    await failLists(f);

    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key-shown-once');
    expect(text(f)).not.toContain('could not be rotated');
    expect(el(f, '.m-stale').textContent).toContain('could not be read again');

    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(all(f, '.m-key-text')).toEqual([]);

    // "Read again" brings the new key's preview, and the note goes.
    button(f, 'Read again', '.m-stale').click();
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }], TOOLS);
    expect(all(f, '.m-stale')).toEqual([]);
    expect(el(f, '[role=row].pick').textContent).toContain('c0ffee00…');
  });

  it('shows a new profile\'s key though the list cannot be read again, and does not offer to create it twice', async () => {
    const f = await open('profiles');
    button(f, 'New profile').click();
    await settle(f);
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Support');
    await settle(f);
    button(f, 'Create profile').click();
    await settle(f);
    const post = http.expectOne({ method: 'POST', url: '/api/mcp/profiles' });
    // No tool was granted in the card, so no grants are sent.
    expect(post.request.body).not.toHaveProperty('publishedTools');
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await failLists(f);

    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    button(f, 'Done', '.modal').click();
    await settle(f);

    // The card is the profile that now exists: saving it again is a change, not a second profile.
    expect(el(f, 'app-mcp-profiles-tab .card .section-title').textContent).toBe('Support');
    expect(all<HTMLButtonElement>(f, 'button').map(b => b.textContent?.trim())).not.toContain('Create profile');
    expect(all(f, '[role=row].pick').map(r => r.classList.contains('sel'))).toEqual([false, true]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('the list of profiles is one stop for the Tab key, walked with the arrows', async () => {
    const f = await open('profiles', { profiles: [PROFILES[0], SUPPORT] });
    const rows = (): HTMLElement[] => all(f, '[role=row].pick');
    expect(rows().map(r => r.getAttribute('tabindex'))).toEqual(['0', '-1']);

    press(rows()[0], 'ArrowDown');
    await settle(f);
    expect(rows().map(r => r.getAttribute('tabindex'))).toEqual(['-1', '0']);
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['false', 'true']);
    expect(document.activeElement).toBe(rows()[1]);
    expect(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]').value).toBe('Support');

    // Past the end there is nowhere to go; Space on the other row picks it.
    press(rows()[1], 'ArrowDown');
    await settle(f);
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['false', 'true']);
    press(rows()[0], ' ');
    await settle(f);
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['true', 'false']);

    // A card with unsaved input is left only after the question; "no" keeps the row and the input.
    const confirm = vi.fn(() => false);
    vi.stubGlobal('confirm', confirm);
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);
    press(rows()[0], 'ArrowDown');
    await settle(f);
    expect(confirm).toHaveBeenCalledWith('This profile has unsaved changes. Discard them?');
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['true', 'false']);
    expect(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]').value).toBe('Payments developers');
  });

  // ── Masking ─────────────────────────────────────────────────────────────────

  /** Opens the Masking tab and answers the preview it opens with. */
  async function openMasking(preview: McpPreview = PREVIEW): Promise<ComponentFixture<McpSectionComponent>> {
    const f = await open('masking');
    const first = http.expectOne('/api/mcp/preview');
    expect(first.request.body).toEqual({ mode: 'standard' });
    first.flush(preview);
    await settle(f);
    return f;
  }

  /** The preview's two boxes, each as `key → [text, kind of mark]` runs. */
  const previewLine = (f: ComponentFixture<unknown>, box: 0 | 1, key: string): HTMLElement => {
    const line = [...all(f, '.kv')[box].querySelectorAll<HTMLElement>('.kv-line')]
      .find(l => l.querySelector('.kv-key')?.textContent === key);
    if (!line) throw new Error(`the preview has no line ${key}`);
    return line.querySelector<HTMLElement>('.kv-value')!;
  };

  it('shows an event as stored and as sent, the hidden part marked on both sides — the stack included', async () => {
    const f = await openMasking();

    expect(previewLine(f, 0, 'CardNumber').querySelector('.m-raw')?.textContent).toBe('4111111111111111');
    expect(previewLine(f, 1, 'CardNumber').querySelector('.m-tok')?.textContent).toBe('h:91c0e2aa5d17');
    expect(previewLine(f, 0, 'PaymentId').querySelector('.m-raw')).toBeNull();
    expect(previewLine(f, 0, '@x.message').textContent).toBe('card 4111111111111111 refused');
    expect(previewLine(f, 1, '@x.message').textContent).toBe('card h:91c0e2aa5d17 refused');
    // An exception's stack leaves with the answer too, so it is in the picture of the answer.
    expect(previewLine(f, 0, '@x.stack').textContent).toBe('   at Pay() in Pay.cs:line 12');
    expect(previewLine(f, 1, '@x.stack').textContent).toBe('   at Pay() in Pay.cs:line 12');
    expect(text(f)).toContain('2 values replaced: 1 by field rules, 1 by detectors.');
  });

  it('does not mark a 64-bit number as hidden because the browser cannot hold its digits', async () => {
    // The stored event went through JSON.parse; the cell of the answer is the server's own text.
    const exact = '9007199254740993';
    const f = await openMasking({
      ...PREVIEW,
      stored: { ...PREVIEW.stored!, props: { OrderId: JSON.parse(exact), Amount: 12500 } },
      sent: { ...PREVIEW.sent!, properties: [{ name: 'OrderId', value: exact }, { name: 'Amount', value: 'h:0123456789ab' }] },
    });

    for (const box of [0, 1] as const) {
      expect(previewLine(f, box, 'OrderId').textContent).toBe(exact);
      expect(previewLine(f, box, 'OrderId').querySelector('.m-raw, .m-tok')).toBeNull();
    }
    // A number that was hidden is still marked.
    expect(previewLine(f, 0, 'Amount').querySelector('.m-raw')?.textContent).toBe('12500');
    expect(previewLine(f, 1, 'Amount').querySelector('.m-tok')?.textContent).toBe('h:0123456789ab');
  });

  it('a slow preview does not land on top of a newer one', async () => {
    const f = await openMasking();
    const mode = (label: string): HTMLButtonElement => button(f, label, '[aria-label="Masking mode"]');

    // Strict, then back to Standard before Strict has answered.
    mode('Strict').click();
    await settle(f);
    mode('Standard').click();
    await settle(f);
    const [strict, standard] = http.match('/api/mcp/preview');
    expect(strict.request.body).toEqual({ mode: 'strict' });
    expect(standard.request.body).toEqual({ mode: 'standard' });

    // Standard answers first; Strict, the slow one, would answer last.
    standard.flush(PREVIEW);
    expect(strict.cancelled).toBe(true);
    if (!strict.cancelled) strict.flush(PREVIEW_STRICT);
    await settle(f);

    // What is shown is the answer to what is chosen.
    expect(mode('Standard').getAttribute('aria-pressed')).toBe('true');
    expect(previewLine(f, 1, 'PaymentId').textContent).toBe('pay_1');
    expect(text(f)).toContain('2 values replaced');
  });

  it('Enter while a preview is on its way asks again in its place, not beside it', async () => {
    const f = await openMasking();
    const probe = el<HTMLInputElement>(f, 'input[aria-label="Filter that picks the event"]');
    type(probe, "@l = 'Error'");
    press(probe, 'Enter');
    await settle(f);
    type(probe, "@l = 'Error' and @service = 'payments-api'");
    press(probe, 'Enter');
    await settle(f);

    const [first, second] = http.match('/api/mcp/preview');
    expect(first.cancelled).toBe(true);
    expect(second.request.body).toEqual({ filter: "@l = 'Error' and @service = 'payments-api'", mode: 'standard' });
    // A filter the server cannot read is said at the field it was typed into.
    second.flush({ error: 'Invalid filter: Unexpected end.', field: 'filter' }, { status: 400, statusText: 'Bad Request' });
    await settle(f);
    expect(el(f, '#mcp-preview-error').textContent).toContain('Invalid filter');
    expect(probe.getAttribute('aria-invalid')).toBe('true');
    expect(probe.getAttribute('aria-describedby')).toBe('mcp-preview-error');
  });

  it('every detector\'s switch waits while one is on its way', async () => {
    const f = await openMasking();
    const detector = (name: string): HTMLButtonElement => el(f, `button.m-sw[aria-label="${name}"]`);

    detector('Email address').click();
    await settle(f);
    // The change is the whole list of detectors: a second list, sent now, would be worked out
    // without the first and switch the email detector back on.
    expect(all<HTMLButtonElement>(f, '.detectors button.m-sw').map(b => b.disabled)).toEqual([true, true, true]);
    detector('IBAN').click();
    await settle(f);

    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put.request.body).toEqual({ detectors: ['cardNumber'] });
    put.flush({ ...OVERVIEW, detectors: OVERVIEW.detectors.map(d => ({ ...d, on: d.id === 'cardNumber' })) });
    await settle(f);
    // What an answer hides has changed, so the preview is taken again.
    http.expectOne('/api/mcp/preview').flush(PREVIEW);
    await settle(f);

    expect(all<HTMLButtonElement>(f, '.detectors button.m-sw').map(b => b.disabled)).toEqual([false, false, false]);
    expect(detector('Email address').getAttribute('aria-checked')).toBe('false');
  });

  it('asks once before a rule is deleted, in the same dialog', async () => {
    const f = await openMasking();
    el<HTMLButtonElement>(f, 'button[aria-label="Edit the rule for CardNumber"]').click();
    await settle(f);

    // The first click only asks — nothing is sent (the case would fail on an unexpected request).
    button(f, 'Delete rule', '.modal').click();
    await settle(f);
    expect(el(f, '#mcp-rule-delete-note').textContent)
      .toBe('Delete the rule? Without it Standard mode sends “CardNumber” as it is stored, unless another rule or a detector hides it.');
    // The focus is on the answer that changes nothing.
    expect(document.activeElement?.textContent?.trim()).toBe('Keep rule');

    button(f, 'Keep rule', '.modal').click();
    await settle(f);
    expect(all(f, '#mcp-rule-delete-note')).toEqual([]);
    expect(document.activeElement?.textContent?.trim()).toBe('Delete rule');

    button(f, 'Delete rule', '.modal').click();
    await settle(f);
    button(f, 'Delete rule', '.modal').click();
    await settle(f);
    http.expectOne({ method: 'DELETE', url: '/api/mcp/rules/r1' }).flush(null, NO_CONTENT);
    await settle(f);
    http.expectOne('/api/mcp/preview').flush(PREVIEW);
    await settle(f);

    expect(all(f, '.modal')).toEqual([]);
    expect(text(f)).toContain('No field rules.');
  });

  it('a rule without a field is refused at the field', async () => {
    const f = await openMasking();
    button(f, 'Add rule').click();
    await settle(f);
    button(f, 'Save', '.modal').click();
    await settle(f);

    const field = el<HTMLInputElement>(f, '.modal input[placeholder="CardNumber"]');
    expect(el(f, '#mcp-rule-error').textContent).toBe('Field is required.');
    expect(field.getAttribute('aria-invalid')).toBe('true');
    expect(field.getAttribute('aria-describedby')).toBe('mcp-rule-error');
  });

  // ── Tools ───────────────────────────────────────────────────────────────────

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

  it('every built-in tool\'s switch waits while one is on its way', async () => {
    const f = await open('tools');
    await offered(f);
    const tool = (name: string): HTMLButtonElement => el(f, `button.m-sw[aria-label="${name} for every profile"]`);

    tool('logs_search').click();
    await settle(f);
    // The change is the whole list of names switched off: a second list, sent now, would be
    // worked out without the first and switch logs_search back on.
    expect([tool('logs_overview').disabled, tool('logs_search').disabled]).toEqual([true, true]);
    tool('logs_overview').click();
    await settle(f);

    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/settings' });
    expect(put.request.body).toEqual({ disabledTools: ['logs_search'] });
    put.flush({ ...OVERVIEW, tools: OVERVIEW.tools.map(t => (t.name === 'logs_search' ? { ...t, enabled: false } : t)) });
    await offered(f);

    expect([tool('logs_overview').disabled, tool('logs_search').disabled]).toEqual([false, false]);
    // Every child of a row is a cell: the switch sits in one.
    expect(tool('logs_search').parentElement?.getAttribute('role')).toBe('cell');
  });

  // ── The tool editor ─────────────────────────────────────────────────────────

  /** The tool with an optional parameter that has a default: what the editor's rows are about. */
  const WITH_DEFAULT: McpPublishedTool = {
    ...TOOLS[0],
    parameters: [
      { name: 'merchantId', type: 'string', required: true, description: '', values: [], default: null },
      { name: 'reason', type: 'string', required: false, description: '', values: [], default: 'fraud' },
    ],
    filter: 'MerchantId = {merchantId} and Reason = {reason}',
  };

  async function openEditor(tool: McpPublishedTool = TOOLS[0]): Promise<ComponentFixture<McpSectionComponent>> {
    const f = await open('tools', { tools: [tool] }, tool.id);
    await offered(f);
    return f;
  }

  it('asks before Back or Forward takes an edited tool out of the editor, and "no" puts the address back', async () => {
    const f = await openEditor();
    const name = (): HTMLInputElement => el(f, 'app-mcp-tool-editor input[placeholder="payment_declines"]');
    type(name(), 'payment_declines_v2');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // The browser's Back: the address loses its tool, and nobody was asked first.
    const confirm = vi.fn(() => false);
    vi.stubGlobal('confirm', confirm);
    await router.navigate([], { queryParams: { tool: null }, queryParamsHandling: 'merge' });
    await settle(f);

    expect(confirm).toHaveBeenCalledWith('This tool has unsaved changes. Discard them?');
    // "No": the editor is still open on what was typed, and the address names the tool again.
    expect(name().value).toBe('payment_declines_v2');
    expect(router.url).toContain('tool=t1');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // Back to another tab of the section is the same question.
    await router.navigate([], { queryParams: { mcp: 'profiles', tool: null }, queryParamsHandling: 'merge' });
    await settle(f);
    expect(confirm).toHaveBeenCalledTimes(2);
    expect(name().value).toBe('payment_declines_v2');
    expect(el(f, '.m-tab.active').textContent).toContain('Tools');
    expect(router.url).toContain('mcp=tools');
    expect(router.url).toContain('tool=t1');

    // "Yes": the edit is dropped, and the section goes where the address points.
    confirm.mockReturnValue(true);
    await router.navigate([], { queryParams: { tool: null }, queryParamsHandling: 'merge' });
    await settle(f);
    expect(all(f, 'app-mcp-tool-editor')).toEqual([]);
    expect(router.url).not.toContain('tool=');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('follows Back and Forward out of an editor that holds nothing unsaved without a question', async () => {
    const f = await openEditor();
    const confirm = vi.fn(() => false);
    vi.stubGlobal('confirm', confirm);

    await router.navigate([], { queryParams: { tool: null }, queryParamsHandling: 'merge' });
    await settle(f);
    expect(confirm).not.toHaveBeenCalled();
    expect(all(f, 'app-mcp-tool-editor')).toEqual([]);
    expect(text(f)).toContain('Published tools');
  });

  it('an enum parameter shows its default, chosen from its values — and sends no default it does not show', async () => {
    const f = await openEditor(WITH_DEFAULT);
    const row = (): HTMLElement => all(f, 'app-mcp-tool-editor [role=row].param')[1];
    const typeOf = (): HTMLSelectElement => row().querySelector<HTMLSelectElement>('select[aria-label="Type"]')!;
    const values = (): HTMLInputElement => row().querySelector<HTMLInputElement>('input[aria-label="Allowed values"]')!;
    const fallback = <T extends HTMLElement>(): T => row().querySelector<T>('[aria-label="Default value"]')!;
    expect(fallback<HTMLInputElement>().value).toBe('fraud');

    /** Saves the tool and answers with what was sent; the parameter under test, as it went out. */
    const save = async (): Promise<unknown> => {
      button(f, 'Save', 'app-mcp-tool-editor').click();
      await settle(f);
      const put = http.expectOne({ method: 'PUT', url: '/api/mcp/tools/t1' });
      const saved: McpPublishedTool = { ...WITH_DEFAULT, parameters: put.request.body.parameters };
      put.flush(saved);
      await lists(f, PROFILES, [saved]);
      await offered(f);
      return put.request.body.parameters[1];
    };

    // String → enum, with values that do not hold the old default. The row has no way to show
    // "fraud" any more — so "fraud" must not ride along with the tool out of sight.
    choose(typeOf(), 'enum');
    await settle(f);
    type(values(), 'limit, stolen');
    await settle(f);
    expect(fallback<HTMLSelectElement>().tagName).toBe('SELECT');
    expect([...fallback<HTMLSelectElement>().options].map(o => o.value)).toEqual(['', 'limit', 'stolen']);
    expect(fallback<HTMLSelectElement>().value).toBe('');
    expect(await save()).toEqual({
      name: 'reason', type: 'enum', required: false, description: '', values: ['limit', 'stolen'], default: null,
    });

    // The default is one of the values …
    choose(fallback<HTMLSelectElement>(), 'stolen');
    await settle(f);
    expect(await save()).toMatchObject({ values: ['limit', 'stolen'], default: 'stolen' });

    // … and goes when the values no longer hold it.
    type(values(), 'limit, lost');
    await settle(f);
    expect(fallback<HTMLSelectElement>().value).toBe('');
    expect(await save()).toMatchObject({ values: ['limit', 'lost'], default: null });

    // Every child of a parameter's row is a cell, and the controls keep their own roles inside.
    expect([...row().children].map(c => c.getAttribute('role'))).toEqual(['cell', 'cell', 'cell', 'cell', 'cell']);
  });

  it('a default goes when the parameter is retyped to a type that does not take it', async () => {
    const f = await openEditor(WITH_DEFAULT);
    const row = (): HTMLElement => all(f, 'app-mcp-tool-editor [role=row].param')[1];
    const typeOf = (): HTMLSelectElement => row().querySelector<HTMLSelectElement>('select[aria-label="Type"]')!;
    const fallback = (): HTMLInputElement => row().querySelector<HTMLInputElement>('input[aria-label="Default value"]')!;

    choose(typeOf(), 'number');
    await settle(f);
    expect(fallback().value).toBe('');

    // A default the new type takes is kept.
    type(fallback(), '5');
    await settle(f);
    choose(typeOf(), 'string');
    await settle(f);
    expect(fallback().value).toBe('5');
    choose(typeOf(), 'number');
    await settle(f);
    expect(fallback().value).toBe('5');
    choose(typeOf(), 'boolean');
    await settle(f);
    expect(fallback().value).toBe('');
  });

  it('a refused tool names its field, and the field points at the sentence', async () => {
    const f = await openEditor();
    const name = el<HTMLInputElement>(f, 'app-mcp-tool-editor input[placeholder="payment_declines"]');
    type(name, 'logs_search');
    await settle(f);
    button(f, 'Save', 'app-mcp-tool-editor').click();
    await settle(f);
    http.expectOne({ method: 'PUT', url: '/api/mcp/tools/t1' })
      .flush({ error: "'logs_search' is the name of a built-in tool.", field: 'name' }, { status: 400, statusText: 'Bad Request' });
    await settle(f);

    expect(el(f, '#mcp-tool-error').textContent).toContain('is the name of a built-in tool');
    expect(name.getAttribute('aria-invalid')).toBe('true');
    expect(name.getAttribute('aria-describedby')).toBe('mcp-tool-error');
    // A field the sentence is not about is left alone.
    expect(el(f, '#mcp-tool-filter').hasAttribute('aria-invalid')).toBe(false);
  });

  // ── Activity ────────────────────────────────────────────────────────────────

  it('reads the record of calls for the last day and takes the picked call apart', async () => {
    const f = await open('activity');
    const get = http.expectOne(isActivity);
    expect(get.request.params.get('count')).toBe('50');
    expect(get.request.params.has('profile')).toBe(false);
    // The window is the last 24 hours, give or take the time the test took.
    const from = new Date(get.request.params.get('from')!).getTime();
    expect(Math.abs(Date.now() - 24 * 3_600_000 - from)).toBeLessThan(60_000);
    get.flush({
      more: false,
      calls: [call('42', 'Refused', {
        events: 0, masked: 0, elapsedMs: 1, arguments: '{"filter":"CardNumber like \'4%\'"}',
        refusal: 'CardNumber is a hidden field.', refusalKind: 'masking', filter: "CardNumber like '4%'",
        client: 'claude-code 2.1 · MCP 2026-07-28',
      })],
    });
    await settle(f);

    const row = el(f, '[role=row].pick');
    expect(row.textContent).toContain('logs_search');
    expect(row.textContent).toContain('Refused');
    // A refused call returned nothing, which is not the same as returning zero events.
    expect(row.textContent).toContain('—');
    // An ordinary call stands for itself: no count beside its status.
    expect(row.textContent).not.toContain('×');
    expect(text(f)).toContain('Refused by the masking rules');
    expect(text(f)).toContain('CardNumber is a hidden field.');
    expect(text(f)).toContain('claude-code 2.1 · MCP 2026-07-28');
    // The record holds the filter as the call wrote it — which is all its caption claims for it.
    expect(all(f, 'app-mcp-activity-tab .m-side .m-lbl').map(l => l.textContent))
      .toEqual(['What the AI was told', 'Arguments', 'Filter as written']);
    expect(text(f)).toContain('Showing 1 of 1 284 calls');
  });

  it('says how many refusals a record stands for when a profile ran over its limit', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({
      more: false,
      calls: [
        call('44', 'Refused', { refusalKind: 'limit', refusal: 'The profile has made its 30 calls of this minute.', repeats: 8 }),
        call('43', 'Refused', { refusalKind: 'limit', refusal: 'The profile has made its 30 calls of this minute.', repeats: 1 }),
        call('42', 'OK', { repeats: 0 }),
      ],
    });
    await settle(f);

    const rows = all(f, '[role=row].pick');
    expect(rows[0].textContent).toContain('×8');
    expect(rows[1].textContent).not.toContain('×');
    expect(rows[2].textContent).not.toContain('×');
    expect(text(f)).toContain('Refused 8 times this minute so far — only the 1st, 2nd, 4th, 8th… are recorded.');

    // The first refusal of a minute is one refusal: nothing to add.
    rows[1].click();
    await settle(f);
    expect(text(f)).not.toContain('this minute so far');
  });

  it('a filter that changes abandons the page being loaded under the old one', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({ more: true, calls: [call('42', 'OK')] });
    await settle(f);

    button(f, 'Load more').click();
    await settle(f);
    const more = http.expectOne(isActivity);
    expect(more.request.params.get('afterId')).toBe('42');

    // The filter changes while that page is on its way.
    choose(el<HTMLSelectElement>(f, 'select[aria-label="Status"]'), 'Refused');
    await settle(f);
    const list = http.expectOne(isActivity);
    expect(list.request.params.get('status')).toBe('Refused');
    expect(list.request.params.has('afterId')).toBe(false);
    list.flush({ more: false, calls: [call('77', 'Refused', { refusalKind: 'limit' })] });
    // The old page arrives last — or would, had it not been abandoned.
    expect(more.cancelled).toBe(true);
    if (!more.cancelled) more.flush({ more: false, calls: [call('41', 'OK')] });
    await settle(f);

    const rows = all(f, '[role=row].pick');
    expect(rows.length).toBe(1);
    expect(rows[0].textContent).toContain('Refused');
    expect(all(f, 'app-mcp-activity-tab button').map(b => b.textContent?.trim())).not.toContain('Load more');
  });

  it('a read of the calls that fails shows the reason, and nothing that reads as an answer', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({ error: 'Too many searches are running. Try again in a moment.' }, BUSY);
    await settle(f);

    expect(el(f, 'app-mcp-activity-tab [role=alert]').textContent).toContain('Too many searches are running.');
    expect(text(f)).not.toContain('No calls in this window.');
    expect(text(f)).not.toContain('Showing');
  });

  it('calls read under one filter are not left on screen when the read under another fails', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({ more: false, calls: [call('42', 'OK')] });
    await settle(f);

    choose(el<HTMLSelectElement>(f, 'select[aria-label="Status"]'), 'Refused');
    await settle(f);
    http.expectOne(isActivity).flush({ error: 'Too many searches are running. Try again in a moment.' }, BUSY);
    await settle(f);
    expect(all(f, '[role=row].pick')).toEqual([]);
    expect(el(f, 'app-mcp-activity-tab [role=alert]').textContent).toContain('Too many searches are running.');

    // The same filters read again and failing: what is on screen answered them, and stays.
    choose(el<HTMLSelectElement>(f, 'select[aria-label="Status"]'), '');
    await settle(f);
    http.expectOne(isActivity).flush({ more: false, calls: [call('42', 'OK')] });
    await settle(f);
    el<HTMLButtonElement>(f, 'button[aria-label="Read the list again"]').click();
    await settle(f);
    http.expectOne(isActivity).flush({ error: 'Too many searches are running. Try again in a moment.' }, BUSY);
    http.expectOne(isSummary).flush(SUMMARY);
    http.expectOne(isUsage).flush(USAGE);
    await settle(f);
    expect(all(f, '[role=row].pick').length).toBe(1);
  });

  it('reads the figures again on "read again" and on coming back — not on every filter', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({ more: false, calls: [call('42', 'OK')] });
    await settle(f);

    // A filter cuts the list; the figures are of the whole record, and are not asked for again
    // (the case would fail on the two requests nobody answered).
    choose(el<HTMLSelectElement>(f, 'select[aria-label="Time range"]'), '1');
    await settle(f);
    http.expectOne(isActivity).flush({ more: false, calls: [call('42', 'OK')] });
    await settle(f);

    // "Read again" is a look at the record as it is now: the list and the figures.
    el<HTMLButtonElement>(f, 'button[aria-label="Read the list again"]').click();
    await settle(f);
    http.expectOne(isActivity).flush({ more: false, calls: [call('42', 'OK')] });
    http.expectOne(isSummary).flush({ ...SUMMARY, calls: 1300 });
    http.expectOne(isUsage).flush(USAGE);
    await settle(f);

    // So is coming back to the tab.
    await show(f, 'Connection');
    expect(text(f)).toContain('1 300');
    await show(f, 'Activity');
    http.expectOne(isActivity).flush({ more: false, calls: [call('42', 'OK')] });
    http.expectOne(isSummary).flush({ ...SUMMARY, calls: 1301 });
    http.expectOne(isUsage).flush(USAGE);
    await settle(f);
  });

  it('the list of calls is one stop for the Tab key, walked with the arrows', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({ more: false, calls: [call('44', 'OK'), call('43', 'Trimmed'), call('42', 'Refused')] });
    await settle(f);
    const rows = (): HTMLElement[] => all(f, '[role=row].pick');
    expect(rows().map(r => r.getAttribute('tabindex'))).toEqual(['0', '-1', '-1']);

    press(rows()[0], 'ArrowDown');
    await settle(f);
    expect(rows().map(r => r.getAttribute('tabindex'))).toEqual(['-1', '0', '-1']);
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['false', 'true', 'false']);
    expect(document.activeElement).toBe(rows()[1]);
    expect(el(f, 'app-mcp-activity-tab .m-side .m-pill').textContent).toBe('Trimmed');

    press(rows()[1], 'ArrowUp');
    await settle(f);
    press(rows()[0], 'ArrowUp');
    await settle(f);
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['true', 'false', 'false']);
    expect(document.activeElement).toBe(rows()[0]);

    for (const key of ['Enter', ' ']) {
      press(rows()[key === ' ' ? 2 : 1], key);
      await settle(f);
    }
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['false', 'false', 'true']);
  });
});
