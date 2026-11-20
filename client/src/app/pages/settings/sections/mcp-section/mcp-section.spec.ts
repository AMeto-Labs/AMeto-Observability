import { ComponentFixture, TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { Location, PlatformNavigation } from '@angular/common';
import { HttpRequest, provideHttpClient } from '@angular/common/http';
import { HttpTestingController, TestRequest, provideHttpClientTesting } from '@angular/common/http/testing';
import { Router, provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { McpCall, McpOverview, McpPreview, McpProfile, McpPublishedTool, McpRule, McpSummary } from '../../../../core/models/mcp.model';
import { SettingsDirtyService } from '../../settings-dirty.service';
import { DONE_PAUSE_MS, DOUBLE_CLICK_MS, McpSectionComponent } from './mcp-section';

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

const SUMMARY: McpSummary = {
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
  summary?: McpSummary;
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
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
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
   *
   * Under fake timers the clock stands still: what the next turns of the event loop would run —
   * the store's next step, the change detection after it — is run by moving the clock a
   * millisecond a turn (a timer set for no delay while the clock moves is due a millisecond
   * later), and a pause (Done's, say) is passed only by moving it past the pause. `whenStable`
   * is not waited for there: it would wait for a timer that only the case can move.
   */
  async function settle(f: ComponentFixture<unknown>): Promise<void> {
    if (vi.isFakeTimers()) {
      for (let turn = 0; turn < 4; turn++) await vi.advanceTimersByTimeAsync(1);
      return;
    }
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
    http.expectOne(isSummary).flush(world.summary ?? SUMMARY);
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

  /**
   * The read of both lists that follows a change to either, answered. Told by its method: a new
   * profile still on its way is a POST to the same address.
   */
  async function lists(f: ComponentFixture<unknown>, profiles: McpProfile[], tools: McpPublishedTool[]): Promise<void> {
    await settle(f);
    http.expectOne({ method: 'GET', url: '/api/mcp/profiles' }).flush(profiles);
    http.expectOne({ method: 'GET', url: '/api/mcp/tools' }).flush({ builtIn: OVERVIEW.tools, published: tools });
    await settle(f);
  }

  /** The same read, failing: the profiles are refused, which abandons the tools. */
  async function failLists(f: ComponentFixture<unknown>): Promise<void> {
    await settle(f);
    http.expectOne({ method: 'GET', url: '/api/mcp/profiles' }).flush({ error: 'The store is busy.' }, BUSY);
    expect(http.expectOne({ method: 'GET', url: '/api/mcp/tools' }).cancelled).toBe(true);
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
    // A server that does not count unrecorded refusals: every figure is a count, not a floor.
    expect(text(f)).not.toContain('≥');
    expect(all(f, '.m-tile[title]')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('counts the refusals past a limit that were not recorded one by one, and says the totals are floors', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    // 60 refusals past a profile's minute are known only by the counts their minute's records carry.
    await land(f, { summary: { ...SUMMARY, unrecorded: 60, refusals: { limit: 65, masking: 2 } } });
    const tile = (label: string): HTMLElement => {
      const found = all(f, '.m-tile').find(t => t.querySelector('.m-hint')?.textContent === label);
      if (!found) throw new Error(`no tile says "${label}"`);
      return found;
    };

    // The record's 1 284 calls and 7 refusals, and the 60 it holds no record of: at least that many.
    expect(tile('Calls').querySelector('b')?.textContent).toBe('≥ 1 344');
    expect(tile('Refused').querySelector('b')?.textContent).toBe('≥ 67');
    expect(tile('Refused').textContent).toContain('≥ 65 by limits, 2 by masking');
    for (const label of ['Calls', 'Refused']) {
      expect(tile(label).getAttribute('title')).toBe(
        'At least: past a profile’s calls-per-minute limit only the 1st, 2nd, 4th, 8th… refusal of a minute is recorded.');
    }
    // What the unrecorded refusals have no part in stays a count.
    expect(tile('Events returned').hasAttribute('title')).toBe(false);
    expect(tile('Events returned').querySelector('b')?.textContent).toBe('48 310');
  });

  it('says the totals are floors whenever refusals past a limit were thinned, none unrecorded included', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    // A minute whose last record is its 2nd refusal: nothing is known to be unrecorded, and a
    // 3rd may have come — which no record holds.
    await land(f, { summary: { ...SUMMARY, unrecorded: 0, thinned: true, refusals: { limit: 2 } } });
    const tile = (label: string): HTMLElement => {
      const found = all(f, '.m-tile').find(t => t.querySelector('.m-hint')?.textContent === label);
      if (!found) throw new Error(`no tile says "${label}"`);
      return found;
    };

    // The separators are no-break spaces, as fmtCount and the sign write them.
    expect(tile('Calls').querySelector('b')?.textContent).toMatch(/^≥\s1\s284$/u);
    expect(tile('Refused').querySelector('b')?.textContent).toMatch(/^≥\s7$/u);
    expect(tile('Refused').textContent).toMatch(/≥\s2 by limits/u);
    expect(tile('Calls').getAttribute('title')).toContain('At least');
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

  it('the key dialog takes no place in the section\'s column, so no gap is put before it', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);
    // The section is a flex column with gaps; its last item is the key dialog's host, which
    // holds nothing that takes room (the dialog itself is drawn over the page).
    const host = el(f, ':scope > app-modal');
    expect(host.nextElementSibling).toBeNull();
    expect(getComputedStyle(host).display).toBe('contents');
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

  it('the widest window stops pointing at the refusal once it is put right', async () => {
    const f = TestBed.createComponent(McpSectionComponent);
    await land(f);
    const window = el<HTMLInputElement>(f, 'input[aria-label="Widest time window"]');
    type(window, 'a week');
    await settle(f);
    button(f, 'Save limits').click();
    await settle(f);
    expect(el(f, '#mcp-limits-error').textContent).toContain('Write the widest window with a unit');
    expect(window.getAttribute('aria-describedby')).toBe('mcp-limits-error');

    type(window, '7d');
    await settle(f);
    expect(window.hasAttribute('aria-invalid')).toBe(false);
    expect(window.hasAttribute('aria-describedby')).toBe(false);
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

  it('saving a profile sends what was changed in its card, the granted tools as one list', async () => {
    const f = await open('profiles');
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    // The published tool's switch: off withdraws it from this profile.
    el<HTMLButtonElement>(f, 'button.m-sw[aria-label="payment_declines"]').click();
    await settle(f);

    button(f, 'Save').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(put.request.body).toEqual({ name: 'Payments developers', publishedTools: [] });
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

  it('a card filled before somebody else narrowed the profile cannot widen it again by saving a new name', async () => {
    const f = await open('profiles');
    // Meanwhile, elsewhere, the profile was made Strict and given another service. This card has
    // not heard of it, and still shows Standard and payments-api.
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);

    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(put.request.body).toEqual({ name: 'Payments developers' });
    const saved: McpProfile = { ...PROFILES[0], name: 'Payments developers', maskingMode: 'strict', services: ['acquiring-gw'] };
    put.flush(saved);
    await lists(f, [saved], TOOLS);

    // The card is the profile as the server kept it: the narrowing stands.
    expect(button(f, 'Strict', 'app-mcp-profiles-tab [aria-label="Masking"]').getAttribute('aria-pressed')).toBe('true');
    expect(all(f, 'app-mcp-profiles-tab .m-chip.m-cd').map(c => c.textContent?.trim())).toEqual(['acquiring-gw']);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a save answered after the card moved to another profile leaves that card as it is', async () => {
    const f = await open('profiles', { profiles: [PROFILES[0], SUPPORT] });
    const name = (): HTMLInputElement => el(f, 'input[placeholder="Payments team"]');
    const rows = (): HTMLElement[] => all(f, '[role=row].pick');
    type(name(), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });

    // While the save is on its way the other profile is picked — after the question — and typed into.
    const confirm = vi.fn(() => true);
    vi.stubGlobal('confirm', confirm);
    rows()[1].click();
    await settle(f);
    expect(confirm).toHaveBeenCalledTimes(1);
    type(name(), 'Support desk');
    await settle(f);

    const saved = { ...PROFILES[0], name: 'Payments developers' };
    put.flush(saved);
    await settle(f);
    // The card is still the one being typed into, with what was typed; the save is in the list.
    expect(el(f, 'app-mcp-profiles-tab .card .section-title').textContent).toBe('Support');
    expect(name().value).toBe('Support desk');
    expect(rows().map(r => r.getAttribute('aria-selected'))).toEqual(['false', 'true']);
    expect(rows()[0].textContent).toContain('Payments developers');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
    await lists(f, [saved, SUPPORT], TOOLS);
    expect(name().value).toBe('Support desk');
  });

  it('a save refused after the card moved on names its profile, and marks nothing on the card that is open', async () => {
    const f = await open('profiles', { profiles: [PROFILES[0], SUPPORT] });
    const name = (): HTMLInputElement => el(f, 'input[placeholder="Payments team"]');
    type(name(), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[1].click();
    await settle(f);

    put.flush({ error: 'A profile is called that already.', field: 'name' }, { status: 400, statusText: 'Bad Request' });
    await settle(f);
    expect(el(f, '#mcp-profile-error').textContent).toBe('“Payments team”: A profile is called that already.');
    expect(name().value).toBe('Support');
    expect(name().hasAttribute('aria-invalid')).toBe(false);
    expect(name().hasAttribute('aria-describedby')).toBe(false);
  });

  it('what is typed while a save is on its way stays unsaved, over the profile as it was saved', async () => {
    const f = await open('profiles');
    const name = (): HTMLInputElement => el(f, 'input[placeholder="Payments team"]');
    const description = (): HTMLInputElement => el(f, 'input[placeholder="Who or what uses this key?"]');
    type(name(), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });

    // The description is typed while the name is on its way.
    type(description(), 'Claude Code and Cursor');
    await settle(f);
    const saved = { ...PROFILES[0], name: 'Payments developers' };
    put.flush(saved);
    await lists(f, [saved], TOOLS);

    expect(name().value).toBe('Payments developers');
    expect(description().value).toBe('Claude Code and Cursor');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
    // The name is saved; what is left to save is the description alone.
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const again = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(again.request.body).toEqual({ description: 'Claude Code and Cursor' });
    const described = { ...saved, description: 'Claude Code and Cursor' };
    again.flush(described);
    await lists(f, [described], TOOLS);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a window typed another way is not an unsaved change of the card', async () => {
    const f = await open('profiles');
    const window = el<HTMLInputElement>(f, 'app-mcp-profiles-tab input[placeholder="24h"]');
    expect(window.value).toBe('24 h');

    for (const same of ['24h', '1 d', '1440 min']) {
      type(window, same);
      await settle(f);
      expect(dirty.dirtyTabsList()).toEqual([]);
      expect(button(f, 'Save', 'app-mcp-profiles-tab').disabled).toBe(true);
    }
    type(window, '25h');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
  });

  it('a refusal stops marking a field of the card once the field holds another value', async () => {
    const f = await open('profiles');
    const name = (): HTMLInputElement => el(f, 'input[placeholder="Payments team"]');
    type(name(), 'Support');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' })
      .flush({ error: 'A profile is called that already.', field: 'name' }, { status: 400, statusText: 'Bad Request' });
    await settle(f);
    expect(name().getAttribute('aria-invalid')).toBe('true');
    expect(name().getAttribute('aria-describedby')).toBe('mcp-profile-error');

    // Another field changed: the refusal is still about the name, and stays.
    type(el<HTMLInputElement>(f, 'input[placeholder="Who or what uses this key?"]'), 'Cursor');
    await settle(f);
    expect(name().getAttribute('aria-invalid')).toBe('true');
    // The name changed: what was refused is no longer there to point at.
    type(name(), 'Support desk');
    await settle(f);
    expect(name().hasAttribute('aria-invalid')).toBe(false);
    expect(name().hasAttribute('aria-describedby')).toBe(false);
    expect(all(f, '#mcp-profile-error')).toEqual([]);
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

  it('a grant switched on for a tool that is then deleted leaves nothing unsaved in the card', async () => {
    const f = await open('profiles', { tools: [TOOLS[0], REFUNDS] });
    el<HTMLButtonElement>(f, 'app-mcp-profiles-tab button.m-sw[aria-label="refunds"]').click();
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // The tool the switch was for is deleted on the Tools tab.
    await show(f, 'Tools');
    await offered(f);
    el<HTMLButtonElement>(f, 'button[aria-label="Delete refunds"]').click();
    await settle(f);
    button(f, 'Delete tool', '.modal').click();
    await settle(f);
    http.expectOne({ method: 'DELETE', url: '/api/mcp/tools/t2' }).flush(null, NO_CONTENT);
    await lists(f, PROFILES, [TOOLS[0]]);
    await offered(f);

    // A grant of a tool that is no more is no grant — the server would refuse its id — so the
    // switch is all there was to save, and it is gone with its tool.
    await show(f, 'Profiles');
    expect(all(f, 'app-mcp-profiles-tab button.m-sw[aria-label="refunds"]')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual([]);
    expect(button(f, 'Save', 'app-mcp-profiles-tab').disabled).toBe(true);
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
    // A new profile is all of it. No tool was granted in the card, so no grants are sent.
    expect(post.request.body).toEqual({
      name: 'Support', description: '', toolsMode: 'builtInAndPublished', tools: ['logs_overview', 'logs_search'],
      services: [], maskingMode: 'standard', freeFilters: true, maxEvents: 200, maxWindowMinutes: 1440, callsPerMinute: 30,
    });
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await failLists(f);

    expect(el(f, '.modal__title').textContent).toBe('Key of “Support”');
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    button(f, 'Done', '.modal').click();
    await settle(f);

    // The card is the profile that now exists: saving it again is a change, not a second profile.
    expect(el(f, 'app-mcp-profiles-tab .card .section-title').textContent).toBe('Support');
    expect(all<HTMLButtonElement>(f, 'button').map(b => b.textContent?.trim())).not.toContain('Create profile');
    expect(all(f, '[role=row].pick').map(r => r.classList.contains('sel'))).toEqual([false, true]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a key on its way names its profile when it arrives, whichever card is open by then', async () => {
    const f = await open('profiles');
    button(f, 'New profile').click();
    await settle(f);
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Support');
    await settle(f);
    button(f, 'Create profile').click();
    await settle(f);
    const post = http.expectOne({ method: 'POST', url: '/api/mcp/profiles' });

    // While the key is on its way the first profile is picked again, after the question.
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    // Nothing is typed into that card — and the page has something to lose all the same.
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    // The key is shown, and says whose it is: the card under it is another profile's, and stays so.
    expect(el(f, '.modal__title').textContent).toBe('Key of “Support”');
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    expect(el(f, 'app-mcp-profiles-tab .card .section-title').textContent).toBe('Payments team');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a new profile written while another is being created is not the one created', async () => {
    const f = await open('profiles');
    const name = (): HTMLInputElement => el(f, 'input[placeholder="Payments team"]');
    button(f, 'New profile').click();
    await settle(f);
    type(name(), 'Support');
    await settle(f);
    button(f, 'Create profile').click();
    await settle(f);
    const post = http.expectOne({ method: 'POST', url: '/api/mcp/profiles' });

    // Away to the first profile and back to a blank new one — another card, though it is "new" too.
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    button(f, 'New profile').click();
    await settle(f);
    type(name(), 'Operations');
    await settle(f);

    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    expect(el(f, '.modal__title').textContent).toBe('Key of “Support”');
    button(f, 'Done', '.modal').click();
    await settle(f);
    // The card is still the profile being written, not the one just created under another name.
    expect(el(f, 'app-mcp-profiles-tab .card .section-title').textContent).toBe('New profile');
    expect(name().value).toBe('Operations');
    expect(button(f, 'Create profile').disabled).toBe(false);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
  });

  it('the rotate and delete questions stay up while their answers are on their way', async () => {
    const f = await open('profiles', { profiles: [PROFILES[0], SUPPORT] });
    const closeButtons = (): HTMLElement[] => all(f, '.modal button[title="Close"]');

    button(f, 'Rotate', 'app-mcp-profiles-tab .card').click();
    await settle(f);
    // Before it is answered, the question can be put away …
    expect(closeButtons().length).toBe(1);
    button(f, 'Rotate key', '.modal').click();
    await settle(f);
    const rotate = http.expectOne({ method: 'POST', url: '/api/mcp/profiles/p1/rotate-key' });
    // … but not while its answer is on its way: no close button, and a click beside it does nothing.
    expect(closeButtons()).toEqual([]);
    el(f, '.modal-backdrop').click();
    await settle(f);
    expect(el(f, '.modal__title').textContent).toBe('Rotate the key?');
    // The old key stops working with this answer, and the new one is not on screen yet.
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
    rotate.flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }, SUPPORT], TOOLS);
    expect(el(f, '.modal__title').textContent).toBe('Key of “Payments team”');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual([]);

    button(f, 'Delete profile', 'app-mcp-profiles-tab .card').click();
    await settle(f);
    button(f, 'Delete profile', '.modal').click();
    await settle(f);
    const remove = http.expectOne({ method: 'DELETE', url: '/api/mcp/profiles/p1' });
    expect(closeButtons()).toEqual([]);
    el(f, '.modal-backdrop').click();
    await settle(f);
    expect(el(f, '.modal__title').textContent).toBe('Delete the profile?');

    // Deleted: the question goes, and the card moves to the profile that is left.
    remove.flush(null, NO_CONTENT);
    await lists(f, [SUPPORT], [{ ...TOOLS[0], profiles: [] }]);
    expect(all(f, '.modal')).toEqual([]);
    expect(all(f, '[role=row].pick').map(r => r.querySelector('.m-strong')?.textContent)).toEqual(['Support']);
    expect(el(f, 'app-mcp-profiles-tab .card .section-title').textContent).toBe('Support');
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  /** Creates a profile called Support from a blank card, and leaves the request on its way. */
  async function createSupport(f: ComponentFixture<unknown>): Promise<TestRequest> {
    button(f, 'New profile').click();
    await settle(f);
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Support');
    await settle(f);
    button(f, 'Create profile').click();
    await settle(f);
    return http.expectOne({ method: 'POST', url: '/api/mcp/profiles' });
  }

  /** Asks for a new key of the profile in the card, and leaves the request on its way. */
  async function rotateInCard(f: ComponentFixture<unknown>): Promise<TestRequest> {
    button(f, 'Rotate', 'app-mcp-profiles-tab .card').click();
    await settle(f);
    button(f, 'Rotate key', '.modal').click();
    await settle(f);
    return http.expectOne({ method: 'POST', url: '/api/mcp/profiles/p1/rotate-key' });
  }

  it('a save answered while a rotation is on its way leaves the page asking before a reload', async () => {
    const f = await open('profiles');
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    const rotate = await rotateInCard(f);

    // The save answers first. Nothing typed is left unsaved — but the old key has stopped
    // working with the rotation, and the new one has not arrived: a reload would lose it.
    const saved = { ...PROFILES[0], name: 'Payments developers' };
    put.flush(saved);
    await lists(f, [saved], TOOLS);
    expect(button(f, 'Save', 'app-mcp-profiles-tab').disabled).toBe(true);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // The key arrives, named as the list names its profile by then.
    rotate.flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...saved, keyPreview: 'c0ffee00…' }], TOOLS);
    expect(el(f, '.modal__title').textContent).toBe('Key of “Payments developers”');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a key on screen is named as the list names its profile now — and, once the profile is gone, as when it arrived', async () => {
    const f = await open('profiles');
    const title = (): string | null => el(f, '.modal__title').textContent;
    type(el<HTMLInputElement>(f, 'input[placeholder="Payments team"]'), 'Payments developers');
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    const rotate = await rotateInCard(f);

    // The key answers first: the profile is still called what it was.
    rotate.flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }], TOOLS);
    expect(title()).toBe('Key of “Payments team”');

    // The rename is answered while the key is on screen: the dialog calls the profile what the list does.
    put.flush({ ...PROFILES[0], name: 'Payments developers', keyPreview: 'c0ffee00…' });
    await settle(f);
    expect(title()).toBe('Key of “Payments developers”');

    // Deleted somewhere else meanwhile: the read after the save lists it no more. The key is
    // still the one shown, under the name it came with.
    await lists(f, [], [{ ...TOOLS[0], profiles: [] }]);
    expect(title()).toBe('Key of “Payments team”');
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
  });

  it('a key taken down while another is still on its way leaves the page asking before a reload', async () => {
    const f = await open('profiles');
    const post = await createSupport(f);
    // While the new profile is on its way, the first one's key is rotated — and that answers first.
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    (await rotateInCard(f)).flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }], TOOLS);
    expect(el(f, '.modal__title').textContent).toBe('Key of “Payments team”');
    vi.useFakeTimers();
    button(f, 'Done', '.modal').click();
    await settle(f);

    // Nothing is on screen and nothing typed is unsaved; the new profile's key is still out.
    expect(all(f, '.modal')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }, SUPPORT], TOOLS);
    expect(el(f, '.modal__title').textContent).toBe('Key of “Support”');
    // It came up a moment after a key was taken down, under where that key's Done was: its own
    // Done waits that moment out, as it would had it come up in the other's place.
    expect(button(f, 'Done', '.modal').getAttribute('aria-disabled')).toBe('true');
    await vi.advanceTimersByTimeAsync(DONE_PAUSE_MS);
    await settle(f);
    expect(button(f, 'Done', '.modal').hasAttribute('aria-disabled')).toBe(false);
    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('keys that arrive one after the other are all shown, one at a time, over any question still up', async () => {
    const f = await open('profiles');
    const post = await createSupport(f);
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    const rotate = await rotateInCard(f);

    // The new profile's key arrives while the question about the rotation is still up. Every
    // dialog stands at the same height, and of two the later one in the page is drawn on top:
    // the key is shown over the question, its Copy and Done within reach.
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    expect(all(f, '.modal__title').map(t => t.textContent)).toEqual(['Rotate the key?', 'Key of “Support”']);
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');

    // The rotated key arrives: it waits for its turn, and does not take the place of the one on screen.
    rotate.flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }, SUPPORT], TOOLS);
    expect(all(f, '.modal__title').map(t => t.textContent)).toEqual(['Key of “Support”']);
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    // The dialog says another key follows, so the next one is no surprise.
    expect(el(f, '.modal').textContent).toContain('Another key is waiting: it is shown after this one.');

    vi.useFakeTimers();
    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(all(f, '.modal__title').map(t => t.textContent)).toEqual(['Key of “Payments team”']);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    expect(el(f, '.modal').textContent).not.toContain('waiting');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // Its Done takes it down once the moment after the one before has passed.
    await vi.advanceTimersByTimeAsync(DONE_PAUSE_MS);
    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(all(f, '.modal')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a double click on Done takes down the key on screen, not the one that comes up in its place', async () => {
    const f = await open('profiles');
    // A new profile on its way, and the first profile's key rotated meanwhile: both answer.
    const post = await createSupport(f);
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    const rotate = await rotateInCard(f);
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    rotate.flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }, SUPPORT], TOOLS);

    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    expect(el(f, '.modal').textContent).toContain('Another key is waiting');
    // Nothing was taken down before it: its Done takes no pause.
    const done = button(f, 'Done', '.modal');
    expect(done.hasAttribute('aria-disabled')).toBe(false);

    // The first click takes the new profile's key down. The rotated key comes up in the same
    // dialog, under the very same Done, which keeps the focus — and is held.
    vi.useFakeTimers();
    done.focus();
    done.click();
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    expect(button(f, 'Done', '.modal')).toBe(done);
    expect(document.activeElement).toBe(done);
    expect(done.getAttribute('aria-disabled')).toBe('true');

    // The second click of the double click lands there: it takes nothing down.
    done.click();
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // A moment later Done is Done again, for the key that has now been seen.
    await vi.advanceTimersByTimeAsync(DONE_PAUSE_MS);
    await settle(f);
    expect(done.hasAttribute('aria-disabled')).toBe(false);
    done.click();
    await settle(f);
    expect(all(f, '.modal')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  /** Two keys, the new profile's on screen and the rotated one waiting behind it. */
  async function twoKeys(): Promise<ComponentFixture<McpSectionComponent>> {
    const f = await open('profiles');
    const post = await createSupport(f);
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    const rotate = await rotateInCard(f);
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    rotate.flush({ key: 'the-rotated-key' });
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }, SUPPORT], TOOLS);
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    return f;
  }

  it('Done held is still in the pointer\'s way, and stays where it was when the next key comes up', async () => {
    const f = await twoKeys();
    // What is said of the keys waiting stands beside Done, in Done's own row: when it goes —
    // with the last key but one — the row is as tall as it was, and Done where it was.
    const done = button(f, 'Done', '.modal');
    const row = done.parentElement!;
    expect(row.classList.contains('m-actions')).toBe(true);
    expect(all(f, '.modal .m-key-behind').map(n => n.parentElement)).toEqual([row]);
    expect(row.querySelector('.m-key-behind')!.textContent).toBe('Another key is waiting: it is shown after this one.');

    // Done stands at one height for this key and for the next. In the centre of the window a
    // difference in height between two keys moved Done by half of it: a profile's name that
    // took a second line of the title did, and so did what is said beside Done where it
    // wrapped. So the dialog's lower edge is what is held, not its middle; Done's row is two
    // lines of that sentence, with it or without, and the sentence is held to them — its whole
    // text is its title. The name in the dialog's title is not cut: it is how one key is told
    // from the next, and two names may differ at their end.
    const dialog = el(f, 'app-modal.m-key-dialog');
    expect(getComputedStyle(dialog.querySelector('.modal-backdrop')!).alignItems).toBe('flex-end');
    const drawnTitle = getComputedStyle(dialog.querySelector('.modal__title')!);
    expect([drawnTitle.whiteSpace, drawnTitle.textOverflow]).not.toContain('nowrap');
    expect([drawnTitle.whiteSpace, drawnTitle.textOverflow]).not.toContain('ellipsis');
    expect(getComputedStyle(row).minHeight).toBe('32px');
    const behind = row.querySelector<HTMLElement>('.m-key-behind')!;
    expect([getComputedStyle(behind).maxHeight, getComputedStyle(behind).overflow]).toEqual(['32px', 'hidden']);
    expect(behind.title).toBe(behind.textContent);

    vi.useFakeTimers();
    done.focus();
    done.click();
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    expect(all(f, '.modal .m-key-behind')).toEqual([]);
    expect(button(f, 'Done', '.modal')).toBe(done);
    expect(done.parentElement).toBe(row);
    expect(getComputedStyle(row).minHeight).toBe('32px');

    // Held, Done looks switched off and still takes the pointer: with pointer-events off, the
    // second click of a double click went through it to what lay behind, and the focus went
    // out of the dialog with it. The click that lands on it now is dropped where it is handled.
    expect(done.getAttribute('aria-disabled')).toBe('true');
    expect(getComputedStyle(done).pointerEvents).not.toBe('none');
    expect(document.activeElement).toBe(done);
  });

  it('a click counted as the second of a run takes nothing down while the key has only just come up', async () => {
    const f = await twoKeys();
    vi.useFakeTimers();
    const done = button(f, 'Done', '.modal');
    const click = (detail: number): boolean => done.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail }));
    click(1);
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');

    // A slow double click. A system can be set to count clicks 900 ms apart as one run, and the
    // pause is over by then; the browser says which click of its run this is.
    await vi.advanceTimersByTimeAsync(DONE_PAUSE_MS + 150);
    await settle(f);
    expect(done.hasAttribute('aria-disabled')).toBe(false);
    click(2);
    await settle(f);
    click(3);
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);

    // Not for ever: every click made inside the system's interval lengthens the run, and
    // somebody clicking on would never get through. A second after the key came up — one
    // second, and no longer — a click is a click, whichever of its run the browser calls it.
    expect(DOUBLE_CLICK_MS).toBe(1000);
    await vi.advanceTimersByTimeAsync(1000 - DONE_PAUSE_MS - 150);
    await settle(f);
    click(4);
    await settle(f);
    expect(all(f, '.modal')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('a key that queues behind the one on screen does not make that one a key just come up', async () => {
    const f = await open('profiles');
    const post = await createSupport(f);
    vi.stubGlobal('confirm', vi.fn(() => true));
    all(f, '[role=row].pick')[0].click();
    await settle(f);
    const rotate = await rotateInCard(f);
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');

    // The first key has been on screen for two seconds when the second arrives behind it.
    vi.useFakeTimers();
    await vi.advanceTimersByTimeAsync(2000);
    rotate.flush({ key: 'the-rotated-key' });
    await vi.advanceTimersByTimeAsync(50);
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    expect(el(f, '.modal').textContent).toContain('Another key is waiting');

    // A click the browser counts as the second of a run takes the first key down: it is that
    // key the click is judged by, and that key was read long ago.
    button(f, 'Done', '.modal').dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail: 2 }));
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    vi.useRealTimers();
    await lists(f, [{ ...PROFILES[0], keyPreview: 'c0ffee00…' }, SUPPORT], TOOLS);
  });

  it('a key that comes up under a double click begun elsewhere is not taken down by its second click', async () => {
    const f = await open('profiles');
    const post = await createSupport(f);
    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    vi.useFakeTimers();
    const done = button(f, 'Done', '.modal');
    const click = (detail: number): boolean => done.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, detail }));

    // Nothing was taken down before it, so Done is not held — and the click that lands on it
    // is the second of a run that began on whatever stood there before the dialog.
    expect(done.hasAttribute('aria-disabled')).toBe(false);
    await vi.advanceTimersByTimeAsync(200);
    click(2);
    await settle(f);
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');

    // A click of its own takes the key down at once; so does Enter or Space, whose click counts none.
    click(1);
    await settle(f);
    expect(all(f, '.modal')).toEqual([]);
  });

  it('Enter held down on Done is one press: its repeats activate nothing', async () => {
    const f = await twoKeys();
    const done = button(f, 'Done', '.modal');
    /** True when the page stopped the key from doing what it does — for Enter on a button, a click. */
    const stopped = (key: string, repeat: boolean): boolean =>
      !done.dispatchEvent(new KeyboardEvent('keydown', { key, repeat, bubbles: true, cancelable: true }));

    expect(stopped('Enter', false)).toBe(false);
    expect(stopped(' ', false)).toBe(false);
    // Held down, the key repeats, and past the pause each repeat would take a key down unseen.
    expect(stopped('Enter', true)).toBe(true);
    expect(stopped(' ', true)).toBe(true);
    // Any other key is none of Done's business, repeated or not.
    expect(stopped('Tab', true)).toBe(false);
  });

  it('for a moment after the last key is taken down, a click meant for its Done does not reach the page behind', async () => {
    const f = await twoKeys();
    vi.useFakeTimers();
    button(f, 'Done', '.modal').click();
    await settle(f);
    // While a key is on screen, its dialog is what stands over the page.
    expect(all(f, '.m-key-shield')).toEqual([]);
    await vi.advanceTimersByTimeAsync(DONE_PAUSE_MS);
    await settle(f);
    expect(all(f, '.m-key-shield')).toEqual([]);

    // The last key is taken down and the dialog goes. What stood behind its Done — a question
    // a tab still has open, a switch that saves on a click — is where the second click of a
    // double click would land. For as long as Done would have been held, this takes it.
    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(all(f, '.modal')).toEqual([]);
    const shield = el(f, '.m-key-shield');
    const drawn = getComputedStyle(shield);
    expect([drawn.position, drawn.zIndex]).toEqual(['fixed', '1000']);
    expect(shield.getAttribute('aria-hidden')).toBe('true');
    // Last in the section, as the key dialog was: over whatever dialog a tab has open.
    expect(shield.nextElementSibling).toBeNull();

    await vi.advanceTimersByTimeAsync(DONE_PAUSE_MS);
    await settle(f);
    expect(all(f, '.m-key-shield')).toEqual([]);
  });

  it('Copy copies the key it stands beside, not the one that has just taken its place', async () => {
    const f = await twoKeys();
    const written: string[] = [];
    const before = Object.getOwnPropertyDescriptor(navigator, 'clipboard');
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText: (text: string) => { written.push(text); return Promise.resolve(); } },
    });
    try {
      vi.useFakeTimers();
      // Done takes the first key down. Until the page is drawn again the Copy beside it is still
      // that key's, and a click that lands on it then copies the key it was drawn for — not
      // whichever the store holds first by now, which nobody has seen.
      button(f, 'Done', '.modal').click();
      el<HTMLButtonElement>(f, '.modal .m-key-reveal button[title="Copy"]').click();
      await settle(f);
      expect(written).toEqual(['the-new-key-shown-once']);
      expect(el(f, '.m-key-text').textContent).toBe('the-rotated-key');
    } finally {
      if (before) Object.defineProperty(navigator, 'clipboard', before);
      else delete (navigator as { clipboard?: unknown }).clipboard;
    }
  });

  it('a key that arrives while another tab is open is shown on that tab, which stays open', async () => {
    const f = await open('profiles');
    const post = await createSupport(f);
    // On to the Masking tab while the key is on its way; the tab opens on a preview.
    await show(f, 'Masking');
    http.expectOne('/api/mcp/preview').flush(PREVIEW);
    await settle(f);

    post.flush({ profile: SUPPORT, key: 'the-new-key-shown-once' });
    await lists(f, [PROFILES[0], SUPPORT], TOOLS);
    // The Profiles tab is hidden, and the key is not inside it.
    expect(el(f, '.m-key-text').textContent).toBe('the-new-key-shown-once');
    expect(el(f, '.m-key-text').closest('[hidden]')).toBeNull();
    expect(el(f, '.m-tab.active').textContent).toContain('Masking');

    button(f, 'Done', '.modal').click();
    await settle(f);
    expect(all(f, '.modal')).toEqual([]);
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

  /** Opens the Masking tab and answers the preview it opens with: an answer, or the server's own text of one. */
  async function openMasking(preview: McpPreview | string = PREVIEW): Promise<ComponentFixture<McpSectionComponent>> {
    const f = await open('masking');
    const first = http.expectOne('/api/mcp/preview');
    expect(first.request.body).toEqual({ mode: 'standard' });
    first.flush(preview);
    await settle(f);
    return f;
  }

  /** A line of one of the preview's two boxes (0: as stored, 1: as sent), found by the name it shows. */
  const previewEntry = (f: ComponentFixture<unknown>, box: 0 | 1, key: string): HTMLElement => {
    const line = [...all(f, '.kv')[box].querySelectorAll<HTMLElement>('.kv-line')]
      .find(l => l.querySelector('.kv-key')?.textContent === key);
    if (!line) throw new Error(`the preview has no line ${key}`);
    return line;
  };
  /** The value of a line, in runs: `.m-raw` marks what is hidden, `.m-tok` what stands in for it. */
  const previewLine = (f: ComponentFixture<unknown>, box: 0 | 1, key: string): HTMLElement =>
    previewEntry(f, box, key).querySelector<HTMLElement>('.kv-value')!;
  /** The name of a line, marked the same way where the name itself is hidden. */
  const previewKey = (f: ComponentFixture<unknown>, box: 0 | 1, key: string): HTMLElement =>
    previewEntry(f, box, key).querySelector<HTMLElement>('.kv-key')!;

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

  it('shows a 64-bit number with the digits it is stored with, hidden or not', async () => {
    // The server's own text: the stored event has the numbers as they are stored, which a
    // double — what JSON.parse makes of them — cannot hold.
    const exact = '9007199254740993';
    const f = await openMasking(
      '{"found":true,"stored":{"@t":"2026-11-16T09:14:31.0000000+00:00","@mt":"Order {OrderId} paid","@l":"Information",'
      + `"@service":"payments-api","props":{"OrderId":${exact},"MerchantRef":${exact},"Amount":12500}},`
      + '"sent":{"template":"Order {OrderId} paid","properties":['
      + `{"name":"OrderId","value":"${exact}","stored":"OrderId"},`
      + '{"name":"MerchantRef","value":"h:91c0e2aa5d17","stored":"MerchantRef"},'
      + '{"name":"Amount","value":"h:0123456789ab","stored":"Amount"}]},'
      + '"masked":{"total":2,"rules":{"MerchantRef":1,"Amount":1},"strict":{},"detectors":{}}}');

    // Sent as stored: nothing is marked, and both sides have the digits right.
    for (const box of [0, 1] as const) {
      expect(previewLine(f, box, 'OrderId').textContent).toBe(exact);
      expect(previewLine(f, box, 'OrderId').querySelector('.m-raw, .m-tok')).toBeNull();
    }
    // Hidden: what is shown as stored is what is stored — not the double nearest to it.
    expect(previewLine(f, 0, 'MerchantRef').querySelector('.m-raw')?.textContent).toBe(exact);
    expect(previewLine(f, 1, 'MerchantRef').querySelector('.m-tok')?.textContent).toBe('h:91c0e2aa5d17');
    expect(previewLine(f, 0, 'Amount').querySelector('.m-raw')?.textContent).toBe('12500');
    expect(previewLine(f, 1, 'Amount').querySelector('.m-tok')?.textContent).toBe('h:0123456789ab');
  });

  it('a property whose name is itself a value is shown sent under its token, not as removed', async () => {
    const f = await openMasking({
      ...PREVIEW,
      stored: { ...PREVIEW.stored!, props: { PaymentId: 'pay_1', 'a.serikov@example.com': 'active', Note: 'internal' } },
      sent: {
        ...PREVIEW.sent!,
        properties: [
          { name: 'PaymentId', value: 'pay_1', stored: 'PaymentId' },
          { name: 'h:3aa0715ce9d4', value: 'active', stored: 'a.serikov@example.com' },
        ],
      },
    });

    // As stored: the name is what is hidden, not the value.
    expect(previewKey(f, 0, 'a.serikov@example.com').querySelector('.m-raw')?.textContent).toBe('a.serikov@example.com');
    expect(previewLine(f, 0, 'a.serikov@example.com').textContent).toBe('active');
    expect(previewLine(f, 0, 'a.serikov@example.com').querySelector('.m-raw')).toBeNull();
    // As sent: under its token, with its value — the AI gets it.
    expect(previewKey(f, 1, 'h:3aa0715ce9d4').querySelector('.m-tok')?.textContent).toBe('h:3aa0715ce9d4');
    expect(previewLine(f, 1, 'h:3aa0715ce9d4').textContent).toBe('active');
    for (const box of [0, 1] as const) {
      expect(previewKey(f, box, box === 0 ? 'a.serikov@example.com' : 'h:3aa0715ce9d4').getAttribute('title'))
        .toBe('The name itself holds a value: the AI gets its token in place of it.');
    }
    expect(all(f, '.kv')[1].textContent).not.toContain('a.serikov@example.com');
    // A property the answer leaves out is still said to be removed; one sent as it is, is not marked.
    expect(previewLine(f, 1, 'Note').textContent).toBe('removed');
    expect(previewKey(f, 1, 'PaymentId').hasAttribute('title')).toBe(false);
  });

  it('a preview taken as a profile is taken again when that profile\'s masking is saved on the Profiles tab', async () => {
    const f = await openMasking();
    choose(el<HTMLSelectElement>(f, 'select[aria-label="Preview as"]'), 'p1');
    await settle(f);
    const asProfile = http.expectOne('/api/mcp/preview');
    expect(asProfile.request.body).toEqual({ profileId: 'p1' });
    asProfile.flush(PREVIEW);
    await settle(f);

    // The profile goes Strict on the Profiles tab; the Masking tab stays mounted meanwhile.
    await show(f, 'Profiles');
    button(f, 'Strict', 'app-mcp-profiles-tab [aria-label="Masking"]').click();
    await settle(f);
    button(f, 'Save', 'app-mcp-profiles-tab').click();
    await settle(f);
    const put = http.expectOne({ method: 'PUT', url: '/api/mcp/profiles/p1' });
    expect(put.request.body).toEqual({ maskingMode: 'strict' });
    const strict: McpProfile = { ...PROFILES[0], maskingMode: 'strict' };
    put.flush(strict);
    await settle(f);
    // The preview is of the profile as it is now.
    const again = http.expectOne('/api/mcp/preview');
    expect(again.request.body).toEqual({ profileId: 'p1' });
    again.flush(PREVIEW_STRICT);
    await lists(f, [strict], TOOLS);

    await show(f, 'Masking');
    expect(el<HTMLSelectElement>(f, 'select[aria-label="Preview as"]').value).toBe('p1');
    expect(button(f, 'Strict', '[aria-label="Masking mode"]').getAttribute('aria-pressed')).toBe('true');
    expect(previewLine(f, 1, 'PaymentId').textContent).toBe('h:0123456789ab');
  });

  it('a preview as a profile deleted meanwhile says that the profile is gone, and no longer offers it', async () => {
    const f = await openMasking();
    const previewAs = (): HTMLSelectElement => el(f, 'select[aria-label="Preview as"]');
    // Deleted somewhere else after this page read the list: the list here still offers it.
    choose(previewAs(), 'p1');
    await settle(f);
    const asProfile = http.expectOne('/api/mcp/preview');
    expect(asProfile.request.body).toEqual({ profileId: 'p1' });
    asProfile.flush('', { status: 404, statusText: 'Not Found' });
    await settle(f);

    expect(el(f, '#mcp-preview-error').textContent)
      .toBe('The profile “Payments team” is gone: it was deleted after this page read the list.');
    // Out of the list at once — nothing is read for it (the case would fail on a request) — and
    // the select is back on the mode the profile had.
    expect([...previewAs().options].map(o => o.value)).toEqual(['mode:standard', 'mode:strict']);
    expect(previewAs().value).toBe('mode:standard');
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

    // Typed into, the field no longer holds the filter that was refused: the panel still says
    // why the last preview failed, but the field is not marked for it.
    type(probe, "@l = 'Error' and @service = 'payments-api' and Amount > 0");
    await settle(f);
    expect(probe.hasAttribute('aria-invalid')).toBe(false);
    expect(probe.hasAttribute('aria-describedby')).toBe(false);
    expect(el(f, '#mcp-preview-error').textContent).toContain('Invalid filter');
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

    // Typed into, the field is no longer what was refused.
    type(field, 'Customer.Iin');
    await settle(f);
    expect(field.hasAttribute('aria-invalid')).toBe(false);
    expect(field.hasAttribute('aria-describedby')).toBe(false);
    expect(all(f, '#mcp-rule-error')).toEqual([]);
  });

  it('says of a keep that it vouches for its own field, not for what is under it', async () => {
    const f = await openMasking();
    button(f, 'Add rule').click();
    await settle(f);

    const action = el<HTMLSelectElement>(f, '.modal select.m-select');
    const says = (): string => action.closest('label')!.querySelector('.m-hint')!.textContent!.trim();
    expect(says()).toBe(
      'Replace the value with its token. The AI can still match and search by it. Where two rules match one field, the more protective wins.');

    // The server lets what is under a kept path through Strict mode and leaves the detectors on
    // it: a keep on `card` does not send `payment.card.number` as stored. The editor says so.
    choose(action, 'keep');
    await settle(f);
    expect(says()).toBe(
      'Send the value as stored; in Strict mode this is the allow-list. Properties under it are let through as well, ' +
      'and the detectors still check them. Where two rules match one field, the more protective wins.');
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

  it('"no" to the browser\'s Back steps forward to the editor again, and keeps the entry Back went to', async () => {
    // The browser's history, as the test bed has it: the Router's Location runs on a fake of the
    // Navigation API. The section reads where an entry stands from the browser's own
    // `navigation`, which jsdom has not got — so here that is the fake as well.
    const fake = TestBed.inject(PlatformNavigation);
    vi.stubGlobal('navigation', { get currentEntry() { return fake.currentEntry; } });
    const browser = TestBed.inject(Location);
    const go = vi.spyOn(browser, 'historyGo');
    // The Router follows Back and Forward, as it does once the app has started.
    router.setUpLocationChangeListener();
    /** The browser's own Back. */
    const back = async (f: ComponentFixture<unknown>): Promise<void> => {
      browser.back();
      for (let turn = 0; turn < 3; turn++) await settle(f);
    };

    // Two entries: the list of tools — marked, so that it is told from any other entry with the
    // same address — then the editor of one.
    await router.navigate([], { queryParams: { mcp: 'tools', entry: 'list' } });
    const f = await openEditor();
    const name = (): HTMLInputElement => el(f, 'app-mcp-tool-editor input[placeholder="payment_declines"]');
    type(name(), 'payment_declines_v2');
    await settle(f);

    const confirm = vi.fn(() => false);
    vi.stubGlobal('confirm', confirm);
    await back(f);
    // "No": a step forward again, to the editor's own entry — not its address written over the list's.
    expect(confirm).toHaveBeenCalledTimes(1);
    expect(go).toHaveBeenCalledWith(1);
    expect(router.url).toBe('/?mcp=tools&tool=t1');
    expect(name().value).toBe('payment_declines_v2');

    // The list's entry is still there: Back reaches it again, and "yes" goes to it.
    confirm.mockReturnValue(true);
    await back(f);
    expect(confirm).toHaveBeenCalledTimes(2);
    expect(router.url).toBe('/?mcp=tools&entry=list');
    expect(all(f, 'app-mcp-tool-editor')).toEqual([]);
    expect(dirty.dirtyTabsList()).toEqual([]);
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
    // A field the sentence is not about is left alone — and changing it leaves the sentence be.
    expect(el(f, '#mcp-tool-filter').hasAttribute('aria-invalid')).toBe(false);
    type(el<HTMLTextAreaElement>(f, '#mcp-tool-filter'), 'MerchantId = {merchantId} and @l = \'Error\'');
    await settle(f);
    expect(name.getAttribute('aria-invalid')).toBe('true');

    // The name typed again is no longer the name that was refused.
    type(name, 'payment_declines_v2');
    await settle(f);
    expect(name.hasAttribute('aria-invalid')).toBe(false);
    expect(name.hasAttribute('aria-describedby')).toBe(false);
    expect(all(f, '#mcp-tool-error')).toEqual([]);
  });

  it('a window typed another way is not an unsaved change of the tool', async () => {
    const f = await openEditor();
    const window = el<HTMLInputElement>(f, 'app-mcp-tool-editor input[placeholder="1h"]');
    expect(window.value).toBe('1 h');
    type(window, '60m');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual([]);
    expect(button(f, 'Discard changes', 'app-mcp-tool-editor').disabled).toBe(true);
    type(window, '2h');
    await settle(f);
    expect(dirty.dirtyTabsList()).toEqual(['ai-mcp']);
  });

  /** A tool with an argument of each type the test run sends differently. */
  const TYPED: McpPublishedTool = {
    ...TOOLS[0],
    parameters: [
      { name: 'merchantId', type: 'string', required: true, description: '', values: [], default: null },
      { name: 'orderId', type: 'number', required: false, description: '', values: [], default: null },
      { name: 'urgent', type: 'boolean', required: false, description: '', values: [], default: null },
    ],
    filter: 'MerchantId = {merchantId} and OrderId = {orderId} and Urgent = {urgent}',
    inputSchema: {
      properties: { merchantId: {}, orderId: {}, urgent: {}, from: {}, to: {}, cursor: {} }, required: ['merchantId'],
    },
  };

  it('runs the tool as a profile would call it, a 64-bit number argument as it was typed — and saves nothing', async () => {
    const f = await openEditor(TYPED);
    const argument = <T extends HTMLElement>(name: string): T => {
      const label = all(f, 'app-mcp-tool-editor .args label').find(l => l.querySelector('.m-lbl')?.textContent?.trim() === name);
      if (!label) throw new Error(`the test run has no argument ${name}`);
      return label.querySelector<T>('input, select')!;
    };
    type(argument<HTMLInputElement>('merchantId'), 'm-42');
    type(argument<HTMLInputElement>('orderId'), '9007199254740993');
    choose(argument<HTMLSelectElement>('urgent'), 'true');
    type(argument<HTMLInputElement>('from'), '-2h');
    await settle(f);
    // Arguments are not the tool: nothing about it is unsaved.
    expect(dirty.dirtyTabsList()).toEqual([]);

    button(f, 'Run', 'app-mcp-tool-editor').click();
    await settle(f);
    const run = http.expectOne({ method: 'POST', url: '/api/mcp/tools/test' });
    expect(run.request.body).toMatchObject({ id: 't1', profileId: 'p1', tool: { name: 'payment_declines', filter: TYPED.filter } });
    // The id as typed: as a number it would be 9007199254740992, another order's.
    expect(run.request.body.arguments).toEqual({ merchantId: 'm-42', orderId: '9007199254740993', urgent: true, from: '-2h' });
    run.flush({
      text: 'PaymentId=pay_1 CardNumber=h:91c0e2aa5d17', isError: false, status: 'OK',
      filter: "MerchantId = 'm-42' and OrderId = 9007199254740993 and Urgent = true",
      events: 1, masked: 1, maskedBy: { rules: { CardNumber: 1 } }, tokens: 60, elapsedMs: 12,
      definition: { name: 'payment_declines', description: TYPED.description, inputSchema: TYPED.inputSchema },
    });
    await settle(f);

    expect(text(f)).toContain('1 event · 1 value masked · about 60 tokens · 12 ms');
    const [bound, answer, definition] = all(f, 'app-mcp-tool-editor .m-side pre.m-code');
    expect(bound.textContent).toBe("MerchantId = 'm-42' and OrderId = 9007199254740993 and Urgent = true");
    expect(answer.querySelector('.m-tok')?.textContent).toBe('h:91c0e2aa5d17');
    expect(JSON.parse(definition.textContent ?? '')).toMatchObject({ name: 'payment_declines', inputSchema: { required: ['merchantId'] } });
    expect(dirty.dirtyTabsList()).toEqual([]);
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
    // Records, not calls: past a profile's limit one record can stand for many refused calls.
    expect(text(f)).toContain('Showing 1 of 1 284 records');
  });

  it('shows a call\'s 64-bit argument with the digits the filter under it has', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({
      more: false,
      calls: [call('42', 'OK', {
        tool: 'payment_declines', arguments: '{"merchantId":"m-42","orderId":9007199254740993}',
        filter: "MerchantId = 'm-42' and OrderId = 9007199254740993",
      })],
    });
    await settle(f);

    const [args, filter] = all(f, 'app-mcp-activity-tab .m-side pre.m-code');
    expect(args.textContent).toBe('{\n  "merchantId": "m-42",\n  "orderId": 9007199254740993\n}');
    expect(filter.textContent).toBe("MerchantId = 'm-42' and OrderId = 9007199254740993");
  });

  it('says how many refusals a record stands for when a profile ran over its limit', async () => {
    const f = await open('activity');
    const refused = { refusalKind: 'limit', refusal: 'The profile has made its 30 calls of this minute.' };
    http.expectOne(isActivity).flush({
      more: false,
      calls: [
        call('45', 'Refused', { ...refused, repeats: 8 }),
        call('44', 'Refused', { ...refused, repeats: 1 }),
        call('43', 'OK', { repeats: 0 }),
        // What the server sends for a call that was no such refusal.
        call('42', 'OK', { repeats: null }),
      ],
    });
    await settle(f);
    const note = (): HTMLElement[] => all(f, 'app-mcp-activity-tab .m-side > section > .m-hint');

    const rows = all(f, '[role=row].pick');
    expect(rows[0].textContent).toContain('×8');
    for (const row of rows.slice(1)) expect(row.textContent).not.toContain('×');
    expect(note().map(n => n.textContent)).toContain('Refused 8 times this minute so far — only the 1st, 2nd, 4th, 8th… are recorded.');

    // The first refusal of a minute is one refusal — and the start of the thinning, which is said.
    rows[1].click();
    await settle(f);
    expect(note().map(n => n.textContent)).toContain('The first call refused this minute — after it, only the 2nd, 4th, 8th… are recorded.');
    expect(text(f)).not.toContain('this minute so far');

    // Any other call has nothing of the kind to say.
    for (const row of [rows[2], rows[3]]) {
      row.click();
      await settle(f);
      expect(text(f)).not.toContain('this minute');
    }
  });

  it('adds the next page of calls under the list', async () => {
    const f = await open('activity');
    http.expectOne(isActivity).flush({ more: true, calls: [call('42', 'OK')] });
    await settle(f);
    expect(text(f)).toContain('Showing 1 of 1 284 records');

    button(f, 'Load more').click();
    await settle(f);
    const more = http.expectOne(isActivity);
    // After the last call on screen, under the same filters.
    expect(more.request.params.get('afterTs')).toBe('639000000000000000');
    expect(more.request.params.get('afterId')).toBe('42');
    expect(more.request.params.get('count')).toBe('50');
    more.flush({ more: false, calls: [call('41', 'Trimmed'), call('40', 'OK')] });
    await settle(f);

    expect(all(f, '[role=row].pick').map(r => r.querySelector('.m-pill')?.textContent)).toEqual(['OK', 'Trimmed', 'OK']);
    // The picked call stays picked, and there is nothing more to load.
    expect(all(f, '[role=row].pick').map(r => r.getAttribute('aria-selected'))).toEqual(['true', 'false', 'false']);
    expect(all(f, 'app-mcp-activity-tab button').map(b => b.textContent?.trim())).not.toContain('Load more');
    expect(text(f)).toContain('Showing 3 of 1 284 records');
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
