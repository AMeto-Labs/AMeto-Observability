import {
  Component, signal, computed, inject, effect, viewChild, ElementRef,
  OnInit, OnDestroy, HostListener,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Subscription } from 'rxjs';
import { LucideAngularModule } from 'lucide-angular';
import { subDays, format, startOfDay } from 'date-fns';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { ActivatedRoute, Router } from '@angular/router';

import { ApiService } from '../../core/services/api.service';
import { EventDto, LEVELS } from '../../core/models/event.model';
import { EventRowComponent } from './event-row/event-row';
import { SignalsPanelComponent } from './signals-panel/signals-panel';
import { DateMaskDirective } from '../../shared/directives/date-mask.directive';
import { highlightFilterExpression } from '../../shared/utils/filter-highlight';
import { injectVirtualizer } from '@tanstack/angular-virtual';
import { VmeasureDirective } from '../../shared/directives/virtual-measure.directive';
import { EmptyStateComponent } from '../../shared/components/ui';

type TimePreset = '5m' | '15m' | '30m' | '1d' | '7d' | '2w' | '1mo' | 'custom';

/** Built-in tokens always offered by the filter autocomplete popup. */
const BUILTIN_SUGGESTIONS = [
  '@l', '@mt', '@t', '@x', '@x.Type', '@x.Message', '@x.StackTrace',
  '@i', '@r', '@sp', '@tr',
  'and', 'or', 'not', 'in', 'like',
  'true', 'false', 'null',
  'has(', 'isDefined(', 'startsWith(', 'contains(', 'endsWith(',
  'ci_startsWith(', 'ci_contains(',
  'length(', 'coalesce(', 'fromJson(', 'toJson(',
  'toLower(', 'toUpper(', 'toNumber(',
  'substring(', 'indexOf(', 'lastIndexOf(', 'replace(', 'concat(',
  'ci_endsWith(', 'typeOf(', 'elementAt(', 'keys(', 'values(',
  'round(', 'now(', 'dateTime(', 'toIsoString(',
  'datePart(', 'timeOfDay(', 'timeSpan(', 'totalMilliseconds(',
  'toTimeString(', 'toHexString(', 'bucket(', 'offsetIn(', 'arrived(',
  'fromXml(', 'fromBase64(', 'toBase64(', 'regexMatch(', 'regexExtract(',
];

/** Safe identifier as accepted by the server-side lexer (letter/digit/_/@). */
const IDENT_RE = /^[A-Za-z_@][A-Za-z0-9_@]*$/;

/** Matches the trailing token under the caret that the popup is completing. */
const PREFIX_RE = /[@A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/;

/**
 * Walks a property bag and yields every reachable path in filter-language
 * form (<c>Foo.Bar</c>, <c>Foo['weird-key']</c>, <c>Tags[0]</c>). Arrays are
 * traversed but only their first element contributes path suggestions —
 * the goal is to expose property <em>shape</em>, not enumerate values.
 */
function collectPropPaths(
  obj: unknown,
  prefix: string,
  out: Set<string>,
  depth: number,
): void {
  if (depth > 4 || obj === null || obj === undefined) return;
  if (Array.isArray(obj)) {
    if (prefix) out.add(prefix);
    if (obj.length > 0) collectPropPaths(obj[0], `${prefix}[0]`, out, depth + 1);
    return;
  }
  if (typeof obj !== 'object') return;
  for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
    const seg = IDENT_RE.test(k)
      ? (prefix ? `${prefix}.${k}` : k)
      : `${prefix}['${k.replace(/'/g, "\\'")}']`;
    out.add(seg);
    collectPropPaths(v, seg, out, depth + 1);
  }
}

/** Milliseconds between .NET DateTime min (0001-01-01 UTC) and Unix epoch (1970-01-01 UTC). */
const DOTNET_TICKS_UNIX_EPOCH_MS = 62_135_596_800_000;

/**
 * Removes all matches of `pattern` from a filter expression string, then
 * cleans up dangling `and`/`or` connectives and extra whitespace.
 * Used by applyLevels / applyServices to splice out a previous clause before
 * inserting a new one, without erasing the user's own expression.
 */
function stripFilterClause(expr: string, pattern: RegExp): string {
  let s = expr.replace(pattern, '');
  // Remove dangling connectives like "and and", leading/trailing "and"/"or"
  s = s.replace(/\s+and\s+and\s+/gi, ' and ')
       .replace(/^\s*(and|or)\s+/gi, '')
       .replace(/\s+(and|or)\s*$/gi, '')
       .trim();
  return s;
}

/**
 * Extracts the active log levels from a filter expression string.
 * Recognises: @l = 'Error'  |  @l in ['Error', 'Fatal']
 * Returns full set (all levels) when no @l clause is present.
 */
function parseLevelsFromFilter(expr: string): Set<string> {
  // @l in ['A','B',...]
  const inMatch = expr.match(/@l\s+in\s*\[([^\]]+)\]/i);
  if (inMatch) {
    const levels = new Set<string>();
    for (const m of inMatch[1].matchAll(/'([^']+)'/g)) levels.add(m[1]);
    return levels.size > 0 ? levels : new Set(LEVELS as unknown as string[]);
  }
  // @l = 'X'
  const eqMatch = expr.match(/@l\s*=\s*'([^']+)'/i);
  if (eqMatch) return new Set([eqMatch[1]]);
  return new Set(LEVELS as unknown as string[]);
}

/**
 * Extracts selected service names from a filter expression string.
 * Recognises: ['service.name'] = 'X'  |  ['service.name'] in ['A','B']
 */
function parseServicesFromFilter(expr: string): Set<string> {
  // ['service.name'] in ['A','B']
  const inMatch = expr.match(/\['service\.name'\]\s+in\s*\[([^\]]+)\]/i);
  if (inMatch) {
    const svcs = new Set<string>();
    for (const m of inMatch[1].matchAll(/'([^']+)'/g)) svcs.add(m[1]);
    return svcs;
  }
  // ['service.name'] = 'X'
  const eqMatch = expr.match(/\['service\.name'\]\s*=\s*'([^']+)'/i);
  if (eqMatch) return new Set([eqMatch[1]]);
  return new Set<string>();
}

/** Converts JS milliseconds-since-Unix-epoch into .NET UTC ticks (100ns since 0001-01-01). */
function msToDotNetUtcTicks(ms: number): number {
  return (ms + DOTNET_TICKS_UNIX_EPOCH_MS) * 10_000;
}

@Component({
  selector: 'app-events',
  imports: [FormsModule, LucideAngularModule, EventRowComponent, SignalsPanelComponent, EmptyStateComponent, DateMaskDirective, VmeasureDirective],
  templateUrl: './events.html',
  styleUrl: './events.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class EventsComponent implements OnInit, OnDestroy {
  private api       = inject(ApiService);
  private cdr       = inject(ChangeDetectorRef);
  private sanitizer = inject(DomSanitizer);
  private route     = inject(ActivatedRoute);
  private router    = inject(Router);

  // ── State ──────────────────────────────────────────────────────────────
  filterInput   = signal('');
  filter        = signal('');
  timePreset    = signal<TimePreset>('1d');
  customFrom           = signal('');
  customTo             = signal('');
  customFromSuggestion = signal('');
  customToSuggestion   = signal('');
  events        = signal<EventDto[]>([]);
  loading       = signal(false);
  loadingMore   = signal(false);
  hasMore       = signal(true);
  error         = signal<string | null>(null);
  signalsPanelOpen    = signal(false);
  // activeLevels and selectedServices are derived from filterInput (single source of truth).
  // Dropdowns read/write only the filter string — no separate state to desync.
  activeLevels  = computed(() => parseLevelsFromFilter(this.filterInput()));
  selectedServices = computed(() => parseServicesFromFilter(this.filterInput()));
  liveActive          = signal(false);
  autoScroll          = signal(true);
  wrapMessages        = signal(false);
  expandedEventIds    = signal<ReadonlySet<string>>(new Set());
  levelsDropdownOpen  = signal(false);
  serviceDropdownOpen = signal(false);
  dateDropdownOpen    = signal(false);
  // Pending state for dropdowns (edits before Apply)
  pendingServices     = signal<Set<string>>(new Set());
  serviceSearch       = signal('');
  showMoreServices    = signal(false);
  // Services loaded from backend (independent of loaded events)
  backendServices     = signal<string[]>([]);
  // Pending levels for dropdown
  pendingLevels       = signal<Set<string>>(new Set(LEVELS as unknown as string[]));
  // Calendar for date picker
  calendarNav         = signal<{ year: number; month: number }>({
    year: new Date().getFullYear(), month: new Date().getMonth(),
  });
  quickSearch         = signal('');

  // ── Filter autocomplete state ─────────────────────────────────────────
  /** Whether the suggestion popup is visible below the filter textarea. */
  suggestOpen     = signal(false);
  /** Currently-highlighted entry inside the popup (0-based). */
  suggestIndex    = signal(0);
  /** Word fragment under the caret that the popup is filtering against. */
  private suggestPrefix = signal('');
  /** Caret position at the moment the popup was opened. */
  private suggestAnchor = 0;

  /** Property paths discovered in the currently loaded events (sorted). */
  knownPropPaths = computed<string[]>(() => {
    const set = new Set<string>();
    for (const ev of this.events()) collectPropPaths(ev.props ?? {}, '', set, 0);
    return [...set].sort();
  });

  /** Items shown in the suggestion popup, filtered by the current prefix. */
  suggestItems = computed<string[]>(() => {
    if (!this.suggestOpen()) return [];
    const all = [...BUILTIN_SUGGESTIONS, ...this.knownPropPaths()];
    const q   = this.suggestPrefix().toLowerCase();
    if (!q) return all.slice(0, 40);
    return all.filter(s => s.toLowerCase().includes(q)).slice(0, 40);
  });

  readonly levelItems = [
    { key: 'Verbose',     short: 'VRB' },
    { key: 'Debug',       short: 'DBG' },
    { key: 'Information', short: 'INF' },
    { key: 'Warning',     short: 'WRN' },
    { key: 'Error',       short: 'ERR' },
    { key: 'Fatal',       short: 'FTL' },
  ];

  readonly datePresets: { label: string; value: TimePreset }[] = [
    { label: 'Last 5 min',   value: '5m'     },
    { label: 'Last 15 min',  value: '15m'    },
    { label: 'Last 30 min',  value: '30m'    },
    { label: 'Last 1 day',   value: '1d'     },
    { label: 'Last 7 days',  value: '7d'     },
    { label: 'Last 2 weeks', value: '2w'     },
    { label: 'Last 1 month', value: '1mo'    },
    { label: 'Custom range', value: 'custom' },
  ];

  readonly calendarWeekDays = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];
  readonly maskPattern = 'yyyy-mm-dd HH:mm';

  /** Allowed page sizes shown in the dropdown next to the event counter. */
  readonly pageSizeOptions = [50, 100, 150, 300, 500];
  /** Active page size — applied to every events query (initial load + paging). */
  pageSize = signal<number>(50);

  maskRemaining(value: string): string {
    return this.maskPattern.slice(value.length);
  }

  onExpandRequested(eventId: string): void {
    if (!eventId) return;
    this.expandedEventIds.update(prev => {
      const next = new Set(prev);
      if (next.has(eventId)) next.delete(eventId);
      else next.add(eventId);
      return next;
    });
  }

  hasExpandedRows = computed(() => this.expandedEventIds().size > 0);

  // ── Computed ───────────────────────────────────────────────────────────
  /** Events list — optionally filtered client-side by service and quick-search. */
  displayedEvents = computed(() => {
    let evs = this.events();
    const svcs = this.selectedServices();
    if (svcs.size > 0) evs = evs.filter(e => svcs.has((e['service.name'] as string) ?? ''));
    const q = this.quickSearch().trim().toLowerCase();
    if (q) evs = evs.filter(e => (e['@mt'] ?? '').toLowerCase().includes(q));
    return evs;
  });

  availableServices = computed<string[]>(() => {
    // Merge backend-known services with any additional ones seen in current events
    const svcs = new Set<string>(this.backendServices());
    for (const ev of this.events()) {
      const svc = ev['service.name'] as string | undefined;
      if (svc) svcs.add(svc);
    }
    return [...svcs].sort();
  });

  levelCounts = computed(() => {
    const counts: Record<string, number> = {};
    for (const ev of this.events()) {
      const lvl = (ev['@l'] ?? 'information').toLowerCase();
      counts[lvl] = (counts[lvl] ?? 0) + 1;
    }
    return counts;
  });

  totalCount      = computed(() => this.events().length);
  allLevelsActive = computed(() => this.activeLevels().size === LEVELS.length);

  levelsLabel = computed(() => {
    const active = this.activeLevels();
    if (active.size === LEVELS.length) return 'All levels';
    if (active.size === 1) return [...active][0];
    return `${active.size} levels`;
  });

  serviceLabel   = computed(() => {
    const s = this.selectedServices();
    if (s.size === 0) return 'All services';
    if (s.size === 1) return [...s][0];
    return `${s.size} services`;
  });
  dateRangeLabel = computed(() => `${this.customFrom() || '\u2026'} \u2013 ${this.customTo() || 'now'}`);

  calendarMonthLabel = computed(() => {
    const { year, month } = this.calendarNav();
    return format(new Date(year, month, 1), 'MMMM yyyy');
  });

  calendarDays = computed(() => {
    const { year, month } = this.calendarNav();
    const firstDow    = new Date(year, month, 1).getDay();
    const startOffset = (firstDow + 6) % 7; // Monday-first
    const today   = format(new Date(), 'yyyy-MM-dd');
    const fromDate = this.parseCustomDate(this.customFrom());
    const toDate   = this.parseCustomDate(this.customTo());
    const fromStr  = fromDate ? format(fromDate, 'yyyy-MM-dd') : null;
    const toStr    = toDate   ? format(toDate,   'yyyy-MM-dd') : null;
    const days: Array<{ day: number; month: number; year: number; isCurrentMonth: boolean; isToday: boolean; isFrom: boolean; isTo: boolean; inRange: boolean }> = [];
    for (let i = 0; i < 42; i++) {
      const d  = new Date(year, month, 1 - startOffset + i);
      const ds = format(d, 'yyyy-MM-dd');
      days.push({
        day:            d.getDate(),
        month:          d.getMonth(),
        year:           d.getFullYear(),
        isCurrentMonth: d.getMonth() === month,
        isToday:        ds === today,
        isFrom:         ds === fromStr,
        isTo:           ds === toStr,
        inRange:        !!(fromStr && toStr && ds > fromStr && ds < toStr),
      });
    }
    return days;
  });

  serviceCounts = computed(() => {
    const counts: Record<string, number> = {};
    for (const ev of this.events()) {
      const svc = ev['service.name'] as string | undefined;
      if (svc) counts[svc] = (counts[svc] ?? 0) + 1;
    }
    return counts;
  });

  filteredServices = computed(() => {
    const q = this.serviceSearch().toLowerCase();
    return q ? this.availableServices().filter(s => s.toLowerCase().includes(q)) : this.availableServices();
  });

  allPendingLevelsActive   = computed(() => this.pendingLevels().size === LEVELS.length);
  allPendingServicesActive = computed(() => this.pendingServices().size === 0);

  /** Syntax-highlighted HTML for the filter overlay div. */
  filterHighlight = computed<SafeHtml>(() =>
    this.sanitizer.bypassSecurityTrustHtml(highlightFilterExpression(this.filterInput()))
  );

  /** Number of visual rows the textarea should show (auto-grows with newlines). */
  filterRows = computed(() => Math.max(1, this.filterInput().split('\n').length));

  // ── View refs / sync ──────────────────────────────────────────────────
  private filterRef    = viewChild<ElementRef<HTMLTextAreaElement>>('filterEl');
  private eventsScroll = viewChild<ElementRef<HTMLElement>>('eventsScroll');
  private dateFbGroup  = viewChild<ElementRef<HTMLElement>>('dateFbGroup');
  private levelsFbGroup = viewChild<ElementRef<HTMLElement>>('levelsFbGroup');
  private svcFbGroup    = viewChild<ElementRef<HTMLElement>>('svcFbGroup');

  /** Position (fixed) for the currently-open dropdown. */
  ddPos = signal<{ top: number; left: number }>({ top: 0, left: 0 });

  // ── TanStack Virtual ──────────────────────────────────────────────────
  readonly virtualizer = injectVirtualizer(() => ({
    count: this.displayedEvents().length,
    scrollElement: this.eventsScroll(),
    estimateSize: () => 29,
    overscan: 20,
    getItemKey: (i: number) => this.displayedEvents()[i]?.id ?? i,
  }));

  /** Stable ref to measureElement — avoids creating a new closure every CD cycle. */
  readonly measureRow = (el: Element) => this.virtualizer.measureElement(el);

  private computeDdPos(el: HTMLElement | undefined): void {
    if (!el) return;
    const r = el.getBoundingClientRect();
    const left = Math.min(r.left, window.innerWidth - (el.closest('.date-dd') ? 544 : 300));
    this.ddPos.set({ top: Math.round(r.bottom + 4), left: Math.max(0, Math.round(left)) });
  }

  // ── RxJS plumbing ─────────────────────────────────────────────────────
  private subs: Subscription[] = [];
  private querySub?: Subscription;
  private liveSub?: Subscription;

  constructor() {
    // One-way sync from signal → DOM. Only write when the DOM value actually
    // differs, otherwise we'd reset the caret position on every keystroke.
    effect(() => {
      const v  = this.filterInput();
      const el = this.filterRef()?.nativeElement;
      if (el && el.value !== v) el.value = v;
    });
  }

  ngOnInit(): void {
    // Restore state from URL query params (initial navigation).
    const qp = this.route.snapshot.queryParamMap;
    const urlFilter = qp.get('filter') ?? '';
    const urlPreset = (qp.get('preset') as TimePreset) || '1d';
    const urlFrom   = qp.get('from') ?? '';
    const urlTo     = qp.get('to')   ?? '';
    const urlLevels = qp.get('levels');
    const urlSize   = Number(qp.get('size'));
    if (this.pageSizeOptions.includes(urlSize)) this.pageSize.set(urlSize);

    this.filterInput.set(urlFilter);
    this.filter.set(urlFilter);
    this.timePreset.set(urlPreset);
    if (urlFrom) {
      this.customFrom.set(urlFrom);
      this.customTo.set(urlTo);
    } else {
      this.applyPresetDates(urlPreset);
    }
    if (urlLevels) {
      // Legacy: old URLs had a separate `levels=` param. Only apply it if the
      // filter string does NOT already contain an @l clause (avoids duplicate on F5).
      const alreadyInFilter = /@l\s*(=|in)/i.test(urlFilter);
      if (!alreadyInFilter) {
        const lvlSet = new Set(urlLevels.split(',').filter(Boolean));
        if (lvlSet.size < LEVELS.length) {
          const clause = lvlSet.size === 1
            ? `@l = '${[...lvlSet][0]}'`
            : `@l in [${[...lvlSet].map(l => `'${l}'`).join(', ')}]`;
          const base = urlFilter.trim();
          const merged = base ? `${clause} and ${base}` : clause;
          this.filterInput.set(merged);
          this.filter.set(merged);
        }
      }
    }

    this.loadEvents();
  }

  ngOnDestroy(): void {
    this.subs.forEach(s => s.unsubscribe());
    this.querySub?.unsubscribe();
    this.liveSub?.unsubscribe();
  }

  // ── Helpers ─────────────────────────────────────────────────────────────
  private levelsParam(): string | undefined {
    const lvls = this.activeLevels();
    return lvls.size === LEVELS.length ? undefined : [...lvls].join(',');
  }

  private syncRoute(): void {
    // levels is no longer stored as a separate URL param — the @l clause in
    // `filter` is the single source of truth. We keep null here so old `levels=`
    // params are removed from the URL on the first navigation after upgrade.
    const qp: Record<string, string | null> = {
      filter: this.filter() || null,
      preset:  this.timePreset(),
      from:    this.customFrom() || null,
      to:      this.customTo()   || null,
      levels:  null,
      size:    this.pageSize() === 50 ? null : String(this.pageSize()),
    };
    this.router.navigate([], { queryParams: qp, replaceUrl: true });
  }

  private fmtDateInput(d: Date): string {
    return format(d, 'yyyy-MM-dd HH:mm');
  }

  private applyPresetDates(preset: TimePreset): void {
    if (preset === 'custom') return;
    const now = new Date();
    const msMap: Partial<Record<TimePreset, number>> = {
      '5m': 5 * 60_000, '15m': 15 * 60_000, '30m': 30 * 60_000,
    };
    const daysMap: Partial<Record<TimePreset, number>> = { '1d': 1, '7d': 7, '2w': 14, '1mo': 30 };
    if (msMap[preset] !== undefined) {
      this.customFrom.set(this.fmtDateInput(new Date(now.getTime() - msMap[preset]!)));
    } else if (daysMap[preset] !== undefined) {
      this.customFrom.set(this.fmtDateInput(startOfDay(subDays(now, daysMap[preset]!))));
    }
    this.customTo.set('');
    this.customFromSuggestion.set('');
    this.customToSuggestion.set('');
  }

  /** Parses yyyy-MM-dd or yyyy-MM-dd HH:mm (also accepts legacy dd/MM/yyyy) */
  private parseCustomDate(val: string): Date | null {
    if (!val) return null;
    // yyyy-MM-dd [HH:mm]
    const iso = val.match(/^(\d{4})-(\d{2})-(\d{2})(?:[\s,T]+(\d{1,2}):(\d{2}))?$/);
    if (iso) {
      const [, y, mo, d, h = '0', min = '0'] = iso;
      const dt = new Date(+y, +mo - 1, +d, +h, +min);
      return isNaN(dt.getTime()) ? null : dt;
    }
    // legacy dd/MM/yyyy [HH:mm]
    const leg = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[\s,T]+(\d{1,2}):(\d{2}))?$/);
    if (leg) {
      const [, d, mo, y, h = '0', min = '0'] = leg;
      const dt = new Date(+y, +mo - 1, +d, +h, +min);
      return isNaN(dt.getTime()) ? null : dt;
    }
    return null;
  }

  readonly customFromValid = computed(() => !!this.parseCustomDate(this.customFrom()));
  /** "To" is optional — empty is valid; non-empty must parse. */
  readonly customToValid   = computed(() => {
    const v = this.customTo();
    return !v || !!this.parseCustomDate(v);
  });
  readonly canSearch       = computed(() => this.customFromValid() && this.customToValid());

  setFrom(v: string): void {
    this.customFrom.set(v);
    this.timePreset.set('custom');
    // suggestion is managed by (suggestionChange) binding — don't clear it here
  }

  setTo(v: string): void {
    this.customTo.set(v);
    this.timePreset.set('custom');
    // suggestion is managed by (suggestionChange) binding — don't clear it here
  }

  acceptFromSuggestion(): void {
    const s = this.customFromSuggestion();
    if (s) {
      this.customFrom.update(v => v + s);
      this.customFromSuggestion.set('');
      this.timePreset.set('custom');
    }
  }

  acceptToSuggestion(): void {
    const s = this.customToSuggestion();
    if (s) {
      this.customTo.update(v => v + s);
      this.customToSuggestion.set('');
      this.timePreset.set('custom');
    }
  }

  /** Strips leading `-` or space from suggestion for display (e.g. `-26` → `26`, ` 00:00` → `00:00`). */
  suggestionLabel(s: string): string {
    return s.replace(/^[-\s]+/, '');
  }

  private getFromDate(): Date {
    const d = this.parseCustomDate(this.customFrom());
    return d ?? subDays(new Date(), 1);
  }

  private getToDate(): string | undefined {
    const d = this.parseCustomDate(this.customTo());
    return d ? d.toISOString() : undefined;
  }

  // ── Actions ────────────────────────────────────────────────────────────
  loadEvents(): void {
    if (this.liveActive()) return;
    this.querySub?.unsubscribe();
    this.loading.set(true);
    this.error.set(null);
    this.events.set([]);
    this.hasMore.set(true);
    this.expandedEventIds.set(new Set());

    const acc: EventDto[] = [];
    const size = this.pageSize();
    this.querySub = this.api.streamEvents({
      filter: this.filter() || undefined,
      from: this.getFromDate().toISOString(),
      to: this.getToDate(),
      count: size,
      dir: 'backward',
      levels: this.levelsParam(),
    }).subscribe({
      next: ev => {
        acc.push(ev);
        if (acc.length % 10 === 0) {
          this.events.set([...acc]);
          this.cdr.markForCheck();
        }
      },
      complete: () => {
        this.events.set([...acc]);
        this.loading.set(false);
        this.hasMore.set(acc.length >= size);
        this.syncRoute();
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set((err as Error).message ?? 'Failed to load events');
        this.loading.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  /**
   * Loads the next page of older events using the (@t, id) cursor of the
   * oldest event currently displayed. Triggered when the user scrolls near
   * the bottom of the events list.
   */
  loadMore(): void {
    if (this.liveActive() || this.loading() || this.loadingMore() || !this.hasMore()) return;

    const list = this.events();
    const last = list[list.length - 1];
    if (!last) return;

    const lastTsMs = new Date(last['@t']).getTime();
    const afterTsTicks = msToDotNetUtcTicks(lastTsMs);

    this.loadingMore.set(true);
    this.error.set(null);

    const acc: EventDto[] = [];
    const size = this.pageSize();
    this.querySub?.unsubscribe();
    this.querySub = this.api.streamEvents({
      filter:  this.filter() || undefined,
      from:    this.getFromDate().toISOString(),
      to:      this.getToDate(),
      count:   size,
      dir:     'backward',
      afterId: last.id,
      afterTs: afterTsTicks,
      levels:  this.levelsParam(),
    }).subscribe({
      next: ev => acc.push(ev),
      complete: () => {
        if (acc.length > 0) {
          this.events.update(evs => {
            // Deduplicate: server cursor overlap can occasionally return the same
            // event twice; duplicate IDs would cause multiple rows to appear expanded.
            const seen = new Set(evs.map(e => e.id));
            const fresh = acc.filter(e => !seen.has(e.id));
            return fresh.length > 0 ? [...evs, ...fresh] : evs;
          });
        }
        this.loadingMore.set(false);
        this.hasMore.set(acc.length >= size);
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set((err as Error).message ?? 'Failed to load more events');
        this.loadingMore.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  onEventsScroll(e: Event): void {
    const el = e.target as HTMLElement;
    if (el.scrollHeight - el.scrollTop - el.clientHeight < 400) this.loadMore();
  }

  scrollToOlderLogs(): void {
    // Events are newest-first, so the oldest loaded event is at the bottom.
    const last = this.displayedEvents().length - 1;
    if (last >= 0) this.virtualizer.scrollToIndex(last, { behavior: 'smooth' });
  }

  scrollToNewerLogs(): void {
    // Newest events sit at the top of the list.
    this.virtualizer.scrollToOffset(0, { behavior: 'smooth' });
  }

  /** TrackBy for *cdkVirtualFor over event rows. */
  trackEvent = (_: number, ev: EventDto): string | number =>
    ev.id ?? ev['@t'] ?? _;

  /** Called by the page-size <select>. Updates the signal and reloads. */
  onPageSizeChange(raw: string | number): void {
    const v = typeof raw === 'string' ? Number(raw) : raw;
    if (!this.pageSizeOptions.includes(v) || v === this.pageSize()) return;
    this.pageSize.set(v);
    this.loadEvents();
  }

  search(): void {
    if (!this.canSearch()) return;
    this.filter.set(this.filterInput());
    this.loadEvents();
  }

  /**
   * Filter textarea key handler.
   *   Enter                  → submit (or accept suggestion when popup is open)
   *   Ctrl+Enter, Shift+Enter→ insert newline (default textarea behaviour)
   *   Ctrl+Space             → open autocomplete popup
   *   ↑/↓                    → navigate suggestions (when popup open)
   *   Escape                 → close popup
   *   Tab                    → accept highlighted suggestion (when popup open)
   */
  onFilterKeydown(e: KeyboardEvent): void {
    if (e.ctrlKey && e.code === 'Space') {
      e.preventDefault();
      this.openSuggest();
      return;
    }

    if (this.suggestOpen()) {
      const items = this.suggestItems();
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (items.length) this.suggestIndex.update(i => (i + 1) % items.length);
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (items.length) this.suggestIndex.update(i => (i - 1 + items.length) % items.length);
        return;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        this.closeSuggest();
        return;
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        const pick = items[this.suggestIndex()];
        if (pick) {
          e.preventDefault();
          this.applySuggestion(pick);
          return;
        }
      }
    }

    if (e.key === 'Enter' && !e.ctrlKey && !e.shiftKey && !e.altKey && !e.metaKey) {
      e.preventDefault();
      this.search();
    }
  }

  // ── Autocomplete ──────────────────────────────────────────────────────
  private openSuggest(): void {
    const el = this.filterRef()?.nativeElement;
    if (!el) return;
    const caret = el.selectionStart ?? el.value.length;
    this.suggestAnchor = caret;
    this.suggestPrefix.set(this.currentPrefix(el.value, caret));
    this.suggestIndex.set(0);
    this.suggestOpen.set(true);
  }

  closeSuggest(): void {
    if (this.suggestOpen()) this.suggestOpen.set(false);
  }

  selectSuggestion(item: string): void {
    this.applySuggestion(item);
  }

  private applySuggestion(item: string): void {
    const el = this.filterRef()?.nativeElement;
    if (!el) { this.closeSuggest(); return; }
    const caret = el.selectionStart ?? el.value.length;
    const prefix = this.currentPrefix(el.value, caret);
    const start  = caret - prefix.length;
    const next   = el.value.slice(0, start) + item + el.value.slice(caret);
    el.value = next;
    const newCaret = start + item.length;
    el.setSelectionRange(newCaret, newCaret);
    this.filterInput.set(next);
    this.closeSuggest();
    el.focus();
  }

  /** Extracts the identifier-like token that ends at <paramref name="caret"/>. */
  private currentPrefix(value: string, caret: number): string {
    const head = value.slice(0, caret);
    const m = head.match(PREFIX_RE);
    return m ? m[0] : '';
  }

  /** Enter inside a date input triggers a search (only when both dates are valid). */
  onDateKeydown(e: Event): void {
    if ((e as KeyboardEvent).key === 'Enter') {
      e.preventDefault();
      this.search();
    }
  }

  reset(): void {
    this.filterInput.set('');
    this.filter.set('');
    this.timePreset.set('1d');
    this.applyPresetDates('1d');
    this.loadEvents();
  }

  /** Live updates the signal so the syntax-highlight overlay stays in sync.
   *  Does NOT trigger a search — submission happens on Enter or the Search button. */
  onFilterInput(value: string): void {
    this.filterInput.set(value);
    if (this.suggestOpen()) {
      const el = this.filterRef()?.nativeElement;
      const caret = el?.selectionStart ?? value.length;
      const prefix = this.currentPrefix(value, caret);
      this.suggestPrefix.set(prefix);
      this.suggestIndex.set(0);
    }
  }

  setTimePreset(preset: TimePreset): void {
    this.calPickingEnd.set(false);
    this.timePreset.set(preset);
    this.applyPresetDates(preset);
    this.loadEvents();
  }

  toggleLevel(level: string): void {
    // Toggle by patching the filter string
    const current = this.activeLevels();
    const next = new Set(current);
    if (next.has(level)) next.delete(level); else next.add(level);
    const allActive = next.size === LEVELS.length;
    this.filterInput.update(q => {
      const stripped = stripFilterClause(q, /@l\s*(=|in|!=)[^)\n]+(\))?/g);
      if (allActive) return stripped;
      const clause = next.size === 1
        ? `@l = '${[...next][0]}'`
        : `@l in [${[...next].map(l => `'${l}'`).join(', ')}]`;
      return stripped.trim() ? `${clause} and ${stripped.trim()}` : clause;
    });
    this.filter.set(this.filterInput());
    this.loadEvents();
  }

  isLevelActive(level: string): boolean {
    return this.activeLevels().has(level);
  }

  applyFilter(filter: string): void {
    this.filterInput.set(filter);
    this.filter.set(filter);
    this.loadEvents();
  }

  seek(from: Date, to: Date): void {
    if (this.liveActive()) this.stopLive();
    this.loading.set(true);
    this.error.set(null);

    const acc: EventDto[] = [];
    const size = this.pageSize();
    this.querySub?.unsubscribe();
    this.querySub = this.api.streamEvents({
      filter: this.filter() || undefined,
      from: from.toISOString(),
      to: to.toISOString(),
      count: size,
      dir: 'backward',
      levels: this.levelsParam(),
    }).subscribe({
      next: ev => {
        acc.push(ev);
        if (acc.length % 10 === 0) {
          this.events.set([...acc]);
          this.cdr.markForCheck();
        }
      },
      complete: () => {
        this.events.set([...acc]);
        this.loading.set(false);
        this.hasMore.set(acc.length >= size);
        this.cdr.markForCheck();
      },
      error: err => {
        this.error.set((err as Error).message ?? 'Seek failed');
        this.loading.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  toggleLive(): void {
    if (this.liveActive()) {
      this.stopLive();
    } else {
      this.startLive();
    }
  }

  private startLive(): void {
    this.liveActive.set(true);
    this.events.set([]);
    const cap = this.pageSize() * 4;
    this.liveSub = this.api.streamLive(this.filter() || undefined).subscribe({
      next: ev => {
        this.events.update(evs => [ev, ...evs.slice(0, cap - 1)]);
        this.cdr.markForCheck();
      },
      error: () => {
        this.liveActive.set(false);
        this.cdr.markForCheck();
      },
    });
  }

  private stopLive(): void {
    this.liveActive.set(false);
    this.liveSub?.unsubscribe();
    this.liveSub = undefined;
    this.loadEvents();
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }

  toggleAllLevels(): void {
    // Remove @l clause from filter — no clause means all levels
    this.filterInput.update(q => stripFilterClause(q, /@l\s*(=|in|!=)[^)\n]+(\))?/g));
    this.filter.set(this.filterInput());
    this.loadEvents();
  }

  resetAll(): void {
    this.quickSearch.set('');
    this.reset();
  }

  /** Tracks two-click calendar range selection: false = picking start, true = picking end. */
  calPickingEnd = signal<boolean>(false);

  // ── Date dropdown ─────────────────────────────────────────────────────
  openDateDropdown(): void {
    this.levelsDropdownOpen.set(false);
    this.serviceDropdownOpen.set(false);
    this.computeDdPos(this.dateFbGroup()?.nativeElement);
    this.calPickingEnd.set(false);
    const d = this.parseCustomDate(this.customFrom()) ?? new Date();
    this.calendarNav.set({ year: d.getFullYear(), month: d.getMonth() });
    this.dateDropdownOpen.set(true);
  }

  prevCalendarMonth(): void {
    this.calendarNav.update(({ year, month }) =>
      month === 0 ? { year: year - 1, month: 11 } : { year, month: month - 1 }
    );
  }

  nextCalendarMonth(): void {
    this.calendarNav.update(({ year, month }) =>
      month === 11 ? { year: year + 1, month: 0 } : { year, month: month + 1 }
    );
  }

  selectCalendarDay(d: { day: number; month: number; year: number }): void {
    const date    = new Date(d.year, d.month, d.day);
    const dateStr = format(date, 'yyyy-MM-dd');
    if (!this.calPickingEnd()) {
      const time = this.customFrom().split(' ')[1] || '00:00';
      this.customFrom.set(`${dateStr} ${time}`);
      this.customTo.set('');
      this.customFromSuggestion.set('');
      this.customToSuggestion.set('');
      this.timePreset.set('custom');
      this.calPickingEnd.set(true);
    } else {
      const fromDate = this.parseCustomDate(this.customFrom());
      if (fromDate && date < fromDate) {
        const time = this.customFrom().split(' ')[1] || '00:00';
        this.customFrom.set(`${dateStr} ${time}`);
        this.customTo.set('');
        this.customFromSuggestion.set('');
        this.customToSuggestion.set('');
        this.timePreset.set('custom');
      } else {
        const time = this.customTo().split(' ')[1] || '23:59';
        this.customTo.set(`${dateStr} ${time}`);
        this.customToSuggestion.set('');
        this.timePreset.set('custom');
        this.calPickingEnd.set(false);
      }
    }
  }

  // ── Levels dropdown (pending/apply) ───────────────────────────────────
  openLevelsDropdown(): void {
    // Close all other dropdowns first (fixes: old dropdown stays open)
    this.serviceDropdownOpen.set(false);
    this.dateDropdownOpen.set(false);
    this.computeDdPos(this.levelsFbGroup()?.nativeElement);
    this.pendingLevels.set(new Set(this.activeLevels()));
    this.levelsDropdownOpen.set(true);
  }

  togglePendingLevel(level: string): void {
    this.pendingLevels.update(set => {
      const next = new Set(set);
      if (next.has(level)) next.delete(level); else next.add(level);
      return next;
    });
  }

  toggleAllPendingLevels(): void {
    this.allPendingLevelsActive()
      ? this.pendingLevels.set(new Set())
      : this.pendingLevels.set(new Set(LEVELS as unknown as string[]));
  }

  isPendingLevelActive(level: string): boolean {
    return this.pendingLevels().has(level);
  }

  applyLevels(): void {
    const levels = new Set(this.pendingLevels());
    this.levelsDropdownOpen.set(false);
    // Write @l clause into filter as single source of truth
    const allActive = levels.size === LEVELS.length;
    this.filterInput.update(q => {
      const stripped = stripFilterClause(q, /@l\s*(=|in|!=)[^)\n]+(\))?/g);
      if (allActive) return stripped;
      const clause = levels.size === 1
        ? `@l = '${[...levels][0]}'`
        : `@l in [${[...levels].map(l => `'${l}'`).join(', ')}]`;
      return stripped.trim() ? `${clause} and ${stripped.trim()}` : clause;
    });
    this.filter.set(this.filterInput());
    this.loadEvents();
  }

  resetLevels(): void {
    this.pendingLevels.set(new Set(LEVELS as unknown as string[]));
  }

  pendingLevelsTotal = () =>
    Object.entries(this.levelCounts()).reduce((sum, [, n]) => sum + n, 0);

  // ── Services dropdown (pending/apply) ─────────────────────────────────
  openServicesDropdown(): void {
    // Close all other dropdowns first (fixes: old dropdown stays open)
    this.levelsDropdownOpen.set(false);
    this.dateDropdownOpen.set(false);
    // Lazy-load backend services if not yet loaded
    if (this.backendServices().length === 0) {
      this.api.getServiceNames().subscribe({
        next: s => { this.backendServices.set(s); this.cdr.markForCheck(); },
        error: () => {},
      });
    }
    this.computeDdPos(this.svcFbGroup()?.nativeElement);
    this.pendingServices.set(new Set(this.selectedServices()));
    this.serviceSearch.set('');
    this.showMoreServices.set(false);
    this.serviceDropdownOpen.set(true);
  }

  togglePendingService(svc: string): void {
    this.pendingServices.update(set => {
      const next = new Set(set);
      if (next.has(svc)) next.delete(svc); else next.add(svc);
      return next;
    });
  }

  isPendingServiceActive(svc: string): boolean {
    return this.pendingServices().has(svc);
  }

  applyServices(): void {
    const svcs = new Set(this.pendingServices());
    // selectedServices is computed from filterInput, so just patch the filter\n    this.serviceDropdownOpen.set(false);
    // Use bracket notation ['service.name'] so the parser treats it as a single
    // segment — matches the ev.ServiceName dedicated field fast-path on the backend.
    this.filterInput.update(q => {
      const stripped = stripFilterClause(
        q, /\['service\.name'\]\s*=\s*'[^']*'|\['service\.name'\]\s*in\s*\[[^\]]*\]|\(service\.name\s*=\s*'[^']*'\s*or\s*ApplicationContext\s*=\s*'[^']*'\)/g
      );
      if (svcs.size === 0) return stripped;
      const clause = svcs.size === 1
        ? `['service.name'] = '${[...svcs][0]}'`
        : `['service.name'] in [${[...svcs].map(s => `'${s}'`).join(', ')}]`;
      // Place AFTER @l clause if present, before everything else
      const lvlMatch = stripped.match(/^(@l\s*(?:=|in)\s*\[[^\]]+\]|@l\s*=\s*'[^']*')(\s+and\s+|$)/i);
      if (lvlMatch) {
        const rest = stripped.slice(lvlMatch[0].length).trim();
        return rest ? `${lvlMatch[1]} and ${clause} and ${rest}` : `${lvlMatch[1]} and ${clause}`;
      }
      return stripped.trim() ? `${clause} and ${stripped.trim()}` : clause;
    });
    this.filter.set(this.filterInput());
    this.loadEvents();
  }

  resetServices(): void {
    this.pendingServices.set(new Set());
  }

  servicePercent(svc: string): string {
    const total = this.totalCount();
    if (!total) return '0%';
    return `${Math.round(((this.serviceCounts()[svc] ?? 0) / total) * 100)}%`;
  }

  private readonly SERVICE_COLORS = [
    '#4DA3FF', '#38BDF8', '#34D399', '#A78BFA',
    '#FB923C', '#F472B6', '#22D3EE', '#818CF8',
    '#E879F9', '#4ADE80', '#FACC15', '#F87171',
  ];

  serviceColor(name: string): string {
    let h = 0;
    for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
    return this.SERVICE_COLORS[h % this.SERVICE_COLORS.length];
  }

  @HostListener('document:click', ['$event'])
  closeDropdowns(e: MouseEvent): void {
    if ((e.target as HTMLElement).closest('.fb-group')) return;
    this.levelsDropdownOpen.set(false);
    this.serviceDropdownOpen.set(false);
    this.dateDropdownOpen.set(false);
  }
}
