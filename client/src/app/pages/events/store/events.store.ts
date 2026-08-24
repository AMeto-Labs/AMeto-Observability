import { computed, inject } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { subDays, format } from 'date-fns';
import { Subscription } from 'rxjs';
import {
  patchState, signalStore, withComputed, withHooks, withMethods, withState,
} from '@ngrx/signals';

import { ApiService } from '../../../core/services/api.service';
import { SearchHistoryService } from '../../../core/services/search-history.service';
import { EventDto, LEVELS, AggregationDto } from '../../../core/models/event.model';
import {
  TimePreset,
  parseLevelsFromFilter, parseServicesFromFilter,
  setLevelsClause, setServicesClause, levelsParam,
  parseCustomDate, fmtDateInput, presetFrom,
  collectPropPaths, isoToDotNetUtcTicksString, isAggregationQuery,
} from './events-filter.util';

/** Page sizes offered next to the event counter — also the whitelist for the `size` URL param. */
const PAGE_SIZE_OPTIONS = [50, 100, 150, 300, 500];

/** A single cell of the 42-cell calendar month grid. */
interface CalendarDay {
  day: number;
  month: number;
  year: number;
  isCurrentMonth: boolean;
  isToday: boolean;
  isFrom: boolean;
  isTo: boolean;
  inRange: boolean;
}

/** Writable state owned by the Events page. */
interface EventsState {
  filterInput: string;
  filter: string;
  timePreset: TimePreset;
  customFrom: string;
  customTo: string;
  customFromSuggestion: string;
  customToSuggestion: string;
  events: EventDto[];
  loading: boolean;
  loadingMore: boolean;
  hasMore: boolean;
  error: string | null;
  live: boolean;
  pageSize: number;
  wrapMessages: boolean;
  quickSearch: string;
  backendServices: string[];
  signalsPanelOpen: boolean;
  calendarNav: { year: number; month: number };
  calPickingEnd: boolean;
  /** Single selected event backing the detail drawer (replaces the old expanded-ids set). */
  selectedId: string | null;
  /** Ids of events that just arrived on the live tail — highlighted for ~1s, then dropped. */
  newEventIds: Set<string>;
  /** The table a `select … group by …` query answered with; null whenever the page lists events. */
  aggregation: AggregationDto | null;
}

/**
 * Owns all state and logic for the Events page. Provided at the component level
 * (no `providedIn`) so each mount gets a fresh instance. Ported faithfully from the
 * former `EventsComponent`: same query params, same regex clause handling, same
 * progressive-streaming behaviour — only the change-detection plumbing disappears,
 * since `patchState` is inherently reactive.
 */
export const EventsStore = signalStore(
  // Factory form so `calendarNav` picks up the current month at mount, not at module load.
  withState<EventsState>(() => ({
    filterInput: '',
    filter: '',
    timePreset: '1d',
    customFrom: '',
    customTo: '',
    customFromSuggestion: '',
    customToSuggestion: '',
    events: [],
    loading: false,
    loadingMore: false,
    hasMore: true,
    error: null,
    live: false,
    pageSize: 50,
    wrapMessages: false,
    quickSearch: '',
    backendServices: [],
    signalsPanelOpen: false,
    calendarNav: { year: new Date().getFullYear(), month: new Date().getMonth() },
    calPickingEnd: false,
    selectedId: null,
    newEventIds: new Set<string>(),
    aggregation: null,
  })),

  withComputed((store) => {
    // activeLevels / selectedServices are derived from filterInput (single source of
    // truth). The dropdowns read/write only the filter string, so there is no separate
    // state to desync.
    const activeLevels = computed(() => parseLevelsFromFilter(store.filterInput()));
    const selectedServices = computed(() => parseServicesFromFilter(store.filterInput()));

    /**
     * True while the APPLIED query is an aggregation — deliberately the applied query and not
     * the draft. Reading the draft made the page disagree with itself: the mode note appeared
     * while a list of events was still on screen, the Live button greyed out mid-stream with
     * rows visibly arriving, and a table could outlive the query that produced it. What is
     * displayed came from `filter`, so what the controls say about it has to come from `filter`.
     */
    const isAggregation = computed(() => isAggregationQuery(store.filter()));

    /** Events list — optionally narrowed client-side by service and quick-search. */
    const displayedEvents = computed(() => {
      let evs = store.events();
      const svcs = selectedServices();
      if (svcs.size > 0) evs = evs.filter(e => svcs.has((e['service.name'] as string) ?? ''));
      const q = store.quickSearch().trim().toLowerCase();
      if (q) evs = evs.filter(e => (e['@mt'] ?? '').toLowerCase().includes(q));
      return evs;
    });

    const availableServices = computed<string[]>(() => {
      // Merge backend-known services with any additional ones seen in current events.
      const svcs = new Set<string>(store.backendServices());
      for (const ev of store.events()) {
        const svc = ev['service.name'] as string | undefined;
        if (svc) svcs.add(svc);
      }
      return [...svcs].sort();
    });

    const levelCounts = computed(() => {
      const counts: Record<string, number> = {};
      for (const ev of store.events()) {
        const lvl = (ev['@l'] ?? 'information').toLowerCase();
        counts[lvl] = (counts[lvl] ?? 0) + 1;
      }
      return counts;
    });

    const serviceCounts = computed(() => {
      const counts: Record<string, number> = {};
      for (const ev of store.events()) {
        const svc = ev['service.name'] as string | undefined;
        if (svc) counts[svc] = (counts[svc] ?? 0) + 1;
      }
      return counts;
    });

    const totalCount = computed(() => store.events().length);
    const allLevelsActive = computed(() => activeLevels().size === LEVELS.length);

    const levelsLabel = computed(() => {
      const active = activeLevels();
      if (active.size === LEVELS.length) return 'All levels';
      if (active.size === 1) return [...active][0];
      return `${active.size} levels`;
    });

    const serviceLabel = computed(() => {
      const s = selectedServices();
      if (s.size === 0) return 'All services';
      if (s.size === 1) return [...s][0];
      return `${s.size} services`;
    });

    const dateRangeLabel = computed(
      () => `${store.customFrom() || '…'} – ${store.customTo() || 'now'}`,
    );

    const calendarMonthLabel = computed(() => {
      const { year, month } = store.calendarNav();
      return format(new Date(year, month, 1), 'MMMM yyyy');
    });

    const calendarDays = computed<CalendarDay[]>(() => {
      const { year, month } = store.calendarNav();
      const firstDow = new Date(year, month, 1).getDay();
      const startOffset = (firstDow + 6) % 7; // Monday-first
      const today = format(new Date(), 'yyyy-MM-dd');
      const fromDate = parseCustomDate(store.customFrom());
      const toDate = parseCustomDate(store.customTo());
      const fromStr = fromDate ? format(fromDate, 'yyyy-MM-dd') : null;
      const toStr = toDate ? format(toDate, 'yyyy-MM-dd') : null;
      const days: CalendarDay[] = [];
      for (let i = 0; i < 42; i++) {
        const d = new Date(year, month, 1 - startOffset + i);
        const ds = format(d, 'yyyy-MM-dd');
        days.push({
          day: d.getDate(),
          month: d.getMonth(),
          year: d.getFullYear(),
          isCurrentMonth: d.getMonth() === month,
          isToday: ds === today,
          isFrom: ds === fromStr,
          isTo: ds === toStr,
          inRange: !!(fromStr && toStr && ds > fromStr && ds < toStr),
        });
      }
      return days;
    });

    const customFromValid = computed(() => !!parseCustomDate(store.customFrom()));
    /** "To" is optional — empty is valid; a non-empty value must parse. */
    const customToValid = computed(() => {
      const v = store.customTo();
      return !v || !!parseCustomDate(v);
    });
    const canSearch = computed(() => customFromValid() && customToValid());

    /** Property paths discovered in the currently loaded events (sorted) — feeds autocomplete. */
    const knownPropPaths = computed<string[]>(() => {
      const set = new Set<string>();
      for (const ev of store.events()) collectPropPaths(ev.props ?? {}, '', set, 0);
      return [...set].sort();
    });

    /** The event backing the detail drawer, or null when nothing is selected. */
    const selectedEvent = computed<EventDto | null>(() => {
      const id = store.selectedId();
      if (!id) return null;
      return store.events().find(e => e.id === id) ?? null;
    });

    return {
      activeLevels, selectedServices, displayedEvents, availableServices,
      levelCounts, serviceCounts, totalCount, allLevelsActive,
      levelsLabel, serviceLabel, dateRangeLabel, calendarMonthLabel, calendarDays,
      customFromValid, customToValid, canSearch, knownPropPaths, selectedEvent,
      isAggregation,
    };
  }),

  withMethods((
    store,
    api = inject(ApiService),
    router = inject(Router),
    route = inject(ActivatedRoute),
    history = inject(SearchHistoryService),
  ) => {
    // Streaming subscriptions live in the closure: the previous is torn down before a
    // new one starts, and both are disposed on destroy (via _disposeStreams).
    let querySub: Subscription | undefined;
    let liveSub: Subscription | undefined;
    // What the RUNNING tail was opened with — the only two things it carries. A mutation
    // that leaves both unchanged needs no reconnect (see loadEvents).
    let liveFilter = '';
    let liveLevels: string | undefined;
    // Per-event timers that drop an id out of `newEventIds` ~1s after it arrived.
    let newTimers: ReturnType<typeof setTimeout>[] = [];

    /** Flags a just-arrived live event as "new" for 1s, driving the row highlight. */
    function markNew(id: string): void {
      patchState(store, { newEventIds: new Set(store.newEventIds()).add(id) });
      const t = setTimeout(() => {
        newTimers = newTimers.filter(x => x !== t);
        const next = new Set(store.newEventIds());
        if (next.delete(id)) patchState(store, { newEventIds: next });
      }, 1000);
      newTimers.push(t);
    }

    /** Cancels all pending highlight timers and clears the set. */
    function clearNew(): void {
      newTimers.forEach(clearTimeout);
      newTimers = [];
      if (store.newEventIds().size) patchState(store, { newEventIds: new Set() });
    }

    // ── Private helpers ─────────────────────────────────────────────────────
    /** From-instant for a query: parsed customFrom, else one day back. */
    function getFromDate(): Date {
      return parseCustomDate(store.customFrom()) ?? subDays(new Date(), 1);
    }

    /** To-instant for a query (ISO), or undefined when the range is open-ended. */
    function getToDate(): string | undefined {
      const d = parseCustomDate(store.customTo());
      return d ? d.toISOString() : undefined;
    }

    /**
     * Applies preset date bounds. `custom` leaves the current range untouched; every
     * other preset sets customFrom from presetFrom(), opens the end (customTo ''), and
     * clears both inline suggestions.
     */
    function applyPresetDates(preset: TimePreset): void {
      if (preset === 'custom') return;
      patchState(store, {
        customFrom: presetFrom(preset),
        customTo: '',
        customFromSuggestion: '',
        customToSuggestion: '',
      });
    }

    /**
     * Mirrors the toolbar/filter state into the URL query params. `levels` is written as
     * null so any legacy `levels=` param is dropped — the @l clause inside `filter` is
     * the single source of truth.
     */
    function syncRoute(): void {
      const queryParams: Record<string, string | null> = {
        filter: store.filter() || null,
        preset: store.timePreset(),
        from: store.customFrom() || null,
        to: store.customTo() || null,
        levels: null,
        size: store.pageSize() === 50 ? null : String(store.pageSize()),
      };
      router.navigate([], { queryParams, replaceUrl: true });
    }

    // ── Actions ─────────────────────────────────────────────────────────────
    function loadEvents(): void {
      // Every filter, level, time and page-size mutator ends here.
      // `select count(*) …` asks a different question and gets a different answer: a table,
      // over plain JSON. Checked BEFORE the live branch, not after it: a tail running when the
      // user applies an aggregation would otherwise swallow it as a filter change and open
      // /api/events/live?filter=select…, which the server refuses by design — the page then
      // showed a banner telling the user to make an HTTP call themselves. An aggregation is a
      // snapshot, so it ends the tail rather than competing with it.
      const query = store.filter();
      if (isAggregationQuery(query)) {
        liveSub?.unsubscribe();
        liveSub = undefined;
        querySub?.unsubscribe();
        clearNew();
        patchState(store, { live: false });
        loadAggregation(query);
        return;
      }

      // Reconnect only for what the tail actually carries. It is opened with a filter and a
      // level set and nothing else, so a time-window or page-size change has nothing to apply.
      if (store.live()) {
        if (store.filter() !== liveFilter || levelsParam(store.activeLevels()) !== liveLevels)
          startLive();
        return;
      }
      querySub?.unsubscribe();
      clearNew();

      patchState(store, {
        loading: true, error: null, events: [], aggregation: null, hasMore: true, selectedId: null,
      });

      const acc: EventDto[] = [];
      const size = store.pageSize();
      querySub = api.streamEvents({
        filter: store.filter() || undefined,
        from: getFromDate().toISOString(),
        to: getToDate(),
        count: size,
        dir: 'backward',
        levels: levelsParam(store.activeLevels()),
      }).subscribe({
        next: ev => {
          acc.push(ev);
          // Progressive paint: flush every 10 events so long streams feel responsive.
          if (acc.length % 10 === 0) patchState(store, { events: [...acc] });
        },
        complete: () => {
          patchState(store, { events: [...acc], loading: false, hasMore: acc.length >= size });
          syncRoute();
        },
        error: err => {
          const message = (err as Error).message ?? 'Failed to load events';

          // The server tokenises where this client pattern-matches, so it recognises
          // aggregations the regex declines — `select "count"(*)`, anything with a stray
          // character the lexer drops. It answers those by naming the aggregate endpoint, so
          // take it at its word and run the query where it belongs instead of showing the
          // user a banner telling them to make an HTTP call. That makes the server's
          // tokeniser the single definition and the regex merely a fast path.
          if (message.includes('/api/events/aggregate')) { loadAggregation(query); return; }

          // Keep what arrived. A search stopped by the server's time budget reports an
          // error AFTER streaming real rows, and throwing them away leaves the user with
          // an empty screen and a message, instead of a partial answer and a message
          // explaining why it is partial. hasMore stays false so infinite scroll does not
          // immediately re-fire the same doomed query.
          patchState(store, {
            events: [...acc],
            hasMore: false,
            error: message,
            loading: false,
          });
        },
      });
    }

    /**
     * Runs an aggregation and keeps the table. The event list is emptied on purpose: nothing
     * on screen may belong to a query other than the one in the box.
     */
    function loadAggregation(query: string): void {
      patchState(store, {
        loading: true, error: null, events: [], aggregation: null, hasMore: false, selectedId: null,
      });

      querySub = api.aggregate({
        filter: query,
        from:   getFromDate().toISOString(),
        to:     getToDate(),
      }).subscribe({
        next: agg => {
          patchState(store, { aggregation: agg, loading: false });
          syncRoute();
        },
        error: (err: { error?: { error?: string } }) =>
          patchState(store, {
            loading: false,
            // The server's own sentence — it names the position of a syntax error and the
            // reason a scan stopped, both of which are worth more than "request failed".
            error: err?.error?.error?.trim() || 'The aggregation failed.',
          }),
      });
    }

    /**
     * Loads the next (older) page using the (@t, id) cursor of the oldest loaded event.
     * Triggered when the user scrolls near the bottom of the list.
     */
    function loadMore(): void {
      if (store.live() || store.loading() || store.loadingMore() || !store.hasMore()) return;

      const list = store.events();
      const last = list[list.length - 1];
      if (!last) return;

      const afterTsTicks = isoToDotNetUtcTicksString(last['@t']);

      patchState(store, { loadingMore: true, error: null });

      const acc: EventDto[] = [];
      const size = store.pageSize();
      querySub?.unsubscribe();
      querySub = api.streamEvents({
        filter: store.filter() || undefined,
        from: getFromDate().toISOString(),
        to: getToDate(),
        count: size,
        dir: 'backward',
        afterId: last.id,
        afterTs: afterTsTicks,
        levels: levelsParam(store.activeLevels()),
      }).subscribe({
        next: ev => acc.push(ev),
        complete: () => {
          const patch: Partial<EventsState> = { loadingMore: false, hasMore: acc.length >= size };
          if (acc.length > 0) {
            // Deduplicate: cursor overlap can re-emit an event; duplicate ids would
            // otherwise let two rows share a selection.
            const evs = store.events();
            const seen = new Set(evs.map(e => e.id));
            const fresh = acc.filter(e => !seen.has(e.id));
            if (fresh.length > 0) patch.events = [...evs, ...fresh];
          }
          patchState(store, patch);
        },
        error: err => {
          // Same reasoning as loadEvents: append whatever the page delivered before it
          // failed, and stop paging so a budget-stopped query is not re-issued on the
          // next scroll.
          const patch: Partial<EventsState> = {
            error: (err as Error).message ?? 'Failed to load more events',
            loadingMore: false,
            hasMore: false,
          };
          if (acc.length > 0) {
            const evs = store.events();
            const seen = new Set(evs.map(e => e.id));
            const fresh = acc.filter(e => !seen.has(e.id));
            if (fresh.length > 0) patch.events = [...evs, ...fresh];
          }
          patchState(store, patch);
        },
      });
    }

    function search(): void {
      if (!store.canSearch()) return;
      patchState(store, { filter: store.filterInput() });
      history.record(store.filterInput());
      loadEvents();
    }

    function applyFilter(filter: string): void {
      patchState(store, { filterInput: filter, filter });
      history.record(filter);
      loadEvents();
    }

    /** Live-syncs the filter draft (drives the syntax-highlight overlay); does NOT search. */
    function setFilterInput(value: string): void {
      patchState(store, { filterInput: value });
    }

    function reset(): void {
      patchState(store, { filterInput: '', filter: '', timePreset: '1d' });
      applyPresetDates('1d');
      loadEvents();
    }

    function resetAll(): void {
      patchState(store, { quickSearch: '' });
      reset();
    }

    function setTimePreset(preset: TimePreset): void {
      patchState(store, { calPickingEnd: false, timePreset: preset });
      applyPresetDates(preset);
      loadEvents();
    }

    function setFrom(v: string): void {
      // suggestion is managed by the date input's (suggestionChange) binding — don't clear it here
      patchState(store, { customFrom: v, timePreset: 'custom' });
    }

    function setTo(v: string): void {
      patchState(store, { customTo: v, timePreset: 'custom' });
    }

    /** Ghost-suggestion setters, driven by the date inputs' (suggestionChange) binding. */
    function setFromSuggestion(v: string): void {
      patchState(store, { customFromSuggestion: v });
    }

    function setToSuggestion(v: string): void {
      patchState(store, { customToSuggestion: v });
    }

    /** Prepares the calendar when the date dropdown opens: reset picking, jump to the start month. */
    function openCalendar(): void {
      const d = parseCustomDate(store.customFrom()) ?? new Date();
      patchState(store, {
        calPickingEnd: false,
        calendarNav: { year: d.getFullYear(), month: d.getMonth() },
      });
    }

    function acceptFromSuggestion(): void {
      const s = store.customFromSuggestion();
      if (!s) return;
      patchState(store, {
        customFrom: store.customFrom() + s,
        customFromSuggestion: '',
        timePreset: 'custom',
      });
    }

    function acceptToSuggestion(): void {
      const s = store.customToSuggestion();
      if (!s) return;
      patchState(store, {
        customTo: store.customTo() + s,
        customToSuggestion: '',
        timePreset: 'custom',
      });
    }

    /**
     * The level and service pickers rewrite the filter STRING by regex — `setLevelsClause`
     * splices `@l = 'Error' and ` onto the front of whatever is there. Against a `select …`
     * query that produces a sentence in neither language, so every route into them stops here,
     * not only the toolbar buttons the template disables.
     */
    function clauseEditingBlocked(): boolean { return store.isAggregation(); }

    function toggleLevel(level: string): void {
      if (clauseEditingBlocked()) return;
      const next = new Set(store.activeLevels());
      if (next.has(level)) next.delete(level); else next.add(level);
      const filterInput = setLevelsClause(store.filterInput(), next);
      patchState(store, { filterInput, filter: filterInput });
      loadEvents();
    }

    function toggleAllLevels(): void {
      if (clauseEditingBlocked()) return;
      // No @l clause ⇒ all levels; setLevelsClause with the full set strips the clause.
      const filterInput = setLevelsClause(store.filterInput(), new Set<string>(LEVELS));
      patchState(store, { filterInput, filter: filterInput });
      loadEvents();
    }

    function setLevels(levels: Set<string>): void {
      if (clauseEditingBlocked()) return;
      const filterInput = setLevelsClause(store.filterInput(), levels);
      patchState(store, { filterInput, filter: filterInput });
      loadEvents();
    }

    function setServices(svcs: Set<string>): void {
      if (clauseEditingBlocked()) return;
      const filterInput = setServicesClause(store.filterInput(), svcs);
      patchState(store, { filterInput, filter: filterInput });
      loadEvents();
    }

    /** Client-side quick filter over '@mt' (does not re-query the backend). */
    function setQuickSearch(value: string): void {
      patchState(store, { quickSearch: value });
    }

    function setPageSize(n: number): void {
      if (!PAGE_SIZE_OPTIONS.includes(n) || n === store.pageSize()) return;
      patchState(store, { pageSize: n });
      loadEvents();
    }

    function startLive(): void {
      // Unsubscribe first: this is also the restart path when the filter or the level
      // selection changes mid-tail, and leaving the old stream running would interleave
      // rows the new filter excludes.
      liveSub?.unsubscribe();
      // A backward search started before the tail (toggling live off and straight back on
      // leaves one in flight) still owns its subscription, and its completion handler
      // replaces `events` wholesale without consulting live(). Left running it drops a page
      // of history on top of the live rows, or sets an error banner while the live dot is on.
      querySub?.unsubscribe();
      querySub = undefined;

      liveFilter = store.filter();
      liveLevels = levelsParam(store.activeLevels());
      patchState(store, {
        live: true, error: null, events: [], newEventIds: new Set(), selectedId: null,
        // The table belongs to the query it came from. Leaving it up while a tail runs put a
        // snapshot of totals on screen with the live dot lit beside it.
        aggregation: null,
      });
      liveSub = api.streamLive({ filter: liveFilter || undefined, levels: liveLevels }).subscribe({
        next: ev => {
          // Read per event, not captured: changing the page size then only resizes the
          // buffer, instead of needing the connection torn down and rebuilt.
          const cap = store.pageSize() * 4;
          patchState(store, { events: [ev, ...store.events().slice(0, cap - 1)] });
          markNew(ev.id);
        },
        error: (err: Error) => {
          // Say WHY it stopped. The tail reports a blown poll budget or a server too busy
          // to keep it fed through a query-error frame carrying a real sentence, and
          // discarding it left the toggle flipping itself off for no visible reason.
          liveSub = undefined;
          clearNew();
          patchState(store, { live: false, error: err?.message || 'Live tail stopped' });
        },
      });
    }

    function stopLive(): void {
      patchState(store, { live: false });
      liveSub?.unsubscribe();
      liveSub = undefined;
      clearNew();
      loadEvents();
    }

    function toggleLive(): void {
      // An aggregation is a snapshot of a window, not a stream: there is nothing for a tail to
      // append to a table of totals.
      if (!store.live() && store.isAggregation()) return;
      if (store.live()) stopLive(); else startLive();
    }

    /**
     * Applies a [from, to] window chosen from a timestamp context-menu action (± seek,
     * "events after/before this"). Routes through the normal load path so the toolbar
     * date filter reflects the window and it persists to the URL (loadEvents → syncRoute) —
     * the same behaviour as the filter / bind-time context-menu actions. The filter string
     * is left untouched (the seek only moves the time window).
     *
     * The date inputs are minute-precision, so floor `from` / ceil `to` to the minute to
     * guarantee the picked event stays inside an otherwise sub-minute (± seconds) window.
     */
    function seek(from: Date, to: Date): void {
      if (store.live()) stopLive();
      const floorMin = (d: Date) => new Date(Math.floor(d.getTime() / 60_000) * 60_000);
      const ceilMin  = (d: Date) => new Date(Math.ceil(d.getTime() / 60_000) * 60_000);
      patchState(store, {
        customFrom: fmtDateInput(floorMin(from)),
        customTo: fmtDateInput(ceilMin(to)),
        timePreset: 'custom',
      });
      loadEvents();
    }

    /**
     * Zooms the active query window to an explicit [from, to] range and reloads.
     * Drives the log-volume histogram's click-to-zoom. Reuses the normal reload
     * path (loadEvents) and only rewrites the time bounds — the filter string is
     * left untouched.
     */
    function setTimeRange(fromIso: string, toIso: string): void {
      patchState(store, {
        customFrom: fmtDateInput(new Date(fromIso)),
        customTo: fmtDateInput(new Date(toIso)),
        timePreset: 'custom',
      });
      loadEvents();
    }

    /** A row's timestamp was chosen as the start/end of the active time range. */
    function onTimeRangeBound(e: { side: 'from' | 'to'; date: Date }): void {
      if (e.side === 'from') patchState(store, { customFrom: fmtDateInput(e.date), timePreset: 'custom' });
      else patchState(store, { customTo: fmtDateInput(e.date), timePreset: 'custom' });
      loadEvents();
    }

    /** Selects the event for the detail drawer; pass null to close it. */
    function selectEvent(id: string | null): void {
      patchState(store, { selectedId: id });
    }

    /** Row click: open the drawer for this event, or close it when it is already open. */
    function toggleEvent(id: string): void {
      patchState(store, { selectedId: store.selectedId() === id ? null : id });
    }

    function toggleWrap(): void {
      patchState(store, { wrapMessages: !store.wrapMessages() });
    }

    function toggleSignalsPanel(): void {
      patchState(store, { signalsPanelOpen: !store.signalsPanelOpen() });
    }

    function prevCalendarMonth(): void {
      const { year, month } = store.calendarNav();
      patchState(store, {
        calendarNav: month === 0 ? { year: year - 1, month: 11 } : { year, month: month - 1 },
      });
    }

    function nextCalendarMonth(): void {
      const { year, month } = store.calendarNav();
      patchState(store, {
        calendarNav: month === 11 ? { year: year + 1, month: 0 } : { year, month: month + 1 },
      });
    }

    function selectCalendarDay(d: { day: number; month: number; year: number }): void {
      const date = new Date(d.year, d.month, d.day);
      const dateStr = format(date, 'yyyy-MM-dd');
      if (!store.calPickingEnd()) {
        const time = store.customFrom().split(' ')[1] || '00:00';
        patchState(store, {
          customFrom: `${dateStr} ${time}`,
          customTo: '',
          customFromSuggestion: '',
          customToSuggestion: '',
          timePreset: 'custom',
          calPickingEnd: true,
        });
        return;
      }
      const fromDate = parseCustomDate(store.customFrom());
      if (fromDate && date < fromDate) {
        // Second click precedes the start → treat it as a new start instead.
        const time = store.customFrom().split(' ')[1] || '00:00';
        patchState(store, {
          customFrom: `${dateStr} ${time}`,
          customTo: '',
          customFromSuggestion: '',
          customToSuggestion: '',
          timePreset: 'custom',
        });
      } else {
        const time = store.customTo().split(' ')[1] || '23:59';
        patchState(store, {
          customTo: `${dateStr} ${time}`,
          customToSuggestion: '',
          timePreset: 'custom',
          calPickingEnd: false,
        });
      }
    }

    /** Lazily fetches the full backend service list on first use (static per session). */
    function loadBackendServices(): Promise<void> {
      if (store.backendServices().length > 0) return Promise.resolve();
      return new Promise<void>(resolve => {
        api.getServiceNames().subscribe({
          next: s => { patchState(store, { backendServices: s }); resolve(); },
          error: () => resolve(),
        });
      });
    }

    /** Restores state from URL query params (call from the component's ngOnInit), then loads. */
    function initFromUrl(): void {
      const qp = route.snapshot.queryParamMap;
      const urlFilter = qp.get('filter') ?? '';
      const urlPreset = (qp.get('preset') as TimePreset) || '1d';
      const urlFrom = qp.get('from') ?? '';
      const urlTo = qp.get('to') ?? '';
      const urlLevels = qp.get('levels');
      const urlSize = Number(qp.get('size'));

      const patch: Partial<EventsState> = {
        filterInput: urlFilter, filter: urlFilter, timePreset: urlPreset,
      };
      if (PAGE_SIZE_OPTIONS.includes(urlSize)) patch.pageSize = urlSize;
      patchState(store, patch);

      if (urlFrom) {
        patchState(store, { customFrom: urlFrom, customTo: urlTo });
      } else {
        applyPresetDates(urlPreset);
      }

      if (urlLevels) {
        // Legacy: old URLs carried a separate `levels=` param. Apply it only when the
        // filter has no @l clause yet (avoids a duplicate clause on reload).
        const alreadyInFilter = /@l\s*(=|in)/i.test(urlFilter);
        if (!alreadyInFilter) {
          const lvlSet = new Set(urlLevels.split(',').filter(Boolean));
          if (lvlSet.size < LEVELS.length) {
            const clause = lvlSet.size === 1
              ? `@l = '${[...lvlSet][0]}'`
              : `@l in [${[...lvlSet].map(l => `'${l}'`).join(', ')}]`;
            const base = urlFilter.trim();
            const merged = base ? `${clause} and ${base}` : clause;
            patchState(store, { filterInput: merged, filter: merged });
          }
        }
      }

      loadEvents();
    }

    /** @internal Disposes active SSE streams + highlight timers; invoked from the onDestroy hook. */
    function _disposeStreams(): void {
      querySub?.unsubscribe();
      liveSub?.unsubscribe();
      newTimers.forEach(clearTimeout);
      newTimers = [];
    }

    return {
      initFromUrl,
      loadEvents,
      loadMore,
      search,
      applyFilter,
      setFilterInput,
      reset,
      resetAll,
      setTimePreset,
      setFrom,
      setTo,
      setFromSuggestion,
      setToSuggestion,
      openCalendar,
      acceptFromSuggestion,
      acceptToSuggestion,
      toggleLevel,
      toggleAllLevels,
      setLevels,
      setServices,
      setQuickSearch,
      setPageSize,
      toggleLive,
      seek,
      setTimeRange,
      onTimeRangeBound,
      selectEvent,
      toggleEvent,
      toggleWrap,
      toggleSignalsPanel,
      prevCalendarMonth,
      nextCalendarMonth,
      selectCalendarDay,
      loadBackendServices,
      _disposeStreams,
    };
  }),

  withHooks({
    onDestroy(store) {
      store._disposeStreams();
    },
  }),
);
