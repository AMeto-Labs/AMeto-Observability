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

/**
 * Shared empty highlight set. Reused BY IDENTITY so a flush that flashes nothing writes the
 * same reference back and the row bindings that read it are not invalidated. Never mutated.
 */
const EMPTY_IDS: ReadonlySet<string> = new Set<string>();

/**
 * Floor under the live buffer. The server hands over LiveTail.PageSize (500) events per poll,
 * and up to eight times that while a tail is catching up. `pageSize * 4` is 200 at the default
 * page size — smaller than a single poll — so the whole buffer turned over between two paints
 * and every row a reader could have started was gone before the next frame. That is why the
 * tail read as illegible rather than merely fast: no amount of smoothing rescues content that
 * is replaced faster than it can be looked at.
 */
const LIVE_BUFFER_MIN = 600;

/**
 * Rows in one flush above which the arrival flash is skipped. A highlight that fires on every
 * visible row marks nothing — past a dozen rows in a frame the stream's own motion IS the
 * signal, and the flash degenerates into a full-list strobe.
 */
const FLASH_MAX_BATCH = 12;

/**
 * How long a just-arrived row stays marked. Must match the wash animation in
 * event-list-row.scss: the class is what starts it, so a shorter window truncates the fade and
 * a longer one only leaves an already-finished animation sitting at its end state.
 */
const FLASH_MS = 900;

/**
 * How far back a tail may be asked to pick up. Bounded on purpose: seeding from the newest row
 * of a day-old page would make the server's first poll come back full, latch its catch-up mode,
 * and deliver history at full rate until it drained — the flood this whole path exists to avoid.
 */
const SEED_WINDOW_MS = 30_000;

/**
 * Hard flush trigger, independent of the frame clock. requestAnimationFrame does not fire in a
 * hidden tab while the tail keeps arriving, so this bounds `pending` without depending on the
 * visibility handler being correct. It is one server page, so a catch-up poll lands as whole
 * pages rather than as one unbounded write.
 */
const LIVE_FLUSH_MAX = 500;

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
  /**
   * Whether the list follows the head of the tail. False while the reader has scrolled away,
   * and then arrivals are held rather than published — see the flush path. Strictly stronger
   * than scroll anchoring: nothing is written, so the measurement rebuild, the level rescan and
   * the per-row transform rewrite cost nothing rather than less, and unlike an anchor it cannot
   * expire when the row it was anchored to falls out of the buffer.
   */
  liveFollow: boolean;
  /** How many events are waiting in the held buffer — the number on the "new events" pill. */
  livePending: number;
  /** Arrivals per second over the last whole second, or 0 on a quiet tail. */
  liveRate: number;
  /** Everything this tail has delivered since it opened, including what the cap has dropped. */
  liveReceived: number;
  /** Rows the buffer cap has discarded on this tail. A tail that drops must say so. */
  liveDropped: number;
  pageSize: number;
  wrapMessages: boolean;
  quickSearch: string;
  backendServices: string[];
  signalsPanelOpen: boolean;
  calendarNav: { year: number; month: number };
  calPickingEnd: boolean;
  /** Single selected event backing the detail drawer (replaces the old expanded-ids set). */
  selectedId: string | null;
  /**
   * The selected event held BY VALUE, so the drawer survives its row being evicted from the
   * live buffer. Deliberately a second place the selection lives, against this store's usual
   * single-source-of-truth doctrine: {@link selectedEvent} reconciles them by id, so the two
   * cannot disagree, and every path that clears `selectedId` clears this in the same patch.
   */
  selectedEventPinned: EventDto | null;
  /**
   * Ids of the events published by the LAST live flush — highlighted briefly, then dropped.
   * Readonly because the set is swapped wholesale rather than mutated: every flush either
   * hands over a fresh set or the shared {@link EMPTY_IDS}, and reusing that identity is what
   * lets a flush that flashes nothing avoid invalidating the row bindings that read it.
   */
  newEventIds: ReadonlySet<string>;
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
    liveFollow: true,
    livePending: 0,
    liveRate: 0,
    liveReceived: 0,
    liveDropped: 0,
    pageSize: 50,
    wrapMessages: false,
    quickSearch: '',
    backendServices: [],
    signalsPanelOpen: false,
    calendarNav: { year: new Date().getFullYear(), month: new Date().getMonth() },
    calPickingEnd: false,
    selectedId: null,
    selectedEventPinned: null,
    newEventIds: EMPTY_IDS,
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

    /**
     * How many rows the live tail retains. Floored at {@link LIVE_BUFFER_MIN} so the buffer is
     * never smaller than one server page — the page-size control is labelled "events per
     * request" and quietly resized this too, which at the default of 50 left the tail holding
     * less than half of what a single poll delivers.
     */
    const liveBufferSize = computed(() => Math.max(store.pageSize() * 4, LIVE_BUFFER_MIN));
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

    /**
     * The event backing the detail drawer, or null when nothing is selected.
     *
     * <p>The pin is checked FIRST and by id, so the two sources can never disagree — and while a
     * tail runs it also spares this computed a linear scan of the buffer on every pass. Holding
     * the DTO by value is what stops the drawer deleting itself mid-read: the selection used to
     * be resolved by scanning the live ring buffer, which the tail is constantly evicting, so on
     * a busy stand the selected row fell out within a second, this flipped to null, and the
     * @if in the template unmounted the panel the user was reading — then the list widened into
     * the freed space and re-laid-out every rendered row. A jolt caused entirely by traffic the
     * user did not create.</p>
     */
    const selectedEvent = computed<EventDto | null>(() => {
      const id = store.selectedId();
      if (!id) return null;
      const pinned = store.selectedEventPinned();
      if (pinned?.id === id) return pinned;
      return store.events().find(e => e.id === id) ?? null;
    });

    return {
      activeLevels, selectedServices, displayedEvents, availableServices,
      levelCounts, serviceCounts, totalCount, allLevelsActive, liveBufferSize,
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
    history = inject(SearchHistoryService).forScope('logs'),
  ) => {
    // Streaming subscriptions live in the closure: the previous is torn down before a
    // new one starts, and both are disposed on destroy (via _disposeStreams).
    let querySub: Subscription | undefined;
    let liveSub: Subscription | undefined;
    // What the RUNNING tail was opened with — the only two things it carries. A mutation
    // that leaves both unchanged needs no reconnect (see loadEvents).
    let liveFilter = '';
    let liveLevels: string | undefined;
    // ── Live buffer ──────────────────────────────────────────────────────
    // The tail publishes ONCE PER FRAME, not once per event. The server coalesces writes on a
    // 100 ms floor and answers a poll with up to a full page, so "one event" is not the unit
    // anything arrives in: a poll lands as a burst of hundreds of SSE frames inside a couple of
    // milliseconds. Publishing each one separately made the page's update rate the arrival rate
    // — a state write, a fresh `events` identity (which busts the virtualizer's measurement
    // memo and rewrites the transform on every rendered row), and a change-detection pass, all
    // several hundred times between two paints the user could actually see. The historical
    // search in this same file has flushed in blocks since it was written (see loadEvents), and
    // Traces buffers its stream for the same reason; the live path was the one that did not.
    //
    // `pending` is OLDEST FIRST — the order the tail streams in — and is reversed at flush.
    let pending: EventDto[] = [];
    // Rows that arrived while the reader was scrolled away from the head, NEWEST FIRST (the
    // shape flushLive already produced). They are merged in by resumeFollow and by nothing
    // else: this is the buffer behind the "N new events" pill.
    let held: EventDto[] = [];
    let flushHandle: number | undefined;
    // Whether `flushHandle` came from requestAnimationFrame or from setTimeout; the two need
    // different cancellers and a host without a frame clock must still be able to stop one.
    let flushIsFrame = false;
    /** ONE expiry timer for the whole highlight window, never one per event. */
    let flashTimer: ReturnType<typeof setTimeout> | undefined;
    /** The recent flushes still inside the highlight window — see {@link flashIdsFor}. */
    let flashRuns: { ids: string[]; at: number }[] = [];
    /** Publishes {@link liveRate} once a second. A third live handle: it must be cancelled
     *  everywhere the flush handle is, or a dead tail goes on reporting a rate. */
    let rateTimer: ReturnType<typeof setInterval> | undefined;
    let rateWindow = 0;
    /** Everything this tail has delivered, counted where it arrives and published on a tick —
     *  it has to keep counting what the cap is about to throw away, but it must not cost a
     *  state write to do it. */
    let received = 0;
    /**
     * Whether the thing that failed was the TAIL. The live error handler sets `live: false`
     * before the banner can be shown, so the Retry button had no way to tell a dead tail from a
     * failed search and answered both with a search — which threw away every live row the user
     * was watching, under a message the server had written as "Reconnect in a moment."
     */
    let liveWasRunning = false;

    function cancelFlushHandle(): void {
      if (flushHandle === undefined) return;
      if (flushIsFrame) cancelAnimationFrame(flushHandle);
      else clearTimeout(flushHandle);
      flushHandle = undefined;
    }

    /** Books the next frame to publish on, unless one is already booked. */
    function scheduleFlush(): void {
      if (flushHandle !== undefined) return;
      if (typeof requestAnimationFrame === 'function') {
        flushIsFrame = true;
        flushHandle = requestAnimationFrame(() => flushLive());
      } else {
        // No frame clock (a non-browser host): a 16 ms timer is the same cadence.
        flushIsFrame = false;
        flushHandle = setTimeout(() => flushLive(), 16) as unknown as number;
      }
    }

    /**
     * Publishes everything that arrived since the last frame in ONE state write.
     *
     * <p>Order is the easy thing to get backwards here. The tail is a FORWARD query
     * (EndpointMapper: `Direction = QueryDirection.Forward`), so it streams OLDEST first, and
     * the old code landed newest-on-top only as a side effect of prepending one event at a
     * time. A batch has to reverse explicitly — without it timestamps run the wrong way inside
     * each ~100 ms chunk and are correct only between chunks, which is the kind of wrongness
     * that looks like a rendering glitch rather than a bug.</p>
     */
    function flushLive(): void {
      cancelFlushHandle();
      if (pending.length === 0) return;
      const batch = pending;
      pending = [];
      batch.reverse();

      // Read per flush, not captured at connect: changing the page size then only resizes the
      // buffer, instead of needing the connection torn down and rebuilt.
      const cap = store.liveBufferSize();
      const kept = store.events();
      const holding = !store.liveFollow();
      // A seeded tail is asked to resume from a row it has already sent, and `from` is
      // inclusive, so the boundary event arrives twice. Dedupe against whichever side the join
      // is actually being made on: while the reader is parked, the newest rows are in the hold
      // buffer and `events` is frozen well behind them, so checking `events` would catch
      // nothing. Same shape loadMore uses.
      const against = holding ? held : kept;
      const fresh = against.length === 0
        ? batch
        : (() => { const seen = new Set(against.map(e => e.id)); return batch.filter(e => !seen.has(e.id)); })();
      if (fresh.length === 0) return;

      // While the reader is parked away from the head the list does not move AT ALL — see
      // liveFollow. Held rows are kept newest-first, the shape flushLive already produced.
      if (holding) {
        const overflow = Math.max(0, held.length + fresh.length - cap);
        held = fresh.concat(held).slice(0, cap);
        patchState(store, {
          livePending: Math.min(store.livePending() + fresh.length, cap),
          liveDropped: store.liveDropped() + overflow,
        });
        return;
      }

      const events = fresh.length >= cap
        ? fresh.slice(0, cap)                                   // already newest-first
        : fresh.concat(kept.slice(0, cap - fresh.length));
      const dropped = Math.max(0, kept.length + fresh.length - cap);
      // ONE patchState for rows AND highlight. @ngrx/signals rebuilds the whole state object
      // per CALL, so the cost is the number of calls rather than the size of the patch: three
      // calls per event became one per frame.
      patchState(store, {
        events,
        newEventIds: flashIdsFor(fresh),
        ...(dropped ? { liveDropped: store.liveDropped() + dropped } : {}),
      });
    }

    /**
     * Ids to flash, as a SLIDING WINDOW over the recent flushes rather than the newest one.
     *
     * <p>Replaces the per-event bookkeeping, which copied the whole id set and armed a timer per
     * arriving event: at rate R that is R²/2 set insertions and R timers a second, each expiry
     * rebuilding an R-element array — the reason the page degraded suddenly rather than
     * gradually, and the reason it went on janking for a second after arrivals stopped. It also
     * fixes what the highlight MEANT: with a one-second window over a buffer this size, above a
     * couple of hundred events a second every rendered row was "new", so the flash marked
     * nothing and the list simply strobed.</p>
     *
     * <p>The window is why this keeps a list of runs instead of one set. Publishing only the
     * newest flush's ids meant a row carried `.is-new` for exactly one flush interval — about
     * 100 ms, the server's coalescing floor — so a 900 ms wash was cut off after a tenth of it
     * and the arrival read as a blink rather than a fade. That is the opposite of the thing
     * this whole path exists to fix. The window costs nothing to hold: a run is only recorded
     * when it is small enough to flash at all, so it is bounded by FLASH_MAX_BATCH per flush
     * over {@link FLASH_MS}.</p>
     */
    function flashIdsFor(batch: EventDto[]): ReadonlySet<string> {
      const now = Date.now();
      flashRuns = flashRuns.filter(r => now - r.at < FLASH_MS);
      if (batch.length <= FLASH_MAX_BATCH) flashRuns.push({ ids: batch.map(e => e.id), at: now });
      if (flashRuns.length === 0) return EMPTY_IDS;

      // Re-armed on every flush so the set is emptied FLASH_MS after arrivals stop, which is
      // the only moment nothing else will come along to expire the tail of the window.
      if (flashTimer !== undefined) clearTimeout(flashTimer);
      flashTimer = setTimeout(() => {
        flashTimer = undefined;
        flashRuns = [];
        patchState(store, { newEventIds: EMPTY_IDS });
      }, FLASH_MS);

      const ids = new Set<string>();
      for (const run of flashRuns) for (const id of run.ids) ids.add(id);
      return ids;
    }

    /**
     * Puts everything the tail delivered onto the list, wherever it currently is.
     *
     * <p>The pair, not just the flush: a flush made while the reader is parked routes into the
     * hold buffer rather than onto the list, so anything that follows with {@link clearNew}
     * would throw those rows away. Both callers — a stop and a stream that died — are ending the
     * tail, and neither may delete rows the user was one click away from reading.</p>
     */
    function landEverything(): void {
      flushLive();
      resumeFollow();
    }

    /**
     * Cancels the pending flush and the highlight, and drops everything the tail was holding.
     *
     * <p>Cancellation lives HERE rather than at each call site because every reset path in this
     * file already goes through it — the aggregation branch, the reload branch, stopLive and the
     * live error handler. A frame that survived one of them would drop rows captured under the
     * OLD query on top of a list that was just emptied for a new one, which is precisely what
     * loadAggregation's "nothing on screen may belong to a query other than the one in the box"
     * forbids.</p>
     */
    function clearNew(): void {
      cancelFlushHandle();
      pending = [];
      held = [];
      if (flashTimer !== undefined) { clearTimeout(flashTimer); flashTimer = undefined; }
      flashRuns = [];
      if (rateTimer !== undefined) { clearInterval(rateTimer); rateTimer = undefined; }
      rateWindow = 0;
      if (store.newEventIds().size || store.livePending() || store.liveRate())
        patchState(store, { newEventIds: EMPTY_IDS, livePending: 0, liveRate: 0 });
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
        // BOTH halves read the APPLIED query. `activeLevels` is parsed from the draft, so
        // comparing it against liveLevels mixed an applied filter with an unapplied level set:
        // edit the text without pressing Enter, then change the page size, and the tail
        // silently reopened with the OLD filter and the NEW levels — a combination the user
        // never asked for, with the live dot still lit and nothing on screen to say so.
        if (store.filter() !== liveFilter || levelsParam(parseLevelsFromFilter(store.filter())) !== liveLevels)
          startLive();
        return;
      }
      querySub?.unsubscribe();
      clearNew();
      liveWasRunning = false;

      patchState(store, {
        loading: true, error: null, events: [], aggregation: null, hasMore: true,
        selectedId: null, selectedEventPinned: null,
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
      // An aggregation ENDS a tail — loadEvents tears the stream down before routing here, and
      // the search error handler redirects here too when the server recognises a query this
      // client's regex declined. Either way what is on screen from now on came from a table, so
      // Retry must run the table again. Clearing this in loadEvents' non-live branch alone was
      // not enough: the aggregation branch returns before ever reaching it, so a failed
      // aggregation offered a Retry that reopened a live tail carrying `select … group by …`,
      // which the tail endpoint refuses by design.
      liveWasRunning = false;
      patchState(store, {
        loading: true, error: null, events: [], aggregation: null, hasMore: false,
        selectedId: null, selectedEventPinned: null,
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
    /**
     * The level and service pickers rewrite the filter STRING by regex, and they rewrite the
     * DRAFT — so the question is what the draft is, not what was last applied. Reading the
     * applied query let a chip clicked while an aggregation was half-typed splice
     * `@l = 'Error' and ` onto the front of it and send that, un-asked.
     */
    function clauseEditingBlocked(): boolean { return isAggregationQuery(store.filterInput()); }

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
      // This is also the RESTART path — loadEvents reconnects when the filter or the level set
      // changes mid-tail — and it was the only reset in this file that emptied `newEventIds`
      // without cancelling what feeds it. Rows captured under the old query would then land on
      // the new list a frame later, and a second's worth of expiry timers went on firing
      // against state they no longer described.
      clearNew();

      const nextFilter = store.filter();
      const nextLevels = levelsParam(parseLevelsFromFilter(nextFilter));
      // Restarting on the SAME question is a continuation, not a new answer. This is a
      // deliberate softening of "nothing on screen may belong to a query other than the one in
      // the box": the guard is that the query is byte-identical, and when it is not, the list is
      // still emptied exactly as before.
      const sameQuery = liveFilter === nextFilter && liveLevels === nextLevels;
      liveFilter = nextFilter;
      liveLevels = nextLevels;
      received = 0;

      // Where the tail picks up. Seeding is what lets it CONTINUE the list instead of replacing
      // it with a blank panel, which is the roughest single moment on this page: press Live,
      // lose the search you just ran, read a false "no events match", then take an avalanche out
      // of nowhere. See SEED_WINDOW_MS for why it is bounded.
      const floor = new Date(Date.now() - SEED_WINDOW_MS).toISOString();
      const newest = sameQuery ? store.events()[0]?.['@t'] : undefined;
      // Keep the list only when the tail can actually CONTINUE it — same question, and a newest
      // row inside the seed window. Keeping it otherwise would prepend the last thirty seconds
      // onto a page from yesterday and leave a silent day-wide hole in the middle of a list
      // that reads as one continuous run of time. That is a different lie from the blank panel,
      // not a smaller one, so the old clearing behaviour is kept for exactly that case.
      const continues = !!newest && newest > floor;
      const from = continues ? newest : floor;

      patchState(store, {
        live: true, error: null, newEventIds: EMPTY_IDS,
        livePending: 0, liveRate: 0, liveReceived: 0, liveDropped: 0,
        // The table belongs to the query it came from. Leaving it up while a tail runs put a
        // snapshot of totals on screen with the live dot lit beside it.
        aggregation: null,
        // Follow is reset only where the LIST is: this function is also the restart path, and a
        // tab coming back from the background travels it with the reader still parked halfway
        // down. Forcing follow there would answer "I was reading this" by writing over it.
        // Where the list IS cleared there is nothing to be parked in front of, so the head is
        // the only sensible place to be.
        ...(continues ? {} : {
          events: [], selectedId: null, selectedEventPinned: null, liveFollow: true,
        }),
      });

      liveWasRunning = true;
      openTail(from);
    }

    /**
     * Opens the stream itself and nothing else — no state is reset here.
     *
     * <p>Split out of {@link startLive} because {@link resumeLive} must NOT travel the rest of
     * it. A tab coming back from the background is not a new question: routing it through the
     * full start discarded the hold buffer the reader had parked in front of, and — once the
     * pause outlasted the seed window, which ten seconds on another tab is enough to do — took
     * the `continues` branch and wiped the list, the selection and the pinned drawer with it.</p>
     */
    function openTail(from: string): void {
      liveSub?.unsubscribe();
      rateWindow = 0;
      if (rateTimer !== undefined) clearInterval(rateTimer);
      // Arrivals per second, counted in the tail and published once a second. One interval for
      // the whole stream — the counter resets each tick, so a tail that goes quiet decays to 0
      // instead of freezing on its last busy reading.
      rateTimer = setInterval(() => {
        patchState(store, { liveRate: rateWindow, liveReceived: received });
        rateWindow = 0;
      }, 1000);

      liveSub = api.streamLive({ filter: liveFilter || undefined, levels: liveLevels, from }).subscribe({
        next: ev => {
          // Everything here is O(1) and touches NO state. The whole point of the buffer is that
          // an arriving event costs a push; the counters are closure variables for the same
          // reason — a patchState per event is exactly what this path used to do.
          pending.push(ev);
          rateWindow++;
          received++;
          if (pending.length >= LIVE_FLUSH_MAX) flushLive();
          else scheduleFlush();
        },
        error: (err: Error) => {
          // Say WHY it stopped. The tail reports a blown poll budget or a server too busy
          // to keep it fed through a query-error frame carrying a real sentence, and
          // discarding it left the toggle flipping itself off for no visible reason.
          liveSub = undefined;
          // Everything buffered belongs to the stream that just died, but it is also the last
          // thing the user will get — land it before tearing the buffer down, so the list ends
          // where the tail ended rather than a frame short of it. Publishing alone is not
          // enough: with the reader parked, a flush routes into the hold buffer, which clearNew
          // then drops — a stream that failed would silently delete every row behind the pill.
          landEverything();
          clearNew();
          patchState(store, { live: false, error: err?.message || 'Live tail stopped' });
        },
      });
    }

    /**
     * Stops the tail and FREEZES what is on screen.
     *
     * <p>It used to re-run a backward search over the toolbar's window. That answered a
     * different question from the one being asked: the universal reason to stop a tail is
     * "something went past, hold it so I can read it", and for the default 1d preset the reload
     * returned a page from a completely different part of the timeline, with the selection
     * dropped and the scroll position gone — a second full-screen flicker immediately after the
     * one starting Live had caused. Reloading is still one click away (Apply), but it is now
     * something the user asks for rather than something stop implies.</p>
     */
    function stopLive(): void {
      patchState(store, { live: false });
      liveSub?.unsubscribe();
      liveSub = undefined;
      liveWasRunning = false;
      // Freeze everything the tail actually delivered, not everything it managed to publish:
      // the buffer and the held rows are both part of what the user pressed stop to look at,
      // and clearNew below drops whatever is still in them.
      landEverything();
      clearNew();
    }

    /**
     * Suspends the tail without ending it — the tab went to the background.
     *
     * <p>`live` stays true so the toggle does not flicker, and `events` is untouched. Both other
     * live surfaces on this client already gate on document.hidden; this tail was the one that
     * kept taking a server search slot every hundred milliseconds for a page nobody was looking
     * at, and then dumped the backlog the moment it came back. The frame clock it now publishes
     * on does not run in a hidden tab either, so stopping is the honest behaviour rather than
     * buffering indefinitely.</p>
     */
    function pauseLive(): void {
      if (!store.live() || !liveSub) return;
      liveSub.unsubscribe();
      liveSub = undefined;
      if (rateTimer !== undefined) { clearInterval(rateTimer); rateTimer = undefined; }
      // Publish what already arrived rather than stranding it in the buffer.
      flushLive();
      cancelFlushHandle();
      patchState(store, { liveRate: 0 });
    }

    /**
     * Re-opens a tail suspended by {@link pauseLive}, keeping everything the reader had.
     *
     * <p>Picks up from the newest row actually held — which is the HOLD buffer's head, not the
     * list's, whenever the reader is parked, since those rows are newer than anything on screen.
     * Clamped to the same seed floor as a fresh start, for the same reason: an unbounded seed
     * makes the server's first poll come back full and latch its catch-up mode.</p>
     *
     * <p>A pause longer than that floor therefore leaves a gap. That is a real cost and it is
     * the lesser one: the alternative on the table was to treat the return as a new question and
     * throw away the reader's list, their place in it and the row they had open, which is a
     * larger loss and one they did not ask for by switching tabs.</p>
     */
    function resumeLive(): void {
      if (!store.live() || liveSub) return;
      const floor = new Date(Date.now() - SEED_WINDOW_MS).toISOString();
      const newest = held[0]?.['@t'] ?? store.events()[0]?.['@t'];
      openTail(newest && newest > floor ? newest : floor);
    }

    /**
     * Whether the list follows the head. Driven by the scroll position: the moment the reader
     * moves off the top the tail stops writing, and arrivals pile up behind the pill instead.
     */
    function setFollow(on: boolean): void {
      if (on === store.liveFollow()) return;
      if (on) resumeFollow();
      else patchState(store, { liveFollow: false });
    }

    /**
     * Merges everything held while the reader was away and follows the head again.
     *
     * <p>`held` is ALREADY newest-first — flushLive reversed each batch before holding it — so
     * this must not reverse again. That is the second of the two places in this file where wire
     * order can go silently wrong, and the symptom is identical to the first: timestamps that
     * run the wrong way inside a block and correctly between blocks.</p>
     */
    function resumeFollow(): void {
      const batch = held;
      held = [];
      if (batch.length === 0) {
        if (!store.liveFollow() || store.livePending())
          patchState(store, { liveFollow: true, livePending: 0 });
        return;
      }
      const cap = store.liveBufferSize();
      const kept = store.events();
      const seen = new Set(kept.map(e => e.id));
      const fresh = batch.filter(e => !seen.has(e.id));
      const events = fresh.length >= cap
        ? fresh.slice(0, cap)
        : fresh.concat(kept.slice(0, cap - fresh.length));
      const dropped = Math.max(0, kept.length + fresh.length - cap);
      patchState(store, {
        events, liveFollow: true, livePending: 0,
        // The merge evicts too, and a tail that drops must say so wherever it drops.
        ...(dropped ? { liveDropped: store.liveDropped() + dropped } : {}),
        // Deliberately no flash: everything here is new, so flashing it would mark the whole
        // list — the same thing FLASH_MAX_BATCH exists to prevent on a fast tail.
        newEventIds: EMPTY_IDS,
      });
    }

    /**
     * Recovers from whatever actually failed — the tail, or a search.
     *
     * <p>Both used to answer with a search, which for a dead tail deleted every live row the
     * user still had on screen.</p>
     */
    function retry(): void {
      if (liveWasRunning) startLive(); else loadEvents();
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

    /**
     * Selects the event for the detail drawer; pass null to close it. The DTO is pinned
     * alongside the id — see {@link EventsState.selectedEventPinned} — so a live tail evicting
     * the row does not close the panel under the reader.
     */
    function selectEvent(id: string | null): void {
      patchState(store, {
        selectedId: id,
        selectedEventPinned: id ? store.events().find(e => e.id === id) ?? null : null,
      });
    }

    /** Row click: open the drawer for this event, or close it when it is already open. */
    function toggleEvent(id: string): void {
      selectEvent(store.selectedId() === id ? null : id);
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

    /**
     * @internal Disposes active SSE streams and every live handle; invoked from the onDestroy
     * hook. Cancels directly rather than through {@link clearNew}, so nothing patches state on
     * a store that is being torn down.
     */
    function _disposeStreams(): void {
      querySub?.unsubscribe();
      liveSub?.unsubscribe();
      cancelFlushHandle();
      pending = [];
      held = [];
      if (flashTimer !== undefined) { clearTimeout(flashTimer); flashTimer = undefined; }
      flashRuns = [];
      if (rateTimer !== undefined) { clearInterval(rateTimer); rateTimer = undefined; }
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
      startLive,
      pauseLive,
      resumeLive,
      setFollow,
      resumeFollow,
      retry,
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
