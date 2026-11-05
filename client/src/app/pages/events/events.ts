import {
  Component, inject, viewChild, viewChildren, ElementRef, OnInit, HostListener,
  ChangeDetectionStrategy, afterRenderEffect, effect, untracked, signal, computed, DestroyRef,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { format } from 'date-fns';
import {
  injectVirtualizer, measureElement as measureRenderedElement,
} from '@tanstack/angular-virtual';

import { EmptyStateComponent } from '../../shared/components/ui';
import { BottomSheetComponent } from '../../shared/components/bottom-sheet/bottom-sheet';
import { EventDto, AggregationDto } from '../../core/models/event.model';
import { ViewportService } from '../../core/services/viewport.service';
import { EventsToolbarComponent } from './components/events-toolbar/events-toolbar';
import { EventsFilterBarComponent } from './components/events-filter-bar/events-filter-bar';
import { EventsFilterSheetComponent } from './components/events-filter-sheet/events-filter-sheet';
import { EventListRowComponent } from './components/event-list-row/event-list-row';
import { EventDetailComponent } from './components/event-detail/event-detail';
import { MessageActionSheetComponent } from './components/message-action-sheet/message-action-sheet';
import { AggregationTableComponent, aggRowSummary } from './components/aggregation-table/aggregation-table';
import { SignalsPanelComponent } from './signals-panel/signals-panel';
import { EventsStore } from './store/events.store';
import { UserPreferencesService } from '../../core/services/user-preferences.service';

/** The SERVICE cell around its name: the 12px icon, the 5px gap and the 10px right padding. */
const SVC_COL_CHROME = 27;
/** The SERVICE column is never wider than this — a longer name ends in an ellipsis… */
const SVC_COL_MAX = 140;
/** …and never narrower than its header. */
const SVC_COL_MIN = 70;
/** The tablet's SERVICE column (design canvas, "Планшет 768 — логи"): one width, not measured. */
const SVC_COL_TABLET = 120;
/** The tablet's drawer: a 440px slide-over, whatever width the desktop drawer was dragged to. */
const DRAWER_TABLET = 440;

/**
 * A day separator in the phone's list (design canvas, "Анатомия строки на телефоне"): the date
 * has left the row, so a 24px line names the day before the first row of each new date — and
 * before the first row of the list when that is not today. An item of the virtual list in its
 * own right, so it scrolls with the rows and is counted in their indices.
 */
export interface DaySeparator {
  kind: 'day';
  /** `yyyy-MM-dd`, local time, as the rows' times are local. */
  date: string;
  /** The virtualizer's key: `day:` + the date, which no event id can be. */
  id: string;
}

/** One entry of the virtual list: an event, or (phone only) a day separator. */
export type ListItem = EventDto | DaySeparator;

/** The phone's two-line rows are estimated at this height and measured (design: 49 / 67px). */
export const PHONE_ROW_ESTIMATE = 60;
/** The day separator's height: fixed by its stylesheet, so this is exact rather than a guess. */
export const DAY_SEPARATOR_H = 24;

export function isDaySeparator(item: ListItem | undefined): item is DaySeparator {
  return !!item && (item as DaySeparator).kind === 'day';
}

/** The local calendar day of an event's timestamp, as the separator writes it. */
export function dayOf(iso: string): string {
  return format(new Date(iso), 'yyyy-MM-dd');
}

/**
 * The phone's list: the rows with a separator before the first of each new day, and before the
 * first row of all when it is not from today — today's rows need no heading, and a live tail at
 * the head would otherwise carry one that moves with every arrival.
 */
export function withDaySeparators(rows: readonly EventDto[], today: string): ListItem[] {
  const items: ListItem[] = [];
  let current = today;
  for (const ev of rows) {
    const day = dayOf(ev['@t']);
    if (day !== current) {
      items.push({ kind: 'day', date: day, id: `day:${day}` });
      current = day;
    }
    items.push(ev);
  }
  return items;
}

/**
 * Events page shell. All state and logic live in {@link EventsStore} (provided
 * here so each mount is fresh); the toolbar, filter bar and detail drawer are
 * self-contained children. This container only lays them out and drives the
 * fixed-height virtual list.
 *
 * <p>Below 640px (design canvas, "Логи на телефоне") the toolbar and filter bar give way to the
 * phone header, search row and chip strip the toolbar component renders, the rows are two lines
 * each and measured, day separators replace the date, the detail is a layer over the list and
 * the filters, Signals and the row's actions are bottom sheets. From 640 to 899px the table
 * drops its date column and the drawer and Signals panel slide over the list behind a scrim.
 * The breakpoints are read here through {@link ViewportService} and in the stylesheets through
 * `_breakpoints.scss`; the two name the same numbers.</p>
 */
@Component({
  selector: 'app-events',
  imports: [
    LucideAngularModule, EmptyStateComponent, BottomSheetComponent,
    EventsToolbarComponent, EventsFilterBarComponent, EventsFilterSheetComponent,
    EventListRowComponent, EventDetailComponent, MessageActionSheetComponent, SignalsPanelComponent,
    AggregationTableComponent,
  ],
  templateUrl: './events.html',
  // Two stylesheets: the component-style budget (angular.json) is checked per stylesheet, and
  // the phone and tablet layers are kept apart from the desktop rules, as traces.scss does.
  styleUrls: ['./events.scss', './events.responsive.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers: [EventsStore],
})
export class EventsComponent implements OnInit {
  readonly store = inject(EventsStore);
  readonly prefs = inject(UserPreferencesService);
  readonly viewport = inject(ViewportService);

  private eventsScroll = viewChild<ElementRef<HTMLElement>>('eventsScroll');
  private drawerEl = viewChild<ElementRef<HTMLElement>>('drawerEl');

  private rowEls = viewChildren<ElementRef<HTMLElement>>('evRowEl');
  private readonly destroyRef = inject(DestroyRef);

  /**
   * A touch screen, or the phone layout on any screen: the rows answer a long press with the
   * message action sheet and the detail opens action sheets in place of its cursor-anchored
   * menus. The phone layout counts even under a mouse — at 390px a menu drawn at the cursor
   * runs off the screen as surely as it would under a finger.
   */
  readonly touch = computed(() => this.viewport.phone() || this.viewport.coarse());

  /**
   * Width of the SERVICE column, one for the header and every row: the widest service name in
   * the loaded list with its icon and padding, between SVC_COL_MIN and SVC_COL_MAX. Sized per
   * row it was a different width on each, and the message after it started somewhere else on
   * every line; a fixed width wasted room when the names were short.
   */
  readonly svcColWidth = computed(() => {
    this.fontsReady();                       // widths measured in a fallback font are redone
    let widest = 0;
    for (const name of Object.keys(this.store.serviceCounts())) widest = Math.max(widest, this.textWidth(name));
    return Math.min(SVC_COL_MAX, Math.max(SVC_COL_MIN, Math.ceil(widest) + SVC_COL_CHROME));
  });

  /** What `--svc-col` is set to: the measured width, or the tablet's fixed 120px. */
  readonly svcCol = computed(() => `${this.viewport.tablet() ? SVC_COL_TABLET : this.svcColWidth()}px`);

  /**
   * The drawer's inline width: the dragged preference on a desktop; nothing on a tablet or a
   * phone, where the stylesheet sizes the slide-over (440px) and the layer (the screen).
   */
  readonly drawerWidth = computed(() =>
    this.viewport.phone() || this.viewport.tablet() ? null : this.prefs.drawerWidth());

  /** Measured service-name widths: each name is measured once, not on every live flush. */
  private readonly nameWidths = new Map<string, number>();
  private measureCtx?: CanvasRenderingContext2D | null;
  private readonly fontsReady = signal(false);

  /** A service name's width in the row's font: --fs-xs (11px) in --font-mono, what .ev-svc-name renders at. */
  private textWidth(name: string): number {
    let w = this.nameWidths.get(name);
    if (w !== undefined) return w;
    if (this.measureCtx === undefined) {
      this.measureCtx = document.createElement('canvas').getContext('2d');
      const root = getComputedStyle(document.documentElement);
      const family = root.getPropertyValue('--font-mono').trim();
      // Read from the same token the row's name is set in, so a change to the scale cannot leave
      // the measured column and the rendered name at different sizes. No stylesheet (a test
      // host): the token's value.
      const size = root.getPropertyValue('--fs-xs').trim() || '11px';
      if (this.measureCtx) this.measureCtx.font = `${size} ${family || 'sans-serif'}`;
    }
    // No canvas (a test host): a generous per-character guess rather than nothing.
    w = this.measureCtx ? this.measureCtx.measureText(name).width : name.length * 7;
    this.nameWidths.set(name, w);
    return w;
  }

  /**
   * True while the list is parked at the head. It gates the virtualizer's scroll anchor and it
   * is the same predicate the store's follow mode uses — the viewport is a component concern,
   * so it is read here and pushed down rather than duplicated.
   */
  private readonly atHead = signal(true);

  /**
   * Both other live surfaces on this client gate on document.hidden. This tail was the one that
   * went on taking a server search slot every hundred milliseconds for a page nobody was
   * looking at — and the frame clock it publishes on does not run while hidden, so it would
   * simply accumulate and then dump on return.
   */
  private readonly onVisibility = (): void => {
    if (document.hidden) this.store.pauseLive(); else this.store.resumeLive();
  };

  /**
   * What the virtual list holds: the rows, or on a phone the rows with their day separators.
   * The desktop and tablet lists ARE the store's array — no copy per live flush — and the
   * template reads either shape through {@link asEvent} / {@link asDay}.
   */
  readonly listItems = computed<readonly ListItem[]>(() => {
    const rows = this.store.displayedEvents();
    if (!this.viewport.phone()) return rows;
    return withDaySeparators(rows, dayOf(new Date().toISOString()));
  });

  /** Whether rows are measured rather than assumed: wrapped rows, and every phone row. */
  readonly measured = computed(() => this.store.wrapMessages() || this.viewport.phone());

  /**
   * One virtualizer for BOTH list modes.
   *
   * <p>Wrap mode used to opt out of virtualization entirely — it rendered every row, because
   * fixed-height virtualization cannot hold variable heights. That is true, and the conclusion
   * was the wrong one: it made the mode that produces the TALLEST rows the only one that put
   * all of them in the DOM at once, so a wrapped 5000-row answer was ~5000 rows of layout and
   * the page stopped responding. Measured rows are the answer to variable heights, and the
   * Traces list already does exactly this with the same library.</p>
   *
   * <p>Fixed mode keeps its 24px (--log-row-h) and measures nothing, which is what the original
   * comment here was protecting: its rows are one line by construction, and measuring them
   * re-introduced an overlap on selection. Only wrap mode measures — and the phone, whose
   * two-line rows are 49 or 67px by their message ({@link measured}). `measureElement` is
   * `undefined` (not a no-op function) otherwise so the library takes its own non-measuring
   * path rather than one that lies about a height.</p>
   */
  readonly virtualizer = injectVirtualizer(() => {
    // Snapshot the rows ONCE per options evaluation; the per-item closures below read the
    // snapshot rather than the signal. Two things depend on that, and both fail silently
    // otherwise:
    //  • getItemKey and estimateSize are called once per index inside the measurement rebuild,
    //    so reading a signal there costs two reactive-graph reads per row per pass;
    //  • more importantly, the library decides whether to capture a scroll anchor by comparing
    //    the NEW getItemKey(0) against the key the PREVIOUS options produced — and its
    //    measurement view computes keys LAZILY. A closure that reads the live signal answers
    //    with the NEW key for the OLD measurement, the edge keys compare equal, and, because
    //    `count` is pinned at the buffer cap once a tail is at steady state so nothing else in
    //    that comparison moves, the anchor below would never be captured at all.
    // The closures must still be minted FRESH on each evaluation: a hoisted, stable getItemKey
    // would leave the library's measurement memo permanently valid and it would serve stale
    // keys for ever.
    const items = this.listItems();
    const phone = this.viewport.phone();
    const measured = this.measured();
    // Fixed mode: --log-row-h, the collapsed row's pinned height (event-list-row.scss) — the two
    // numbers must move together. Wrapped rows are one line more often than not (4px of padding
    // around an 18px line), so the estimate only has to be close enough that the scrollbar does
    // not jump while the real heights arrive. The phone's rows are 49 or 67px by their message
    // (event-list-row.scss), so 60 is the guess between the two; its separators are exactly 24.
    const rowHeight = phone ? PHONE_ROW_ESTIMATE : this.store.wrapMessages() ? 40 : 24;
    return {
      count: items.length,
      scrollElement: this.eventsScroll(),
      estimateSize: (i: number) => isDaySeparator(items[i]) ? DAY_SEPARATOR_H : rowHeight,
      overscan: 20,
      getItemKey: (i: number) => items[i]?.id ?? i,
      measureElement: measured ? measureRenderedElement : undefined,
      // Rows are absolutely positioned and moved by transform, so the browser's own scroll
      // anchoring has nothing to work with: a prepend shifts every cumulative `start` while
      // scrollTop is untouched, and the line under the reader's eye walks down the screen one
      // row per arrival. 'end' re-finds the row at the current offset by key after the option
      // change and rewrites both the internal offset and the real scrollTop.
      //
      // It must NOT be on at the head, where the anchor item IS index 0 — that would pin the
      // old top row and scroll the user away from the events they turned Live on to watch.
      anchorTo: this.atHead() ? 'start' : 'end',
    } as const;
  });

  /**
   * The phone's sticky day heading: the date of the row under the list's top edge, drawn over
   * the list while that row's own separator has scrolled away. Nothing at the head (the first
   * separator, when there is one, is in view there), nothing while a separator itself is the
   * topmost item, nothing for today — today's rows carry no separator either.
   */
  readonly stickyDay = computed<string | null>(() => {
    if (!this.viewport.phone()) return null;
    const offset = this.virtualizer.scrollOffset() ?? 0;
    if (offset <= 0) return null;
    const top = this.virtualizer.getVirtualItems().find(v => v.end > offset);
    const item = top ? this.listItems()[top.index] : undefined;
    if (!item || isDaySeparator(item)) return null;
    const day = dayOf(item['@t']);
    return day === dayOf(new Date().toISOString()) ? null : day;
  });

  /**
   * The row last opened in the phone's layer, kept marked after the layer closes (design canvas,
   * "Анатомия строки на телефоне": the row stays selected after Back, the list where it was) —
   * the list is still the one the reader left, so the row they came from is worth finding
   * again. Dropped with the list, when a new search runs.
   */
  readonly lastViewedId = signal<string | null>(null);

  /** The event whose message action sheet is open (a long press on its row), or null. */
  readonly messageSheetEvent = signal<EventDto | null>(null);

  /** How many loaded events share the sheet's message template — the count its heading shows. */
  readonly messageSheetLikeCount = computed(() => {
    const ev = this.messageSheetEvent();
    if (!ev) return 0;
    const mt = ev['@mt'];
    let n = 0;
    for (const e of this.store.events()) if (e['@mt'] === mt) n++;
    return n;
  });

  /**
   * Whether the phone's event layer has pushed a history entry of its own, so the system Back
   * (and, on iOS, the edge swipe) closes the layer instead of leaving the page.
   */
  private detailHistoryPushed = false;

  private readonly onPopState = (): void => {
    if (!this.detailHistoryPushed) return;
    this.detailHistoryPushed = false;
    this.store.selectEvent(null);
  };

  constructor() {
    // Names measured before the web font arrived were measured in its fallback: measure again.
    void document.fonts?.ready.then(() => {
      this.nameWidths.clear();
      this.fontsReady.set(true);
    });

    // Turning Live on means "show me the newest", so go to the head and say so in both places
    // that hold the fact — the anchor's `atHead` here and the store's follow mode, which has no
    // way to see a scroll position of its own.
    //
    // This fires only on the false → true edge, which is exactly a tail the USER started: a
    // mid-tail restart (a filter change) and a resume after the tab was hidden both leave
    // `live` true throughout, so neither yanks the scroll out from under a reader.
    effect(() => {
      if (!this.store.live()) return;
      untracked(() => {
        this.atHead.set(true);
        // Set the mode outright rather than letting the scroll below announce it: a list
        // shorter than the viewport is already at offset 0, so scrollToOffset fires no scroll
        // event, and the page would sit at the head holding arrivals behind a pill.
        this.store.setFollow(true);
        this.virtualizer.scrollToOffset(0, { behavior: 'auto' });
      });
    });
    // Releases row elements the virtualizer still holds by key but that have left the document.
    // Keyed on event id, those entries outlive the answer that produced them, so a page left
    // open across many searches would otherwise pin a detached node per event ever rendered.
    // Tracks the ROW ARRAY, not the rendered elements, so scrolling pays nothing.
    afterRenderEffect(() => {
      this.store.displayedEvents();
      this.virtualizer.measureElement(null);
    });
    // Hands each rendered measured row over to be measured and observed. afterRender, not
    // effect: a height means nothing until the element exists and is laid out. Idempotent per
    // node, and a no-op in fixed mode, where nothing is measured.
    afterRenderEffect(() => {
      if (!this.measured()) return;
      for (const el of this.rowEls()) this.virtualizer.measureElement(el.nativeElement);
    });
    // Every switch of wrap mode — and of the phone layout, whose rows are another height again —
    // starts the row heights over. The virtualizer keeps each MEASURED height by row key and
    // prefers it to the estimate whatever `measureElement` says, so turning wrap off left every
    // row that had been measured at its wrapped height: the one-line rows sat spread out down
    // the list with the gaps where their extra lines had been, and the list never came back to
    // 24px. The reader is kept on the row at the top of the view — found in the OLD layout,
    // before the heights go — rather than left at a pixel offset that now points somewhere else
    // entirely.
    effect(() => {
      this.store.wrapMessages();
      this.viewport.phone();
      untracked(() => {
        const v = this.virtualizer;
        const top = this.atHead() ? undefined : v.getVirtualItemForOffset(v.scrollOffset() ?? 0)?.index;
        v.measure();
        if (top !== undefined) v.scrollToIndex(top, { align: 'start', behavior: 'auto' });
      });
    });
    // The phone's event layer is a place the system Back returns from: opening it pushes a
    // history entry (same URL), the popstate listener closes it, and a close by any other way —
    // the arrow, the X, Escape, a filter picked inside it — pops the entry again so Back does
    // not later land on a layer that is no longer there.
    // The row the phone's layer showed last stays marked; a new search starts the list over.
    effect(() => {
      const id = this.store.selectedId();
      if (id && this.viewport.phone()) untracked(() => this.lastViewedId.set(id));
    });
    effect(() => {
      if (this.store.loading()) untracked(() => this.lastViewedId.set(null));
    });
    effect(() => {
      const open = this.viewport.phone() && !!this.store.selectedEvent();
      untracked(() => {
        if (open && !this.detailHistoryPushed) {
          history.pushState({ ameto: 'event-detail' }, '');
          this.detailHistoryPushed = true;
        } else if (!open && this.detailHistoryPushed) {
          this.detailHistoryPushed = false;
          history.back();
        }
      });
    });
  }

  ngOnInit(): void {
    this.store.initFromUrl();
    document.addEventListener('visibilitychange', this.onVisibility);
    window.addEventListener('popstate', this.onPopState);
    this.destroyRef.onDestroy(() => {
      document.removeEventListener('visibilitychange', this.onVisibility);
      window.removeEventListener('popstate', this.onPopState);
    });
  }

  /** The tablet's slide-overs close on Escape, as the drawer does; the detail owns its own key. */
  @HostListener('document:keydown.escape', ['$event'])
  onEscape(e: Event): void {
    if (e.defaultPrevented) return;
    if (this.viewport.tablet() && this.store.signalsPanelOpen() && !this.store.selectedEvent()) {
      e.preventDefault();
      this.store.toggleSignalsPanel();
    }
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }

  /** A list item as an event, or null for a day separator — the template's discriminator. */
  asEvent(item: ListItem | undefined): EventDto | null {
    return item && !isDaySeparator(item) ? item : null;
  }

  asDay(item: ListItem | undefined): DaySeparator | null {
    return isDaySeparator(item) ? item : null;
  }

  /** The phone's footer under an aggregation, in the table's own words. */
  aggSummary(agg: AggregationDto): string {
    return `${aggRowSummary(agg)} · ${agg.scanned.toLocaleString()} events read`;
  }

  onEventsScroll(e: Event): void {
    const el = e.target as HTMLElement;
    // A repeat set with the same value is free (signal equality), which matters because the
    // anchor path writes the real scrollTop from the library's own after-render pass, and that
    // fires a scroll event of its own.
    const head = el.scrollTop <= 4;
    this.atHead.set(head);
    // One predicate, two consumers: the anchor above and the store's follow mode, which stops
    // writing altogether while the reader is away. Driving both from here is what keeps them
    // from disagreeing — the anchor is the fallback for the frame in which the mode changes,
    // not a second mechanism running alongside it.
    this.store.setFollow(head);
    if (el.scrollHeight - el.scrollTop - el.clientHeight < 400) this.store.loadMore();
  }

  scrollToOlderLogs(): void {
    const last = this.listItems().length - 1;
    // 'auto', not 'smooth': during a tail that index moves on every flush, so a smooth scroll
    // spends its whole duration chasing a target running away from it.
    if (last >= 0) this.virtualizer.scrollToIndex(last, { behavior: 'auto' });
  }

  scrollToNewerLogs(): void {
    this.virtualizer.scrollToOffset(0, { behavior: 'auto' });
  }

  /**
   * Back to the head of the tail: scroll there, release anything the hold buffer collected, and
   * start the stream if it is off. It NEVER stops a running tail — the toolbar's toggle is the
   * control that does that, and this button, labelled "Jump to live", used to be wired to the
   * same toggle: pressing it during a tail stopped the stream and (before stop meant freeze)
   * wiped the buffer, while never scrolling anywhere.
   */
  jumpToLive(): void {
    // Follow first, then scroll, so the scroll event this produces re-affirms the mode instead
    // of cancelling it.
    this.store.resumeFollow();
    this.atHead.set(true);
    this.virtualizer.scrollToOffset(0, { behavior: 'auto' });
    if (!this.store.live()) this.store.startLive();
  }

  /** A long press on a row (touch): the message action sheet for that event. */
  openMessageSheet(ev: EventDto): void {
    this.messageSheetEvent.set(ev);
  }

  closeMessageSheet(): void {
    this.messageSheetEvent.set(null);
  }

  /** The sheet's "Open event": the detail for the pressed row. */
  openFromMessageSheet(ev: EventDto): void {
    this.messageSheetEvent.set(null);
    this.store.selectEvent(ev.id);
  }

  /**
   * Drags the drawer's left edge to resize it. The width signal updates live
   * during the drag (zoneless: the signal write drives change detection) and is
   * persisted to localStorage once on release.
   */
  startResize(e: MouseEvent): void {
    e.preventDefault();
    const el = this.drawerEl()?.nativeElement;
    if (!el) return;

    // The drawer is anchored to the right, so its right edge stays fixed while
    // dragging: width = rightEdge − mouseX. Keep at least 340px for the list.
    const rightEdge = el.getBoundingClientRect().right;
    const maxWidth = Math.max(400, window.innerWidth - 340);

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const onMove = (ev: MouseEvent) =>
      this.prefs.setDrawerWidth(Math.min(rightEdge - ev.clientX, maxWidth), false);

    const onUp = () => {
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      this.prefs.setDrawerWidth(this.prefs.drawerWidth(), true); // commit
    };

    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  }
}
