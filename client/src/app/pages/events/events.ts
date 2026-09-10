import {
  Component, inject, viewChild, viewChildren, ElementRef, OnInit,
  ChangeDetectionStrategy, afterRenderEffect, effect, untracked, signal, DestroyRef,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import {
  injectVirtualizer, measureElement as measureRenderedElement,
} from '@tanstack/angular-virtual';

import { EmptyStateComponent } from '../../shared/components/ui';
import { EventsToolbarComponent } from './components/events-toolbar/events-toolbar';
import { EventsFilterBarComponent } from './components/events-filter-bar/events-filter-bar';
import { EventListRowComponent } from './components/event-list-row/event-list-row';
import { EventDetailComponent } from './components/event-detail/event-detail';
import { AggregationTableComponent } from './components/aggregation-table/aggregation-table';
import { SignalsPanelComponent } from './signals-panel/signals-panel';
import { EventsStore } from './store/events.store';
import { UserPreferencesService } from '../../core/services/user-preferences.service';

/**
 * Events page shell. All state and logic live in {@link EventsStore} (provided
 * here so each mount is fresh); the toolbar, filter bar and detail drawer are
 * self-contained children. This container only lays them out and drives the
 * fixed-height virtual list.
 */
@Component({
  selector: 'app-events',
  imports: [
    LucideAngularModule, EmptyStateComponent,
    EventsToolbarComponent, EventsFilterBarComponent,
    EventListRowComponent, EventDetailComponent, SignalsPanelComponent,
    AggregationTableComponent,
  ],
  templateUrl: './events.html',
  styleUrl: './events.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers: [EventsStore],
})
export class EventsComponent implements OnInit {
  readonly store = inject(EventsStore);
  readonly prefs = inject(UserPreferencesService);

  private eventsScroll = viewChild<ElementRef<HTMLElement>>('eventsScroll');
  private drawerEl = viewChild<ElementRef<HTMLElement>>('drawerEl');

  private rowEls = viewChildren<ElementRef<HTMLElement>>('evRowEl');
  private readonly destroyRef = inject(DestroyRef);

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
   * One virtualizer for BOTH list modes.
   *
   * <p>Wrap mode used to opt out of virtualization entirely — it rendered every row, because
   * fixed-height virtualization cannot hold variable heights. That is true, and the conclusion
   * was the wrong one: it made the mode that produces the TALLEST rows the only one that put
   * all of them in the DOM at once, so a wrapped 5000-row answer was ~5000 rows of layout and
   * the page stopped responding. Measured rows are the answer to variable heights, and the
   * Traces list already does exactly this with the same library.</p>
   *
   * <p>Fixed mode keeps its 29px and measures nothing, which is what the original comment here
   * was protecting: its rows are one line by construction, and measuring them re-introduced an
   * overlap on selection. Only wrap mode measures — {@link measureOnlyWhenWrapping} is what
   * makes that switch, and it is `undefined` (not a no-op function) in fixed mode so the
   * library takes its own non-measuring path rather than one that lies about a height.</p>
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
    const rows = this.store.displayedEvents();
    const wrap = this.store.wrapMessages();
    // Wrapped rows are one line more often than not; the estimate only has to be close enough
    // that the scrollbar does not jump while the real heights arrive.
    const rowHeight = wrap ? 48 : 29;
    return {
      count: rows.length,
      scrollElement: this.eventsScroll(),
      estimateSize: () => rowHeight,
      overscan: 20,
      getItemKey: (i: number) => rows[i]?.id ?? i,
      measureElement: wrap ? measureRenderedElement : undefined,
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

  constructor() {
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
    // Hands each rendered wrapped row over to be measured and observed. afterRender, not
    // effect: a height means nothing until the element exists and is laid out. Idempotent per
    // node, and a no-op in fixed mode where nothing carries #evRowEl.
    afterRenderEffect(() => {
      if (!this.store.wrapMessages()) return;
      for (const el of this.rowEls()) this.virtualizer.measureElement(el.nativeElement);
    });
  }

  ngOnInit(): void {
    this.store.initFromUrl();
    document.addEventListener('visibilitychange', this.onVisibility);
    this.destroyRef.onDestroy(() =>
      document.removeEventListener('visibilitychange', this.onVisibility));
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
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
    const last = this.store.displayedEvents().length - 1;
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
