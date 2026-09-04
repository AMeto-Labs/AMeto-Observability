import {
  Component, inject, viewChild, viewChildren, ElementRef, OnInit,
  ChangeDetectionStrategy, afterRenderEffect,
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
  readonly virtualizer = injectVirtualizer(() => ({
    count: this.store.displayedEvents().length,
    scrollElement: this.eventsScroll(),
    // Wrapped rows are one line more often than not; the estimate only has to be close enough
    // that the scrollbar does not jump while the real heights arrive.
    estimateSize: () => (this.store.wrapMessages() ? 48 : 29),
    overscan: 20,
    getItemKey: (i: number) => this.store.displayedEvents()[i]?.id ?? i,
    measureElement: this.store.wrapMessages() ? measureRenderedElement : undefined,
  }));

  constructor() {
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
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }

  onEventsScroll(e: Event): void {
    const el = e.target as HTMLElement;
    if (el.scrollHeight - el.scrollTop - el.clientHeight < 400) this.store.loadMore();
  }

  scrollToOlderLogs(): void {
    const last = this.store.displayedEvents().length - 1;
    if (last >= 0) this.virtualizer.scrollToIndex(last, { behavior: 'smooth' });
  }

  scrollToNewerLogs(): void {
    this.virtualizer.scrollToOffset(0, { behavior: 'smooth' });
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
