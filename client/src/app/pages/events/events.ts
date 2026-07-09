import {
  Component, inject, viewChild, ElementRef, OnInit, ChangeDetectionStrategy,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { injectVirtualizer } from '@tanstack/angular-virtual';

import { EmptyStateComponent } from '../../shared/components/ui';
import { EventsToolbarComponent } from './components/events-toolbar/events-toolbar';
import { EventsFilterBarComponent } from './components/events-filter-bar/events-filter-bar';
import { EventListRowComponent } from './components/event-list-row/event-list-row';
import { EventDetailComponent } from './components/event-detail/event-detail';
import { SignalsPanelComponent } from './signals-panel/signals-panel';
import { EventsStore } from './store/events.store';

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
  ],
  templateUrl: './events.html',
  styleUrl: './events.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers: [EventsStore],
})
export class EventsComponent implements OnInit {
  readonly store = inject(EventsStore);

  private eventsScroll = viewChild<ElementRef<HTMLElement>>('eventsScroll');

  // Fixed 29px rows — no dynamic measurement, so no overlap on selection/expand.
  readonly virtualizer = injectVirtualizer(() => ({
    count: this.store.displayedEvents().length,
    scrollElement: this.eventsScroll(),
    estimateSize: () => 29,
    overscan: 20,
    getItemKey: (i: number) => this.store.displayedEvents()[i]?.id ?? i,
  }));

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
}
