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
