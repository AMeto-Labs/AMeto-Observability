import { ChangeDetectionStrategy, Component, OnInit, inject, input, output } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { SearchScope } from '../../../core/models/search-history.model';
import { ScopedSearchHistory, SearchHistoryService } from '../../../core/services/search-history.service';

/**
 * The user's **search history** section — pinned first, then recent — as it has
 * always looked on the Events page, now shared by all three signal pages.
 *
 * Section only: no panel chrome, no header, no close button. The host owns the
 * surrounding panel and decides when to mount this; mounting under `@if` is what
 * makes every open reload (the reload lives in {@link ngOnInit}).
 */
@Component({
  selector: 'app-search-history',
  imports: [LucideAngularModule],
  templateUrl: './search-history.html',
  styleUrl: './search-history.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SearchHistoryComponent implements OnInit {
  private historyService = inject(SearchHistoryService);

  /** Which page's history to show — each scope keeps its own list. */
  readonly scope = input.required<SearchScope>();
  /**
   * Row text for a query. Default is the raw query; Metrics passes a compact
   * formatter so a long PromQL expression stays readable in one line. The tooltip
   * and {@link querySelected} always carry the RAW query regardless.
   */
  readonly display = input<(q: string) => string>((q: string) => q);
  /** Second line of the empty state — hosts can name their own "run a query" gesture. */
  readonly emptyHint = input<string>('Run a filter to see it here');

  /** The RAW query the user clicked — never the `display` form. */
  readonly querySelected = output<string>();

  /** Resolved once: the scope input cannot change for a mounted panel. */
  history!: ScopedSearchHistory;

  ngOnInit(): void {
    this.history = this.historyService.forScope(this.scope());
    this.history.load();
  }

  apply(query: string): void {
    this.querySelected.emit(query);
  }

  pin(query: string, pinned: boolean, e: MouseEvent): void {
    e.stopPropagation();
    this.history.setPinned(query, pinned);
  }

  remove(query: string, e: MouseEvent): void {
    e.stopPropagation();
    this.history.remove(query);
  }
}
