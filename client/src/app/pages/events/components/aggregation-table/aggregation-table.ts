import {
  ChangeDetectionStrategy, Component, ElementRef, afterRenderEffect, computed, input, signal, untracked, viewChild,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { AggregationDto } from '../../../../core/models/event.model';
import { EmptyStateComponent } from '../../../../shared/components/ui';

/** Rows returned versus groups the scan actually found — they differ under a `limit`. */
export function aggRowSummary(r: AggregationDto): string {
  const num = (n: number) => n.toLocaleString();
  const shown = r.rows.length;
  return shown < r.groupsFound
    ? `${num(shown)} of ${num(r.groupsFound)} groups`
    : `${num(shown)} ${shown === 1 ? 'group' : 'groups'}`;
}

/**
 * The table a `select … group by …` query answers with.
 *
 * <p>The columns are not known until the answer arrives — they are whatever the query asked
 * for — so the grid is laid out from the response rather than declared in the template. Key
 * columns read left, value columns are right-aligned and tabular, the way numbers meant to be
 * compared down a column have to be.</p>
 *
 * <p>On a phone ({@link phone}; design canvas, "Телефон — агрегация") the table is wider than
 * the screen and scrolls sideways under a sticky header, its first key column pinned at the
 * left, a thin indicator under it saying how far along it is; the summary foot is the page's
 * (events.html), not the table's.</p>
 */
@Component({
  selector: 'app-aggregation-table',
  standalone: true,
  imports: [LucideAngularModule, EmptyStateComponent],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './aggregation-table.html',
  styleUrl: './aggregation-table.scss',
  host: { '[class.agg--phone]': 'phone()' },
})
export class AggregationTableComponent {
  readonly result = input.required<AggregationDto>();
  /** The phone's layout: content-sized columns, the first pinned, the table scrolled sideways. */
  readonly phone = input<boolean>(false);

  /**
   * Key columns sized to their content, value columns fixed — the numbers are what line up. On
   * a phone the design's widths: the pinned first key 150px, the other keys 88, a value no less
   * than 92 and as wide as its numbers.
   */
  readonly gridTemplate = computed(() => {
    const r = this.result();
    if (this.phone()) {
      return [
        ...r.keyColumns.map((_, i) => i === 0 ? '150px' : '88px'),
        ...r.valueColumns.map(() => 'minmax(92px, max-content)'),
      ].join(' ');
    }
    return [
      ...r.keyColumns.map(() => 'minmax(120px, 1fr)'),
      ...r.valueColumns.map(() => '140px'),
    ].join(' ');
  });

  readonly hasRows = computed(() => this.result().rows.length > 0);

  readonly rowSummary = computed(() => aggRowSummary(this.result()));

  /** The phone's scroll indicator: where the visible part of the table is, as percentages. */
  readonly hscroll = signal<{ left: number; width: number } | null>(null);

  private readonly scrollBox = viewChild<ElementRef<HTMLElement>>('aggScroll');

  constructor() {
    // The indicator is there before the first scroll: measured once the table is laid out, and
    // again whenever the answer (and so the table's width) changes.
    afterRenderEffect(() => {
      this.result();
      const el = this.scrollBox()?.nativeElement;
      if (el && this.phone()) untracked(() => this.measureScroll(el));
    });
  }

  onScroll(e: Event): void {
    this.measureScroll(e.target as HTMLElement);
  }

  private measureScroll(el: HTMLElement): void {
    if (el.scrollWidth <= el.clientWidth + 1) { this.hscroll.set(null); return; }
    this.hscroll.set({
      left: (el.scrollLeft / el.scrollWidth) * 100,
      width: (el.clientWidth / el.scrollWidth) * 100,
    });
  }

  /**
   * A missing key is not the empty string and must not look like one: the events in that group
   * carried no value for the column at all.
   */
  key(value: string | null): string {
    return value === null ? '—' : value === '' ? '(empty)' : value;
  }

  /** True for the placeholder above, so the cell can be styled as absent rather than as data. */
  isAbsent(value: string | null): boolean {
    return value === null || value === '';
  }

  /**
   * Null is not zero. A group with no numbers to work on has no average, and printing 0 would
   * put a number on screen that the data does not contain.
   */
  value(v: number | null): string {
    if (v === null) return '—';
    return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 3 });
  }
}
