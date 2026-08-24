import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { AggregationDto } from '../../../../core/models/event.model';
import { EmptyStateComponent } from '../../../../shared/components/ui';

/**
 * The table a `select … group by …` query answers with.
 *
 * <p>The columns are not known until the answer arrives — they are whatever the query asked
 * for — so the grid is laid out from the response rather than declared in the template. Key
 * columns read left, value columns are right-aligned and tabular, the way numbers meant to be
 * compared down a column have to be.</p>
 */
@Component({
  selector: 'app-aggregation-table',
  standalone: true,
  imports: [LucideAngularModule, EmptyStateComponent],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './aggregation-table.html',
  styleUrl: './aggregation-table.scss',
})
export class AggregationTableComponent {
  readonly result = input.required<AggregationDto>();

  /** Key columns sized to their content, value columns fixed — the numbers are what line up. */
  readonly gridTemplate = computed(() => {
    const r = this.result();
    return [
      ...r.keyColumns.map(() => 'minmax(120px, 1fr)'),
      ...r.valueColumns.map(() => '140px'),
    ].join(' ');
  });

  readonly hasRows = computed(() => this.result().rows.length > 0);

  /** Rows returned versus groups the scan actually found — they differ under a `limit`. */
  readonly rowSummary = computed(() => {
    const r = this.result();
    const shown = r.rows.length;
    return shown < r.groupsFound
      ? `${this.num(shown)} of ${this.num(r.groupsFound)} groups`
      : `${this.num(shown)} ${shown === 1 ? 'group' : 'groups'}`;
  });

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
    return Number.isInteger(v) ? this.num(v) : v.toLocaleString(undefined, { maximumFractionDigits: 3 });
  }

  private num(n: number): string {
    return n.toLocaleString();
  }
}
