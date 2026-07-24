import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

/**
 * Unified property/attribute context menu — the SAME menu on the Events page
 * (detail-drawer properties) and the Traces page (span Tags table):
 *
 *   Copy value / Copy key
 *   Find: Search by key · And expression · Or expression — each with = / ≠
 *   optional cross-signal jump (“Find in logs” / “Find in traces”)
 *
 * Purely presentational: renders fixed at (x, y) and emits intents; the host
 * page supplies the filter semantics (CLEF expressions vs TraceQL) and closes
 * the menu on outside click / Escape.
 */
@Component({
  selector: 'app-property-menu',
  imports: [LucideAngularModule],
  templateUrl: './property-menu.html',
  styleUrl: './property-menu.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PropertyMenuComponent {
  readonly x = input.required<number>();
  readonly y = input.required<number>();
  readonly propKey = input.required<string>();
  /** Display form of the value (header tooltip only). */
  readonly propValue = input<string>('');
  /** Label for the cross-signal item; null hides it. */
  readonly crossLabel = input<string | null>(null);

  readonly copyValue = output<void>();
  readonly copyKey = output<void>();
  /** Replace the current query with `key (=|≠) value`. */
  readonly search = output<boolean>();   // payload: neq
  /** Append `&& key (=|≠) value` to the current query. */
  readonly andExpr = output<boolean>();
  /** Append `|| key (=|≠) value` to the current query. */
  readonly orExpr = output<boolean>();
  readonly cross = output<void>();
}
