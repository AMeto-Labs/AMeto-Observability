import {
  ChangeDetectionStrategy, Component, ElementRef, afterRenderEffect, input, linkedSignal, output,
  viewChild,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

/** Kept clear of the window's edges. */
const VIEWPORT_MARGIN = 8;

/**
 * Unified property/attribute context menu — the SAME menu on the Events page
 * (detail-drawer properties) and the Traces page (span Tags table):
 *
 *   Copy: Value · Key · Condition (= / ≠)
 *   Find: Search by key · And expression · Or expression — each with = / ≠;
 *         Any element, for a key inside an array
 *   Seek window: ± 5 / 15 / 30 s — for a date value
 *   Open: Full value · the cross-signal jump (“Find in logs” / “Find in traces”)
 *
 * Purely presentational: renders fixed at (x, y) and emits intents; the host
 * page supplies the filter semantics (CLEF expressions vs TraceQL) and closes
 * the menu on outside click / Escape.
 *
 * Floating (the default), (x, y) is where the host would like it; once the menu has rendered
 * and its real size is known it moves itself back inside the window, so a host need not guess
 * its height — hosts that did guessed low, and the bottom of the menu was cut off near the
 * window's edge. A host that renders it into an overlay (ContextMenuService, anchored to the
 * ⋮ button) turns `floating` off: the overlay positions it, and x / y are not used.
 */
@Component({
  selector: 'app-property-menu',
  imports: [LucideAngularModule],
  templateUrl: './property-menu.html',
  styleUrl: './property-menu.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PropertyMenuComponent {
  /** Draw itself `position: fixed` at (x, y). Off inside an overlay, which places it. */
  readonly floating = input<boolean>(true);
  readonly x = input<number>(0);
  readonly y = input<number>(0);
  readonly propKey = input.required<string>();
  /** Display form of the value (header tooltip only). */
  readonly propValue = input<string>('');
  /** Show the Find section (Search / And / Or). Copy-only fields pass false. */
  readonly findable = input<boolean>(true);
  /** Show "View full value" — hosts that can render a full-value modal opt in. */
  readonly viewable = input<boolean>(false);
  /** Show the "Seek window (±)" section — set when the value is a date/timestamp. */
  readonly seekable = input<boolean>(false);
  /** Label for the cross-signal item; null hides it. */
  readonly crossLabel = input<string | null>(null);
  /** Show "Any element" in Find — the key is inside an array (`items[0].id`), so it can be
   *  asked of every element (`items[%].id`) as well as this one. */
  readonly wildcard = input<boolean>(false);

  /** Open the host's full-value modal — the row clamps to one line, this shows all of it. */
  readonly viewValue = output<void>();
  readonly copyValue = output<void>();
  readonly copyKey = output<void>();
  /** Copy `key (=|≠) value` as the host's filter language writes it (payload: neq). */
  readonly copyCondition = output<boolean>();
  /** Seek the time range to value ± N seconds (payload: seconds). */
  readonly seek = output<number>();
  /** Replace the current query with `key (=|≠) value`. */
  readonly search = output<boolean>();   // payload: neq
  /** Append `&& key (=|≠) value` to the current query. */
  readonly andExpr = output<boolean>();
  /** Append `|| key (=|≠) value` to the current query. */
  readonly orExpr = output<boolean>();
  /** Replace the query with the condition asked of every array element (payload: neq). */
  readonly anyElement = output<boolean>();
  readonly cross = output<void>();

  private readonly menu = viewChild.required<ElementRef<HTMLElement>>('menu');

  /** Where the menu is drawn: (x, y) until measured, then moved inside the window. */
  readonly left = linkedSignal(() => this.x());
  readonly top = linkedSignal(() => this.y());

  constructor() {
    afterRenderEffect({
      earlyRead: () => {
        const { width, height } = this.menu().nativeElement.getBoundingClientRect();
        return { x: this.x(), y: this.y(), width, height };
      },
      write: (m) => {
        if (!this.floating()) return;
        const { x, y, width, height } = m();
        const maxLeft = window.innerWidth - width - VIEWPORT_MARGIN;
        const maxTop = window.innerHeight - height - VIEWPORT_MARGIN;
        this.left.set(Math.max(VIEWPORT_MARGIN, Math.min(x, maxLeft)));
        this.top.set(Math.max(VIEWPORT_MARGIN, Math.min(y, maxTop)));
      },
    });
  }
}
