import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { LEVELS } from '../../../core/models/event.model';
import { OverlayPanelRef } from '../../../shared/services/overlay';

/** Input handed to the levels dropdown via {@link OverlayPanelRef.data}. */
export interface LevelsDropdownData {
  /** Levels currently active in the filter. */
  active: ReadonlySet<string>;
  /** Event counts keyed by lowercased level name. */
  counts: Record<string, number>;
  /** Total loaded events (shown next to "Select all"). */
  total: number;
}

/** Resolved via {@link OverlayPanelRef.close} on Apply; `undefined` when dismissed. */
export type LevelsDropdownResult = Set<string>;

@Component({
  selector: 'app-levels-dropdown',
  standalone: true,
  imports: [LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="dd-panel lvl-dd">
      <div class="dd-panel-header">
        <span class="dd-panel-title">Levels</span>
        <button class="dd-close-btn" title="Close" (click)="ref.close()">
          <lucide-icon name="x" [size]="13" />
        </button>
      </div>

      <label class="dd-select-all-row">
        <input
          type="checkbox"
          [checked]="allActive()"
          [indeterminate]="!allActive() && pending().size > 0"
          (change)="toggleAll()" />
        <span class="dd-select-all-lbl">Select all</span>
        <span class="dd-total-count">{{ fmtCount(ref.data.total) }}</span>
      </label>

      <div class="dd-scroll">
        @for (lvl of levelItems; track lvl.key) {
          <label class="lvl-dd-row" [attr.data-level]="lvl.key.toLowerCase()">
            <input type="checkbox" [checked]="isActive(lvl.key)" (change)="toggle(lvl.key)" />
            <span class="lvl-dot"></span>
            <span class="lvl-name">{{ lvl.key }}</span>
            <span class="lvl-count">{{ fmtCount(ref.data.counts[lvl.key.toLowerCase()] || 0) }}</span>
          </label>
        }
      </div>

      <div class="dd-footer-actions">
        <button class="dd-reset-btn" (click)="reset()">Reset</button>
        <button class="dd-apply-btn" (click)="apply()">Apply</button>
      </div>
    </div>
  `,
})
export class LevelsDropdownComponent {
  readonly ref = inject<OverlayPanelRef<LevelsDropdownResult, LevelsDropdownData>>(OverlayPanelRef);

  readonly levelItems = [
    { key: 'Verbose' }, { key: 'Debug' }, { key: 'Information' },
    { key: 'Warning' }, { key: 'Error' }, { key: 'Fatal' },
  ];

  readonly pending = signal(new Set(this.ref.data.active));
  readonly allActive = computed(() => this.pending().size === LEVELS.length);

  isActive(level: string): boolean {
    return this.pending().has(level);
  }

  toggle(level: string): void {
    this.pending.update((set) => {
      const next = new Set(set);
      next.has(level) ? next.delete(level) : next.add(level);
      return next;
    });
  }

  toggleAll(): void {
    this.pending.set(this.allActive() ? new Set() : new Set(LEVELS as readonly string[]));
  }

  reset(): void {
    this.pending.set(new Set(LEVELS as readonly string[]));
  }

  apply(): void {
    this.ref.close(new Set(this.pending()));
  }

  fmtCount(n: number | undefined): string {
    if (!n) return '0';
    return n >= 1000 ? `${(n / 1000).toFixed(1)}K` : String(n);
  }
}
